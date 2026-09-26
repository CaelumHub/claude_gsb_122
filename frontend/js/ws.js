/* ================================================================
   ws.js —— 协同 WebSocket 客户端

   职责(对应「断线重连状态同步与操作补发」难点):
   - 指数退避 + 抖动的自动重连;
   - last_rev 持久化到 localStorage: 刷新/断线重连后 hello 带上。
     服务端只在能证明 (last_rev, head_rev] 逐号连续时补发操作;
     环形缓冲/磁盘日志任一存在缺口, 或缺口过大, 明确退回全量 state。
   - 「未 ack 操作队列」同样持久化: 断线期间产生的本地操作先入队,
     重连后自动补发; 服务端按 op_id 幂等去重(dup ack), 不会重复生效。
     队列不再按固定条数静默截断, 存储失败会显式报错。
   - 心跳: 应答服务端 ping, 且每 25s 主动 ping 一次, 60s 无任何
     消息则视为假死连接, 主动断开触发重连;
   - 事件分发: on(type, handler), 状态变化广播 wb:conn-status。
   ================================================================ */
import { Api } from './api.js';

const LS = {
  rev: (boardId) => `wb_rev_${boardId}`,
  queue: (boardId) => `wb_pending_${boardId}`,
  clientId: (boardId) => `wb_client_${boardId}`,
};

function lsGet(key, fallback = null) {
  try {
    const raw = localStorage.getItem(key);
    return raw == null ? fallback : JSON.parse(raw);
  } catch { return fallback; }
}

function lsSet(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
    return true;
  } catch {
    return false;
  }
}

export function getClientId(boardId) {
  let id = lsGet(LS.clientId(boardId));
  if (!id || typeof id !== 'string') {
    id = (crypto?.randomUUID ? crypto.randomUUID().replace(/-/g, '') : Math.random().toString(36).slice(2))
      .slice(0, 16);
    lsSet(LS.clientId(boardId), id);
  }
  return id;
}

export class BoardSocket {
  /**
   * @param {string} boardId
   * @param {object} opts
   *   page      —— 页面标识(editor/mindmap/chat/...)
   *   clientId  —— 站点 ID(缺省自动生成并持久化)
   *   persist   —— 是否持久化 rev/队列(默认 true; 回放页等只读场景可关)
   */
  constructor(boardId, opts = {}) {
    this.boardId = boardId;
    this.page = opts.page || 'editor';
    this.persist = opts.persist !== false;
    this.clientId = opts.clientId || getClientId(boardId);
    this.ws = null;
    this.status = 'idle';            // idle | connecting | open | reconnecting | closed
    this.manualClose = false;
    this.retry = 0;
    this.lastRev = this.persist ? (lsGet(LS.rev(boardId), 0) || 0) : 0;
    this.pending = this.persist ? (lsGet(LS.queue(boardId), []) || []) : [];
    // 清理旧版本可能持久化的无状态 move; 它们永远不会被服务端接受。
    this.pending = this._networkReadyOps(this.pending);
    // 已并入当前本地状态的操作: 远端去重; 自己的操作仅用于全量状态后重放。
    this._deliveredOpIds = new Set();
    this._ownedOpIds = new Set(this.pending.map((op) => op?.op_id).filter(Boolean));
    this._stateReplayOpIds = new Set();
    this._handlers = {};
    this._pingTimer = null;
    this._watchdog = null;
    this._reconnectTimer = null;
    this.onStatusChange = opts.onStatusChange || null;
    window.addEventListener('online', () => { if (this.status === 'reconnecting') this.connect(); });
  }

  /* ------------------------------------------------------------ 事件 */
  on(type, handler) {
    (this._handlers[type] = this._handlers[type] || []).push(handler);
    return this;
  }

  emit(type, data) {
    for (const handler of this._handlers[type] || []) {
      try { handler(data); } catch (err) { console.error(`[ws] handler(${type})`, err); }
    }
    for (const handler of this._handlers['*'] || []) {
      try { handler(type, data); } catch (err) { console.error('[ws] handler(*)', err); }
    }
  }

  _setStatus(status, extra = {}) {
    this.status = status;
    if (this.onStatusChange) this.onStatusChange(status, extra);
    window.dispatchEvent(new CustomEvent('wb:conn-status',
      { detail: { boardId: this.boardId, status, ...extra } }));
  }

  /* ------------------------------------------------------------ 连接 */
  connect() {
    if (this.status === 'open' || this.status === 'connecting') return;
    this.manualClose = false;
    this._setStatus(this.retry ? 'reconnecting' : 'connecting');
    const url = Api.wsUrl(this.boardId, {
      clientId: this.clientId, lastRev: this.lastRev, page: this.page,
    });
    let ws;
    try { ws = new WebSocket(url); } catch (err) {
      this._scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      this.retry = 0;
      this._setStatus('open');
      this._startHeartbeat();
    };
    ws.onmessage = (event) => this._onMessage(event.data);
    ws.onclose = (event) => {
      if (this.ws !== ws) return;
      this._stopHeartbeat();
      if (this.manualClose) { this._setStatus('closed'); return; }
      // 4401 未登录 / 4403 无权限: 不做无意义重连
      if (event.code === 4401 || event.code === 4403) {
        this._setStatus('closed', { code: event.code, fatal: true });
        this.emit('fatal', { code: event.code });
        return;
      }
      this._scheduleReconnect();
    };
    ws.onerror = () => { /* onclose 会跟进处理 */ };
  }

  _scheduleReconnect() {
    if (this.manualClose) return;
    this.retry += 1;
    const base = Math.min(30_000, 800 * 2 ** Math.min(this.retry, 5));
    const delay = base * (0.7 + Math.random() * 0.6);   // 抖动防雪崩
    this._setStatus('reconnecting', { retry: this.retry, delay: Math.round(delay) });
    clearTimeout(this._reconnectTimer);
    this._reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  close() {
    this.manualClose = true;
    clearTimeout(this._reconnectTimer);
    this._stopHeartbeat();
    try { this.ws?.close(); } catch { /* 忽略 */ }
    this.ws = null;
    this._setStatus('closed');
  }

  /* ------------------------------------------------------------ 心跳 */
  _startHeartbeat() {
    this._stopHeartbeat();
    this._bumpWatchdog();
    this._pingTimer = setInterval(() => this._send({ type: 'ping' }), 25_000);
  }

  _bumpWatchdog() {
    clearTimeout(this._watchdog);
    this._watchdog = setTimeout(() => {
      // 60s 没有任何下行消息 → 假死, 主动断开走重连
      try { this.ws?.close(); } catch { /* 忽略 */ }
    }, 60_000);
  }

  _stopHeartbeat() {
    clearInterval(this._pingTimer);
    clearTimeout(this._watchdog);
  }

  /* ------------------------------------------------------------ 收发 */
  _send(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
      return true;
    }
    return false;
  }

  _networkReadyOps(ops) {
    const out = [];
    for (const op of Array.isArray(ops) ? ops : [ops]) {
      if (op?.type === 'batch') {
        const subs = this._networkReadyOps(op.ops || []);
        if (subs.length === 1) {
          out.push(subs[0]);
        } else if (subs.length) {
          out.push({ ...op, ops: subs });
        }
        continue;
      }
      // (0,0) move 对 CRDT 状态无影响, 不应进入未 ack 队列阻塞后续补发。
      if (op?.type === 'move' && Number(op.dx || 0) === 0 && Number(op.dy || 0) === 0) continue;
      if (op) out.push(op);
    }
    return out;
  }

  _rememberDelivered(opId) {
    if (!opId) return;
    if (this._deliveredOpIds.size >= 10000) {
      this._deliveredOpIds.delete(this._deliveredOpIds.values().next().value);
    }
    this._deliveredOpIds.add(opId);
  }

  _remoteOps(ops) {
    const out = [];
    for (const op of ops || []) {
      const opId = op?.op_id;
      if (!opId || this._ownedOpIds.has(opId) || this._deliveredOpIds.has(opId)) continue;
      out.push(op);
      this._rememberDelivered(opId);
    }
    return out;
  }

  /**
   * 客户端最后一道防线: 任何无法证明连续、或服务端版本倒退/跳跃的响应,
   * 都不把半截状态交给页面。last_rev=0 后重连, 强制服务端退回全量状态。
   */
  _forceFullState(reason) {
    if (this.manualClose) return;
    console.error('[ws] catchup gap detected; falling back to full state', reason);
    this.emit('sync_fallback', { reason });
    this.lastRev = 0;
    this._saveRev();
    this._stateReplayOpIds = new Set(this.pending.map((op) => op?.op_id).filter(Boolean));
    this.retry = Math.max(this.retry, 1);
    this._stopHeartbeat();
    clearTimeout(this._reconnectTimer);
    try { this.ws?.close(4001, reason); } catch { /* 立即连接路径兜底 */ }
    this._connectSoon();
  }

  _connectSoon() {
    clearTimeout(this._reconnectTimer);
    this._reconnectTimer = setTimeout(() => {
      // 主动全量重连时绕过 open/connecting 状态保护; 旧 socket 的 close
      // 回调看到新连接后不应再启动第二路连接。
      const old = this.ws;
      this.status = 'reconnecting';
      this.retry = Math.max(this.retry, 1);
      this.ws = null;
      try { old?.close(4001, 'full-state-reconnect'); } catch { /* 忽略 */ }
      this.connect();
    }, delay);
  }

  _onMessage(raw) {
    this._bumpWatchdog();
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const type = msg.type;

    if (type === 'welcome') {
      const head = Number(msg.head_rev);
      if (!Number.isInteger(head) || head < this.lastRev) {
        this._forceFullState('invalid-head');
        return;
      }

      if (msg.state) {
        const stateRev = Number(msg.state.rev);
        if (!Number.isInteger(stateRev) || stateRev !== head || !Array.isArray(msg.state.shapes)) {
          this._forceFullState('invalid-state');
          return;
        }
        this._stateReplayOpIds = new Set(this.pending.map((op) => op?.op_id).filter(Boolean));
        this._deliveredOpIds.clear();
        this.lastRev = stateRev;
        this._saveRev();
        this.emit('welcome', msg);
        this._flushPending();
        return;
      }

      if (head > this.lastRev) {
        const catchup = msg.catchup;
        const fromRev = Number(catchup?.from_rev);
        const toRev = Number(catchup?.to_rev);
        if (!catchup || !Array.isArray(catchup.ops)
          || fromRev !== this.lastRev || toRev !== head
          || catchup.ops.length !== head - this.lastRev) {
          this._forceFullState('incomplete-welcome-catchup');
          return;
        }
        const byRev = new Map();
        for (const op of catchup.ops) {
          const rev = Number(op?.rev);
          if (!Number.isInteger(rev) || rev <= fromRev || rev > toRev || byRev.has(rev)) {
            this._forceFullState('invalid-catchup-rev');
            return;
          }
          byRev.set(rev, op);
        }
        for (let rev = fromRev + 1; rev <= toRev; rev += 1) {
          if (!byRev.has(rev)) {
            this._forceFullState('missing-catchup-rev');
            return;
          }
        }
        const orderedOps = [...byRev.keys()].sort((a, b) => a - b)
          .map((rev) => byRev.get(rev));
        const remoteOps = this._remoteOps(orderedOps);
        catchup.ops = remoteOps;
        this.lastRev = toRev;
        this._saveRev();
        this.emit('welcome', msg);
        this._flushPending();
        return;
      }

      // gap=0 且无 state/catchup 是正常的空补发。
      if (msg.catchup) {
        this._forceFullState('unexpected-empty-catchup');
        return;
      }
      this.lastRev = head;
      this._saveRev();
      this.emit('welcome', msg);
      this._flushPending();
      return;
    }

    if (type === 'ops') {
      const head = msg.head_rev == null ? null : Number(msg.head_rev);
      if (head !== null && (!Number.isInteger(head) || head < this.lastRev)) {
        this._forceFullState('invalid-ops-head');
        return;
      }
      const incomingOps = this._remoteOps(msg.ops || []);
      const byRev = new Map();
      for (const op of incomingOps) {
        const rev = Number(op?.rev);
        if (!Number.isInteger(rev) || rev <= this.lastRev || byRev.has(rev)) {
          this._forceFullState('invalid-ops-rev');
          return;
        }
        byRev.set(rev, op);
      }
      let expected = this.lastRev + 1;
      for (const rev of [...byRev.keys()].sort((a, b) => a - b)) {
        if (rev !== expected) {
          this._forceFullState('missing-live-ops');
          return;
        }
        expected += 1;
      }
      const remoteOps = [...byRev.keys()].sort((a, b) => a - b)
        .map((rev) => byRev.get(rev));
      const maxRev = remoteOps.length ? expected - 1 : this.lastRev;
      if (head !== null && head < maxRev) {
        this._forceFullState('ops-head-gap');
        return;
      }
      this.lastRev = head ?? maxRev;
      this._saveRev();
      if (remoteOps.length) this.emit('ops', { ...msg, ops: remoteOps, head_rev: this.lastRev });
      return;
    }

    if (type === 'ack') {
      if (msg.head_rev != null) { this.lastRev = Math.max(this.lastRev, Number(msg.head_rev) || 0); this._saveRev(); }
      const pendingById = new Map(this.pending.map((op) => [op.op_id, op]));
      const replayOps = [];
      const acked = new Set();
      for (const ack of msg.acks || []) {
        const opId = ack.op_id;
        if (!opId) continue;
        acked.add(opId);
        if (!ack.dup && this._stateReplayOpIds.has(opId)) {
          const op = pendingById.get(opId);
          if (op) replayOps.push(op);
        }
        this._stateReplayOpIds.delete(opId);
      }
      this._removePending(acked);
      this.emit('ack', msg);
      // 全量状态中可能已包含/不包含断线期间的本地操作: 非 dup ack 才重放
      // 一次; dup ack 说明全量状态已经有该操作, 不能再次应用。
      if (replayOps.length) this.emit('own_ops_replay', { ops: replayOps });
      // 服务端单批最多接受 max_ops_per_batch 条, 剩余队列在此续排
      this._flushPending();
      return;
    }
    if (type === 'ping') { this._send({ type: 'ping' }); return; }   // 服务端心跳 → 应答
    if (type === 'pong') return;
    this.emit(type, msg);
  }

  _saveRev() {
    if (this.persist && !lsSet(LS.rev(this.boardId), this.lastRev)) {
      this.emit('error', { code: 'local_storage_full', message: '无法保存同步进度, 请清理浏览器存储后重试' });
    }
  }

  /* ------------------------------------------------------------ 操作发送 */
  /** 发送一个(或一批)本地 CRDT 操作; 断线时进入持久化补发队列。 */
  sendOps(ops) {
    const list = this._networkReadyOps(Array.isArray(ops) ? ops : [ops]);
    if (!list.length) return;
    this.pending.push(...list);
    for (const op of list) {
      if (op?.op_id) this._ownedOpIds.add(op.op_id);
    }
    if (this._ownedOpIds.size > 10000) {
      this._ownedOpIds.delete(this._ownedOpIds.values().next().value);
    }
    this._savePending();
    this._flushPending();
  }

  sendOp(op) { this.sendOps([op]); }

  _flushPending() {
    if (this.status !== 'open' || !this.pending.length) return;
    const batch = this.pending.slice(0, 64);
    const ok = this._send({ type: 'ops', ops: batch });
    if (ok) this.emit('pending-flushed', { count: batch.length, remaining: this.pending.length - batch.length });
  }

  _removePending(ackedIds) {
    if (!ackedIds?.size) return;
    this.pending = this.pending.filter((op) => !ackedIds.has(op.op_id));
    this._savePending();
  }

  _savePending() {
    if (!this.persist) return;
    // 未 ack 的本地操作绝不能为了控制存储体积而静默截断。配额失败时保留
    // 内存队列并明确报错; 后续仍会重连重试, 不会伪造同步完成。
    if (!lsSet(LS.queue(this.boardId), this.pending)) {
      this.emit('error', {
        code: 'local_storage_full',
        message: '本地操作队列无法持久化; 请不要关闭页面并清理浏览器存储',
      });
    }
  }

  pendingCount() { return this.pending.length; }

  /* ------------------------------------------------------------ 其他消息 */
  sendChat(text) { return this._send({ type: 'chat', text }); }

  sendPresence(payload) { return this._send({ type: 'presence', ...payload }); }

  sendHello(page) { return this._send({ type: 'hello', page: page || this.page, client_id: this.clientId, last_rev: this.lastRev }); }
}
