/* ================================================================
   ws.js —— 协同 WebSocket 客户端

   职责(对应「断线重连状态同步与操作补发」难点):
   - 指数退避 + 抖动的自动重连;
   - last_rev 持久化到 localStorage: 刷新/断线重连后 hello 带上,
     服务端只补发错过的操作(ring → 磁盘 → 全量快照 三级降级);
   - 「未 ack 操作队列」同样持久化: 断线期间产生的本地操作先入队,
     重连后自动补发; 服务端按 op_id 幂等去重(dup ack), 不会重复生效;
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

/* 客户端 op_id 去重窗口大小(服务端幂等窗口为 8192, 留有余量) */
const SEEN_OPS_LIMIT = 10000;

function lsGet(key, fallback = null) {
  try {
    const raw = localStorage.getItem(key);
    return raw == null ? fallback : JSON.parse(raw);
  } catch { return fallback; }
}

function lsSet(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* 配额满/隐私模式 */ }
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
    this._handlers = {};
    this._pingTimer = null;
    this._watchdog = null;
    this._reconnectTimer = null;
    this._seenOps = new Set();      // 已合并的 op_id(至少一次投递 → 恰好一次)
    this._seenQueue = [];           // FIFO 驱逐队列(与 _seenOps 同步)
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

  _onMessage(raw) {
    this._bumpWatchdog();
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const type = msg.type;

    if (type === 'welcome') {
      if (msg.state) {
        this.lastRev = msg.state.rev ?? msg.head_rev ?? 0;
        this._saveRev();
      } else if (msg.catchup) {
        if (!this._verifyCatchup(msg.catchup, msg.head_rev)) {
          // 补发窗口无法闭合缺口 → 明确退回全量状态, 绝不推进 lastRev
          // 把缺口静默吞掉(否则只有整页刷新才能恢复一致)
          this._resyncFromScratch('catchup-incomplete');
          return;
        }
        // 去重后交回给页面合并(重连竞态下同一操作可能既被广播又被补发)
        msg.catchup.ops = this._filterFresh(msg.catchup.ops);
        this._applyCatchup(msg.catchup.ops);
      }
      if (msg.head_rev != null) { this.lastRev = Math.max(this.lastRev, msg.head_rev); this._saveRev(); }
      this._flushPending();
      this.emit('welcome', msg);
      return;
    }
    if (type === 'ops') {
      if (msg.head_rev != null) { this.lastRev = Math.max(this.lastRev, msg.head_rev); this._saveRev(); }
      msg.ops = this._filterFresh(msg.ops);
      if (msg.ops.length) this.emit('ops', msg);
      return;
    }
    if (type === 'ack') {
      if (msg.head_rev != null) { this.lastRev = Math.max(this.lastRev, msg.head_rev); this._saveRev(); }
      const acked = new Set((msg.acks || []).map((a) => a.op_id));
      this._removePending(acked);
      this.emit('ack', msg);
      // 服务端单批最多接受 max_ops_per_batch 条, 剩余队列在此续排
      this._flushPending();
      return;
    }
    if (type === 'ping') { this._send({ type: 'ping' }); return; }   // 服务端心跳 → 应答
    if (type === 'pong') return;
    if (type === 'error' && msg.code === 'invalid_op' && Array.isArray(msg.rejected)) {
      // 校验失败的操作永远不会被接受: 移出补发队列, 防止毒化后续每次重连
      this._removePending(new Set(msg.rejected));
    }
    this.emit(type, msg);
  }

  /* ------------------------------------------------------------ 补发校验与去重 */
  /** 校验补发窗口恰好连续覆盖 (lastRev, headRev]; 覆盖不了返回 false。 */
  _verifyCatchup(catchup, headRev) {
    const fromRev = Number(catchup && catchup.from_rev);
    const head = Number(headRev);
    if (!Number.isFinite(fromRev) || !Number.isFinite(head)) return false;
    if (fromRev !== this.lastRev) return false;        // 必须正好接上本地进度
    const ops = Array.isArray(catchup.ops) ? catchup.ops : [];
    const expect = head - fromRev;
    if (expect < 0 || ops.length !== expect) return false;
    for (let i = 0; i < ops.length; i += 1) {
      if (Number(ops[i] && ops[i].rev) !== fromRev + i + 1) return false;
    }
    return true;
  }

  /** 补发链路闭合不了缺口时: 丢弃本地 rev 进度并重连, 让服务端发全量状态。 */
  _resyncFromScratch(reason) {
    this.lastRev = 0;
    this._saveRev();
    this.emit('resync', { reason });
    try { this.ws && this.ws.close(); } catch { /* 忽略 */ }
    // onclose → 自动重连 → hello(last_rev=0) → 服务端下发全量 state
  }

  /**
   * op_id 去重(至少一次投递 → 恰好一次)。
   * move 是增量操作, 重复合并会双倍位移, 因此广播/补发/本地回声
   * 统一按 op_id 幂等; 窗口有界, 与后端 _seen 同思路。
   */
  _markSeen(opId) {
    if (!opId || this._seenOps.has(opId)) return;
    this._seenOps.add(opId);
    this._seenQueue.push(opId);
    if (this._seenQueue.length > SEEN_OPS_LIMIT) {
      this._seenOps.delete(this._seenQueue.shift());
    }
  }

  /** 过滤掉已见过的操作并把新操作登记进窗口; 返回可安全合并的子集。 */
  _filterFresh(ops) {
    const fresh = [];
    for (const op of ops || []) {
      const oid = op && op.op_id;
      if (oid && this._seenOps.has(oid)) continue;
      if (oid) this._markSeen(oid);
      fresh.push(op);
    }
    return fresh;
  }

  _applyCatchup(ops) {
    // 补发的操作交给页面合并; 这里只推进 rev
    let maxRev = this.lastRev;
    for (const op of ops || []) maxRev = Math.max(maxRev, op.rev || 0);
    this.lastRev = maxRev;
    this._saveRev();
    this.emit('catchup', { ops: ops || [] });
  }

  _saveRev() {
    if (this.persist) lsSet(LS.rev(this.boardId), this.lastRev);
  }

  /* ------------------------------------------------------------ 操作发送 */
  /** 发送一个(或一批)本地 CRDT 操作; 断线时进入持久化补发队列。 */
  sendOps(ops) {
    const list = Array.isArray(ops) ? ops : [ops];
    if (!list.length) return;
    // 本地已乐观合并: 登记 op_id, 之后补发/广播里的回声不再重复合并
    for (const op of list) if (op && op.op_id) this._markSeen(op.op_id);
    this.pending.push(...list);
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
    if (this.persist) {
      // 队列只保留最近 500 条, 防 localStorage 膨胀
      if (this.pending.length > 500) this.pending = this.pending.slice(-500);
      lsSet(LS.queue(this.boardId), this.pending);
    }
  }

  pendingCount() { return this.pending.length; }

  /* ------------------------------------------------------------ 其他消息 */
  sendChat(text) { return this._send({ type: 'chat', text }); }

  sendPresence(payload) { return this._send({ type: 'presence', ...payload }); }

  sendHello(page) { return this._send({ type: 'hello', page: page || this.page, client_id: this.clientId, last_rev: this.lastRev }); }
}
