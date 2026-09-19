/*
 * WorkerPool：管理 4~8 个排序 Worker。
 *  - 建池失败自动重试，最终保留可用 Worker（最少 1 个）；全部失败时通知上层降级主线程。
 *  - 每个分块带 attempt 版本号，过期回复一律丢弃（防消息乱序/重复）。
 *  - 看门狗：心跳超时即判定 Worker 卡死/丢消息，terminate 后重排。
 *  - 取消：先广播 cancel（协作），100ms 后仍未确认的全部 terminate（强杀）。
 */
(function (global) {
  'use strict';

  var MAX_ATTEMPTS = 4;
  var CREATE_FAIL_BUDGET = 2;
  var HEARTBEAT_TIMEOUT_MS = 1500;
  var FORCE_KILL_MS = 100;

  function WorkerPool(desiredCount, hooks) {
    this.hooks = hooks || {};
    this.slots = [];
    this.pending = [];
    this.paused = false;
    this.cancelled = false;
    this.watchdog = null;
    this.createFailures = 0;
    this.desired = desiredCount;
    this.rawFactory = hooks.rawFactory || function () { return new Worker('js/sort-worker.js'); };
  }

  WorkerPool.prototype.log = function (msg) {
    if (this.hooks.log) this.hooks.log(msg);
  };

  // 返回存活 Worker 数（0 表示需要整体降级）
  WorkerPool.prototype.init = function () {
    var count = this.desired;
    for (var i = 0; i < count && this.createFailures <= CREATE_FAIL_BUDGET; i++) {
      if (!this.createSlot()) {
        i--; // 重试
        if (this.createFailures > CREATE_FAIL_BUDGET) break;
      }
    }
    if (this.slots.length === 0) {
      this.log('所有 Worker 创建失败，降级到主线程分片执行');
      return 0;
    }
    if (this.slots.length < this.desired) {
      this.log('Worker 创建部分失败，降级为 ' + this.slots.length + ' 个 Worker');
    }
    this.watchdog = setInterval(this.checkWatchdog.bind(this), HEARTBEAT_TIMEOUT_MS / 2);
    return this.slots.length;
  };

  WorkerPool.prototype.createSlot = function () {
    var w;
    try {
      w = this.rawFactory();
      if (!w || typeof w.postMessage !== 'function') throw new Error('factory returned invalid worker');
    } catch (e) {
      this.createFailures++;
      this.log('Worker 创建失败 (' + this.createFailures + '): ' + (e.message || e));
      return false;
    }
    var slot = {
      worker: w,
      alive: true,
      busy: false,
      chunkId: -1,
      attempt: -1,
      lastBeat: 0,
      confirmedStop: false
    };
    this.attach(slot);
    this.slots.push(slot);
    return true;
  };

  WorkerPool.prototype.attach = function (slot) {
    var self = this;
    slot.worker.onmessage = function (e) { self.onSlotMessage(slot, e.data); };
    slot.worker.onerror = function (e) {
      self.log('Worker 错误事件: ' + (e.message || 'unknown') + '，回收该 Worker');
      var lostId = slot.chunkId, lostAttempt = slot.attempt, wasBusy = slot.busy;
      self.killSlot(slot);
      if (wasBusy && self.hooks.onChunkLost) self.hooks.onChunkLost(lostId, lostAttempt);
      self.replaceSlot();
      self.pump();
    };
    slot.worker.onmessageerror = function () {
      self.log('Worker 消息解码错误，回收该 Worker');
      var lostId = slot.chunkId, lostAttempt = slot.attempt, wasBusy = slot.busy;
      self.killSlot(slot);
      if (wasBusy && self.hooks.onChunkLost) self.hooks.onChunkLost(lostId, lostAttempt);
      self.replaceSlot();
      self.pump();
    };
  };

  WorkerPool.prototype.replaceSlot = function () {
    if (this.cancelled) return;
    if (this.createFailures > CREATE_FAIL_BUDGET) {
      if (this.aliveCount() === 0) {
        this.shutdown();
        if (this.hooks.onAllWorkersDead) this.hooks.onAllWorkersDead();
      }
      return;
    }
    this.createSlot();
  };

  WorkerPool.prototype.addChunks = function (chunks) {
    for (var i = 0; i < chunks.length; i++) this.pending.push(chunks[i]);
    this.pump();
  };

  WorkerPool.prototype.aliveCount = function () {
    var n = 0;
    for (var i = 0; i < this.slots.length; i++) if (this.slots[i].alive) n++;
    return n;
  };

  WorkerPool.prototype.pump = function () {
    if (this.cancelled || this.paused) return;
    for (var i = 0; i < this.slots.length; i++) {
      var slot = this.slots[i];
      if (!slot.alive || slot.busy || this.pending.length === 0) continue;
      var chunk = this.pending.shift();
      slot.busy = true;
      slot.chunkId = chunk.id;
      slot.attempt = chunk.attempt;
      slot.lastBeat = performance.now();
      slot.confirmedStop = false;
      try {
        slot.worker.postMessage(
          {
            type: 'init',
            id: chunk.id,
            attempt: chunk.attempt,
            buffer: chunk.buffer,
            simulate: chunk.simulate || {}
          },
          [chunk.buffer]
        );
      } catch (e) {
        this.log('派发分块 #' + chunk.id + ' 失败: ' + (e.message || e));
        slot.busy = false;
        slot.chunkId = -1;
        this.requeue(chunk.id, chunk.attempt, chunk.buffer, 'postMessage 失败');
        this.pump();
      }
    }
  };

  WorkerPool.prototype.requeue = function (id, attempt, buffer, reason) {
    if (attempt + 1 > MAX_ATTEMPTS) {
      if (this.hooks.onChunkFatal) this.hooks.onChunkFatal(id, '重试次数耗尽: ' + reason);
      return;
    }
    this.log('分块 #' + id + ' 重新排队（第 ' + (attempt + 1) + ' 次）: ' + reason);
    if (this.hooks.onChunkRetried) this.hooks.onChunkRetried(id, attempt + 1, reason);
    this.pending.push({ id: id, attempt: attempt + 1, buffer: buffer, simulate: {} });
  };

  WorkerPool.prototype.releaseSlot = function (slot) {
    slot.busy = false;
    slot.chunkId = -1;
    slot.attempt = -1;
  };

  WorkerPool.prototype.onSlotMessage = function (slot, msg) {
    if (!msg || !msg.type) return;

    if (msg.type === 'ready') {
      slot.lastBeat = performance.now();
      return;
    }
    if (msg.type === 'progress') {
      // 过期任务的心跳（重排后旧 Worker 又说话）直接丢弃
      if (!slot.busy || msg.id !== slot.chunkId || msg.attempt !== slot.attempt) return;
      slot.lastBeat = performance.now();
      if (this.hooks.onProgress) this.hooks.onProgress(msg.id, msg.attempt, msg.p);
      return;
    }
    if (msg.type === 'paused') {
      if (!slot.busy || msg.id !== slot.chunkId || msg.attempt !== slot.attempt) return;
      if (this.hooks.onWorkerPaused) this.hooks.onWorkerPaused(msg.id);
      return;
    }
    if (msg.type === 'error') {
      if (slot.busy && msg.id === slot.chunkId && msg.attempt === slot.attempt && msg.buffer) {
        var id = msg.id, attempt = msg.attempt, buffer = msg.buffer;
        this.releaseSlot(slot);
        this.requeue(id, attempt, buffer, msg.error || 'worker error');
        this.replaceSlot();
        this.pump();
      }
      return;
    }
    if (msg.type === 'stopped') {
      slot.confirmedStop = true;
      return;
    }
    if (msg.type === 'sorted') {
      if (!slot.busy || msg.id !== slot.chunkId || msg.attempt !== slot.attempt) {
        // 过期结果：缓冲已无用，直接丢弃（GC 回收）
        return;
      }
      slot.lastBeat = performance.now();
      var sid = msg.id;
      var sbuf = msg.buffer;
      this.releaseSlot(slot);
      if (this.hooks.onSorted) this.hooks.onSorted(sid, msg.attempt, sbuf, msg.ms);
      this.pump();
    }
  };

  WorkerPool.prototype.checkWatchdog = function () {
    if (this.cancelled) return;
    var now = performance.now();
    var dead = [];
    for (var i = 0; i < this.slots.length; i++) {
      var slot = this.slots[i];
      if (slot.alive && slot.busy && now - slot.lastBeat > HEARTBEAT_TIMEOUT_MS) {
        dead.push({ slot: slot, id: slot.chunkId, attempt: slot.attempt });
      }
    }
    for (var d = 0; d < dead.length; d++) {
      this.log('分块 #' + dead[d].id + ' 心跳超时（疑似卡死/消息丢失），强杀并重新排队');
      this.killSlot(dead[d].slot);
      // 数据随 Worker 一起丢失（buffer 所有权已转出），主线程从主数据重新切片
      if (this.hooks.onChunkLost) this.hooks.onChunkLost(dead[d].id, dead[d].attempt);
      this.replaceSlot();
    }
    this.pump();
  };

  // terminate 一个 Slot；数据若在途则视为丢失
  WorkerPool.prototype.killSlot = function (slot, graceful) {
    if (!slot.alive) return;
    slot.alive = false;
    slot.busy = false;
    try { slot.worker.terminate(); } catch (_) {}
    var idx = this.slots.indexOf(slot);
    if (idx >= 0) this.slots.splice(idx, 1);
  };

  WorkerPool.prototype.pause = function () {
    this.paused = true;
    for (var i = 0; i < this.slots.length; i++) {
      try { this.slots[i].worker.postMessage({ type: 'pause' }); } catch (_) {}
    }
  };

  WorkerPool.prototype.resume = function () {
    this.paused = false;
    for (var i = 0; i < this.slots.length; i++) {
      try { this.slots[i].worker.postMessage({ type: 'resume' }); } catch (_) {}
    }
    this.pump();
  };

  // 取消：返回 Promise，resolve(全部 Worker 终止的耗时 ms)
  WorkerPool.prototype.cancel = function () {
    var self = this;
    this.cancelled = true;
    this.paused = false;
    this.pending.length = 0;
    var t0 = performance.now();
    var resolved = false;

    for (var i = 0; i < this.slots.length; i++) {
      try { this.slots[i].worker.postMessage({ type: 'cancel' }); } catch (_) {}
    }

    function finish() {
      if (resolved) return;
      resolved = true;
      for (var i = 0; i < self.slots.length; i++) {
        var s = self.slots[i];
        if (s.alive) { try { s.worker.terminate(); } catch (_) {} s.alive = false; }
      }
      self.slots.length = 0;
      if (self.watchdog) { clearInterval(self.watchdog); self.watchdog = null; }
      return performance.now() - t0;
    }

    return new Promise(function (resolve) {
      var timer = setTimeout(function () { resolve(finish()); }, FORCE_KILL_MS);
      // 全部 Worker 协作式确认停止后立即结束（不等满 100ms）
      var poll = setInterval(function () {
        if (self.slots.length === 0) { clearInterval(poll); clearTimeout(timer); resolve(finish()); return; }
        for (var i = 0; i < self.slots.length; i++) {
          if (self.slots[i].busy && !self.slots[i].confirmedStop) return;
        }
        clearInterval(poll);
        clearTimeout(timer);
        resolve(finish());
      }, 5);
    });
  };

  WorkerPool.prototype.shutdown = function () {
    if (this.watchdog) { clearInterval(this.watchdog); this.watchdog = null; }
    for (var i = 0; i < this.slots.length; i++) {
      try { this.slots[i].worker.terminate(); } catch (_) {}
    }
    this.slots.length = 0;
  };

  global.WorkerPool = WorkerPool;
})(typeof self !== 'undefined' ? self : globalThis);
