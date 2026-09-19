/*
 * 排序 Worker：
 *  消息 in:
 *   {type:'init', id, attempt, buffer, simulate:{oom,stall,stallMs}}
 *   {type:'pause'} / {type:'resume'} / {type:'cancel'}
 *  消息 out:
 *   {type:'ready', id}
 *   {type:'progress', id, attempt, p, phase}   （同时充当心跳）
 *   {type:'paused', id, attempt, p}
 *   {type:'sorted', id, attempt, buffer, ms}
 *   {type:'error', id, attempt, error}
 *
 * 协作式中断：每个时间片（~8ms）结束后检查 cancel/pause；
 * 若被注入 stall（死循环），主线程看门狗会 terminate 强杀。
 */
'use strict';

importScripts('mergesort.js');

var SLICE_MS = 8;
var HEARTBEAT_MS = 60;

var task = null;          // {id, attempt, engine}
var buffer = null;        // 当前数据（失败/取消时归还）
var state = 'idle';       // idle | running | paused | cancelling
var timer = null;
var lastBeat = 0;
var startTs = 0;
var simulate = {};

function post(obj, transfer) {
  try {
    if (transfer && transfer.length) self.postMessage(obj, transfer);
    else self.postMessage(obj);
  } catch (_) {
    self.postMessage(obj);
  }
}

function clearTick() {
  if (timer !== null) {
    clearTimeout(timer);
    timer = null;
  }
}

function scheduleTick(delay) {
  clearTick();
  timer = setTimeout(tick, delay || 0);
}

function emitProgress(force) {
  var t = performance.now();
  if (force || t - lastBeat >= HEARTBEAT_MS) {
    lastBeat = t;
    post({
      type: 'progress',
      id: task.id,
      attempt: task.attempt,
      p: task.engine.progress(),
      phase: 'sort'
    });
  }
}

function finishSorted() {
  var ms = performance.now() - startTs;
  var outBuf = task.engine.getResultBuffer();
  var id = task.id, attempt = task.attempt;
  task = null;
  state = 'idle';
  buffer = null;
  post({ type: 'sorted', id: id, attempt: attempt, buffer: outBuf, ms: ms }, [outBuf]);
}

function fail(msg) {
  var info = task ? { id: task.id, attempt: task.attempt } : { id: null, attempt: -1 };
  if (buffer) {
    var b = buffer;
    buffer = null;
    post({ type: 'error', id: info.id, attempt: info.attempt, error: String(msg), buffer: b }, [b]);
  } else {
    post({ type: 'error', id: info.id, attempt: info.attempt, error: String(msg) });
  }
  task = null;
  state = 'idle';
}

function tick() {
  timer = null;
  if (!task) return;
  if (state === 'cancelling') return;
  if (state === 'paused') return;
  state = 'running';

  try {
    var res = task.engine.step(SLICE_MS);
  } catch (e) {
    fail((e && e.name === 'RangeError' ? 'OOM: ' : 'RUNTIME: ') + (e && e.message ? e.message : String(e)));
    return;
  }

  if (state === 'cancelling') return;

  if (res.done) {
    emitProgress(true);
    finishSorted();
    return;
  }

  if (state === 'pause-requested') {
    state = 'paused';
    post({ type: 'paused', id: task.id, attempt: task.attempt, p: task.engine.progress() });
    return;
  }

  emitProgress(false);
  scheduleTick(0);
}

self.onmessage = function (e) {
  var msg = e.data;
  if (!msg || !msg.type) return;

  if (msg.type === 'init') {
    clearTick();
    task = null;
    state = 'running';
    simulate = msg.simulate || {};

    try {
      if (simulate.oom) {
        // 模拟分配输出缓冲时内存不足
        throw new RangeError('simulated out of memory while allocating merge buffer');
      }
      buffer = msg.buffer;
      task = { id: msg.id, attempt: msg.attempt, engine: new MergeSortEngine(msg.buffer) };
    } catch (err) {
      var b = msg.buffer;
      buffer = null;
      task = null;
      state = 'idle';
      post({ type: 'error', id: msg.id, attempt: msg.attempt, error: 'OOM: ' + (err.message || err), buffer: b }, [b]);
      return;
    }

    startTs = performance.now();
    lastBeat = 0;
    post({ type: 'ready', id: task.id });

    if (simulate.stall) {
      // 模拟 Worker 失去响应（无法响应取消），用于验证强杀链路
      var until = performance.now() + (simulate.stallMs || 60000);
      while (performance.now() < until) { /* busy loop */ }
    }
    scheduleTick(0);
    return;
  }

  if (msg.type === 'pause') {
    if (state === 'running') state = 'pause-requested';
    return;
  }

  if (msg.type === 'resume') {
    if (state === 'paused' || state === 'pause-requested') {
      state = 'running';
      scheduleTick(0);
    }
    return;
  }

  if (msg.type === 'cancel') {
    state = 'cancelling';
    clearTick();
    var b2 = buffer;
    buffer = null;
    var info2 = task ? { id: task.id, attempt: task.attempt } : { id: null, attempt: -1 };
    task = null;
    // 归还尚未完成的数据块（终止前的最后一条消息，丢失也无所谓，主线程已 terminate）
    if (b2) {
      try { post({ type: 'stopped', id: info2.id, attempt: info2.attempt, buffer: b2 }, [b2]); } catch (_) {}
    } else {
      post({ type: 'stopped', id: info2.id, attempt: -1 });
    }
    state = 'idle';
  }
};
