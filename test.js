/*
 * test.js — Node.js 验证：
 *  1. 可中断归并排序 / 二路归并的正确性与进度单调性
 *  2. 模拟 Blob Worker 源码拼装（toString 方案）并驱动完整 sort/merge/pause/resume 协议
 * 运行：node test.js
 */
'use strict';
const SortCore = require('./sort-core.js');

let failures = 0;
function assert(cond, msg) {
  if (!cond) { failures++; console.error('FAIL:', msg); }
  else console.log('ok:', msg);
}
function randomArray(n) {
  const a = new Int32Array(n);
  for (let i = 0; i < n; i++) a[i] = (Math.random() * 0x7fffffff) | 0;
  return a;
}
function isSorted(a) {
  for (let i = 1; i < a.length; i++) if (a[i - 1] > a[i]) return false;
  return true;
}

// 1. 小步长切片推进的可中断排序（模拟 Worker 的 12ms 切片，这里用 0ms 强制频繁让出）
{
  const arr = randomArray(100000);
  const task = SortCore.createMergeSortTask(arr);
  let slices = 0, lastDone = -1, monotonic = true;
  while (!task.step(0)) {
    slices++;
    if (task.getDone() < lastDone) monotonic = false;
    lastDone = task.getDone();
    if (slices > 1e7) break;
  }
  assert(isSorted(arr), 'interruptible sort produces sorted array (100k, ' + slices + ' slices)');
  assert(monotonic, 'progress (done) is monotonically non-decreasing');
  assert(task.getDone() === task.total, 'sort done === total when finished (' + task.getDone() + ')');
}

// 2. 边界：空数组 / 单元素
{
  const t0 = SortCore.createMergeSortTask(new Int32Array(0));
  assert(t0.step(1) === true, 'empty array sorts immediately');
  const t1 = SortCore.createMergeSortTask(new Int32Array([42]));
  assert(t1.step(1) === true, 'single element sorts immediately');
}

// 3. 二路归并
{
  const a = new Int32Array([1, 3, 5, 7]);
  const b = new Int32Array([2, 4, 6, 8, 9]);
  const task = SortCore.createMergeTask(a, b);
  while (!task.step(0)) {}
  const out = new Int32Array(task.result());
  assert(isSorted(out) && out.length === 9, 'merge of two sorted arrays is sorted');
  assert(task.getDone() === task.total, 'merge done === total');
}

// 4. 模拟 Blob Worker：用 toString 拼装源码，在 vm 中驱动完整协议
{
  const vm = require('vm');
  const src =
    SortCore.createMergeSortTask.toString() + '\n' +
    SortCore.createMergeTask.toString() + '\n' +
    '(' + SortCore.workerMain.toString() + ')();';

  const posted = [];
  const handlers = {};
  const sandbox = {
    self: {},
    performance,
    Int32Array,
    Math,
    setTimeout: (fn) => { handlers.loop = fn; }, // 手动驱动事件循环
    postMessage: (msg) => posted.push(msg)
  };
  vm.createContext(sandbox);
  vm.runInContext(src, sandbox);
  assert(posted.some(m => m.type === 'ready'), 'worker posts ready');

  // sort 任务
  const data = randomArray(500000); // 足够大，保证 pause 时任务未完成
  const buf = data.buffer.slice(0);
  sandbox.self.onmessage({ data: { type: 'sort', id: 1, buffer: buf } });
  assert(posted.some(m => m.type === 'ack' && m.id === 1), 'worker acks sort job');

  // 暂停：驱动若干切片后 pause，确认不再推进
  let guard = 0;
  while (handlers.loop && guard++ < 2) { const f = handlers.loop; handlers.loop = null; f(); } // 只推进 2 个切片就暂停
  sandbox.self.onmessage({ data: { type: 'pause' } });
  const progressBefore = posted.filter(m => m.type === 'progress').length;
  if (handlers.loop) { const f = handlers.loop; handlers.loop = null; f(); }
  assert(!handlers.loop, 'paused worker stops scheduling slices (no CPU spin)');

  // 继续
  sandbox.self.onmessage({ data: { type: 'resume' } });
  guard = 0;
  while (handlers.loop && guard++ < 1e6) { const f = handlers.loop; handlers.loop = null; f(); }
  const doneMsg = posted.find(m => m.type === 'done' && m.id === 1);
  assert(!!doneMsg, 'worker completes sort after resume');
  assert(doneMsg && isSorted(new Int32Array(doneMsg.buffer)), 'worker sort result is sorted');
  const progressAfter = posted.filter(m => m.type === 'progress').length;
  assert(progressAfter > progressBefore, 'progress messages flow after resume');

  // merge 任务
  const ma = new Int32Array([1, 4, 9]);
  const mb = new Int32Array([2, 3, 10]);
  sandbox.self.onmessage({ data: { type: 'merge', id: 2, bufA: ma.buffer, bufB: mb.buffer } });
  guard = 0;
  while (handlers.loop && guard++ < 1e6) { const f = handlers.loop; handlers.loop = null; f(); }
  const mergeDone = posted.find(m => m.type === 'done' && m.id === 2);
  const merged = mergeDone && Array.from(new Int32Array(mergeDone.buffer));
  assert(merged && merged.join() === '1,2,3,4,9,10', 'worker merge result correct: ' + merged);
}

// 5. 端到端：1M 随机数，分 8 块排序 + 树状归并（全部用可中断任务，模拟主流程）
{
  const N = 1000000, CHUNKS = 8;
  const data = randomArray(N);
  const expected = Int32Array.from(data).sort((a, b) => a - b);

  const t0 = Date.now();
  const runs = [];
  for (let c = 0; c < CHUNKS; c++) {
    const slice = data.slice(c * (N / CHUNKS), (c + 1) * (N / CHUNKS));
    const task = SortCore.createMergeSortTask(slice);
    while (!task.step(50)) {}
    runs.push(slice);
  }
  while (runs.length > 1) {
    const next = [];
    for (let i = 0; i < runs.length; i += 2) {
      if (i + 1 >= runs.length) { next.push(runs[i]); continue; }
      const task = SortCore.createMergeTask(runs[i], runs[i + 1]);
      while (!task.step(50)) {}
      next.push(new Int32Array(task.result()));
    }
    runs.length = 0;
    runs.push(...next);
  }
  const dt = Date.now() - t0;
  const result = runs[0];
  let equal = result.length === expected.length;
  for (let i = 0; equal && i < expected.length; i++) if (result[i] !== expected[i]) equal = false;
  assert(equal, 'end-to-end 1M / 8-chunk chunked sort matches reference (' + dt + 'ms)');
}

// 6. 新协议：ping/pong 心跳、cancel 立即丢弃任务
{
  const vm = require('vm');
  const src =
    SortCore.createMergeSortTask.toString() + '\n' +
    SortCore.createMergeTask.toString() + '\n' +
    '(' + SortCore.workerMain.toString() + ')();';
  const posted = [];
  const handlers = {};
  const sandbox = {
    self: {},
    performance,
    Int32Array,
    Math,
    setTimeout: (fn) => { handlers.loop = fn; },
    postMessage: (msg) => posted.push(msg)
  };
  vm.createContext(sandbox);
  vm.runInContext(src, sandbox);

  // ping 在空闲时也应立刻回 pong
  sandbox.self.onmessage({ data: { type: 'ping' } });
  const pongIdle = posted.find(m => m.type === 'pong');
  assert(!!pongIdle && pongIdle.hasTask === false, 'ping -> pong when idle');

  // 派发大任务，运行中 ping 应带任务进度
  const data = randomArray(500000);
  sandbox.self.onmessage({ data: { type: 'sort', id: 7, buffer: data.buffer.slice(0) } });
  let guard = 0;
  while (handlers.loop && guard++ < 1) { const f = handlers.loop; handlers.loop = null; f(); }
  posted.length = 0;
  sandbox.self.onmessage({ data: { type: 'ping' } });
  const pongBusy = posted.find(m => m.type === 'pong');
  assert(!!pongBusy && pongBusy.hasTask === true && pongBusy.jobId === 7,
    'ping while busy reports job state');

  // cancel：任务被丢弃，不再调度、不再上报
  sandbox.self.onmessage({ data: { type: 'cancel' } });
  posted.length = 0;
  if (handlers.loop) { const f = handlers.loop; handlers.loop = null; f(); }
  assert(!handlers.loop && posted.length === 0, 'cancel drops task and stops scheduling');
}

// 7. freeze：注入后任何消息（含 ping）都不响应
{
  const vm = require('vm');
  const src =
    SortCore.createMergeSortTask.toString() + '\n' +
    SortCore.createMergeTask.toString() + '\n' +
    '(' + SortCore.workerMain.toString() + ')();';
  const posted = [];
  const handlers = {};
  const sandbox = {
    self: {},
    performance,
    Int32Array,
    Math,
    setTimeout: (fn) => { handlers.loop = fn; },
    postMessage: (msg) => posted.push(msg)
  };
  vm.createContext(sandbox);
  vm.runInContext(src, sandbox);
  const data = randomArray(500000);
  sandbox.self.onmessage({ data: { type: 'sort', id: 3, buffer: data.buffer.slice(0) } });
  sandbox.self.onmessage({ data: { type: 'fault', mode: 'freeze' } });
  posted.length = 0;
  sandbox.self.onmessage({ data: { type: 'ping' } });
  sandbox.self.onmessage({ data: { type: 'pause' } });
  assert(posted.length === 0, 'frozen worker ignores all messages (heartbeat will time out)');
}

console.log(failures === 0 ? '\nALL TESTS PASSED' : '\n' + failures + ' FAILURES');
process.exit(failures === 0 ? 0 : 1);
