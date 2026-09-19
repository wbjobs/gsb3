/*
 * sort-core.js — 可中断归并排序核心
 *
 * 同一份源码运行在三个环境：
 *   1. 主线程（<script> 引入，降级 / 主线程分片执行时使用）
 *   2. Web Worker（通过 Function.prototype.toString 拼装进 Blob Worker）
 *   3. Node.js（test.js 直接 require 做正确性验证）
 *
 * 设计要点：
 *   - 排序 / 归并都是可重入的状态机，step(budgetMs) 每次只在时间预算内推进，
 *     返回 false 表示未完成，可随时暂停 / 继续 / 放弃。
 *   - 工作量模型统一：归并中每放置一个元素计 1 个工作量单位。
 *     排序总工作量 = n * ceil(log2(n))，归并总工作量 = a.length + b.length，
 *     因此进度 = done / total 是精确的，误差远小于 5%。
 */
(function (global) {
  'use strict';

  /**
   * 可中断的自底向上（迭代式）归并排序。
   * @param {Int32Array} arr 就地排序的数组
   */
  function createMergeSortTask(arr) {
    // 注意：本函数会被 toString() 拼进 Blob Worker，函数体必须自包含，
    // 只能引用自身局部变量与全局 API（performance / Math 等）。
    var n = arr.length;
    var passes = 0;
    while ((1 << passes) < n) passes++;
    var total = n * passes; // 总工作量（元素放置次数）

    var tmp = new arr.constructor(n);
    var width = 1;      // 当前归并宽度
    var left = 0;       // 当前归并对的左边界
    var i = 0, j = 0, k = 0, mid = 0, right = 0;
    var merging = false;
    var done = 0;

    function step(budgetMs) {
      var start = performance.now();
      while (width < n) {
        if (!merging) {
          if (left >= n) { width *= 2; left = 0; continue; }
          mid = Math.min(left + width, n);
          right = Math.min(left + 2 * width, n);
          i = left; j = mid; k = left;
          merging = true;
        }
        while (k < right) {
          if (i < mid && (j >= right || arr[i] <= arr[j])) tmp[k++] = arr[i++];
          else tmp[k++] = arr[j++];
          done++;
          if ((done & 1023) === 0 && performance.now() - start >= budgetMs) {
            return false; // 预算用完，让出事件循环（此时可响应 pause / 消息）
          }
        }
        for (var t = left; t < right; t++) arr[t] = tmp[t];
        merging = false;
        left += 2 * width;
      }
      return true;
    }

    return {
      step: step,
      total: total,
      result: function () { return arr.buffer; },
      getDone: function () { return done; }
    };
  }

  /**
   * 可中断的二路归并：把两个有序 Int32Array 归并为一个新的有序数组。
   */
  function createMergeTask(a, b) {
    // 同 createMergeSortTask：函数体必须自包含（会被拼进 Blob Worker）。
    var total = a.length + b.length;
    var out = new a.constructor(total);
    var i = 0, j = 0, k = 0;
    var done = 0;

    function step(budgetMs) {
      var start = performance.now();
      while (k < total) {
        if (i < a.length && (j >= b.length || a[i] <= b[j])) out[k++] = a[i++];
        else out[k++] = b[j++];
        done++;
        if ((done & 1023) === 0 && performance.now() - start >= budgetMs) {
          return false;
        }
      }
      return true;
    }

    return {
      step: step,
      total: total,
      result: function () { return out.buffer; },
      getDone: function () { return done; }
    };
  }

  /**
   * Worker 端协议入口。会被 toString() 后拼进 Blob Worker 源码，
   * 因此函数体内只能引用本文件中的顶层函数与 Worker 全局 API。
   *
   * 协议（主 -> Worker）：
   *   {type:'sort',  id, buffer}      buffer 为 Transferable，就地排序
   *   {type:'merge', id, bufA, bufB}  两个有序数组归并
 *   {type:'pause'} / {type:'resume'} / {type:'cancel'}
 *   {type:'ping'}                       心跳探活，立刻回 pong
 *   {type:'fault', mode}               测试注入：'freeze' | 'crash' | 'stall'
   * 协议（Worker -> 主）：
 *   {type:'ready'} / {type:'pong', paused}
   *   {type:'ack', id}
   *   {type:'progress', id, done, total}
   *   {type:'done', id, buffer}       buffer 以 Transferable 回传
   */
  function workerMain() {
    var SLICE_BUDGET_MS = 12; // 每个切片最长执行时间，之后让出事件循环
    var task = null;
    var jobId = -1;
    var paused = false;
    var faultMode = null; // 'freeze'(完全不响应) | 'crash'(退出) | 'stall'(继续算但静默)

    function schedule() {
      setTimeout(loop, 0);
    }

    function loop() {
      if (!task || paused) return;
      var finished;
      try {
        finished = task.step(SLICE_BUDGET_MS);
      } catch (err) {
        postMessage({ type: 'error', id: jobId, message: String(err && err.message || err) });
        task = null;
        return;
      }
      if (faultMode !== 'stall' && faultMode !== 'freeze') {
        postMessage({ type: 'progress', id: jobId, done: task.getDone(), total: task.total });
      }
      if (finished) {
        var buf = task.result();
        task = null;
        if (faultMode !== 'freeze') postMessage({ type: 'done', id: jobId, buffer: buf }, [buf]);
      } else {
        schedule();
      }
    }

    self.onmessage = function (e) {
      var msg = e.data;
      if (!msg || !msg.type) return;
      if (faultMode === 'freeze') return; // 冻结后完全不应答，由主线程看门狗判定死亡
      switch (msg.type) {
        case 'sort':
          try {
            task = createMergeSortTask(new Int32Array(msg.buffer));
            jobId = msg.id;
            paused = false;
            postMessage({ type: 'ack', id: jobId });
            schedule();
          } catch (err) {
            postMessage({ type: 'error', id: msg.id, message: String(err && err.message || err) });
          }
          break;
        case 'merge':
          try {
            task = createMergeTask(new Int32Array(msg.bufA), new Int32Array(msg.bufB));
            jobId = msg.id;
            paused = false;
            postMessage({ type: 'ack', id: jobId });
            schedule();
          } catch (err) {
            postMessage({ type: 'error', id: msg.id, message: String(err && err.message || err) });
          }
          break;
        case 'pause':
          paused = true; // 当前切片结束后停止调度，不再占用 CPU
          break;
        case 'resume':
          if (task) { paused = false; schedule(); }
          break;
        case 'cancel':
          task = null; jobId = -1; paused = false; // 立即丢弃任务状态
          break;
        case 'ping':
          postMessage({ type: 'pong', id: msg.id, jobId: jobId, paused: paused,
                        hasTask: !!task, done: task ? task.getDone() : 0,
                        total: task ? task.total : 0 });
          break;
        case 'fault':
          faultMode = msg.mode;
          if (faultMode === 'crash') {
            task = null;
            self.close();
          } else if (faultMode === 'stall') {
            paused = false;
            if (task) schedule();
          } else if (faultMode === 'freeze') {
            // 不再响应任何消息；任务保留（模拟消息丢失 / 线程卡死）
          }
          break;
      }
    };

    postMessage({ type: 'ready' });
  }

  var api = {
    createMergeSortTask: createMergeSortTask,
    createMergeTask: createMergeTask,
    workerMain: workerMain
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api; // Node.js（测试用）
  }
  global.SortCore = api;
})(typeof self !== 'undefined' ? self : globalThis);
