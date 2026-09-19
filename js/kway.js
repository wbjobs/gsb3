/*
 * KWayMerger：主线程可中断的 k 路归并。
 *  - 输入：k 个已排序 Int32Array（各自 ArrayBuffer 零拷贝来自 Worker）。
 *  - 输出：一块连续 Int32Array，写入过程按时间片推进，可暂停/取消。
 *  - 使用二叉小顶堆；同值时按分块下标、块内下标决胜，保证稳定。
 */
(function (global) {
  'use strict';

  var BATCH = 4096;

  function KWayMerger(chunks, outBuffer) {
    this.chunks = chunks; // Int32Array[]
    this.k = chunks.length;
    this.out = new Int32Array(outBuffer);
    this.pos = new Int32Array(this.k); // 每块当前下标
    this.heap = [];                    // 元素是块下标
    this.written = 0;
    this.total = this.out.length;

    for (var c = 0; c < this.k; c++) {
      if (chunks[c].length > 0) this.heap.push(c);
    }
    // 建堆
    for (var i = (this.heap.length >> 1) - 1; i >= 0; i--) this.siftDown(i);
  }

  KWayMerger.prototype.less = function (a, b) {
    var va = this.chunks[a][this.pos[a]];
    var vb = this.chunks[b][this.pos[b]];
    if (va !== vb) return va < vb;
    if (a !== b) return a < b;
    return this.pos[a] < this.pos[b];
  };

  KWayMerger.prototype.siftDown = function (i) {
    var h = this.heap;
    var n = h.length;
    var v = h[i];
    while (true) {
      var l = 2 * i + 1;
      if (l >= n) break;
      var r = l + 1;
      var child = l;
      if (r < n && this.less(h[r], h[l])) child = r;
      if (!this.less(h[child], v)) break;
      h[i] = h[child];
      i = child;
    }
    h[i] = v;
  };

  KWayMerger.prototype.progress = function () {
    return this.total ? this.written / this.total : 1;
  };

  KWayMerger.prototype.step = function (timeBudget) {
    var deadline = performance.now() + timeBudget;
    var h = this.heap;
    var n = 0;

    while (h.length > 0) {
      var c = h[0];
      this.out[this.written++] = this.chunks[c][this.pos[c]];
      this.pos[c]++;
      if (this.pos[c] >= this.chunks[c].length) {
        h[0] = h[h.length - 1];
        h.pop();
      }
      if (h.length > 1) this.siftDown(0);

      n++;
      if (n >= BATCH) {
        if (performance.now() >= deadline) return { done: false };
        n = 0;
      }
    }
    return { done: true };
  };

  global.KWayMerger = KWayMerger;
})(typeof self !== 'undefined' ? self : globalThis);
