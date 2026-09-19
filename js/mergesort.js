/*
 * 可中断的自底向上归并排序引擎（Worker 与主线程共用）。
 *
 * 特点：
 *  - step(timeBudget) 按时间片推进，每个元素写入都计入预算，
 *    调用方可在任意时间片之间暂停、继续、取消。
 *  - 进度 = 已完成的元素写入数 / 总写入数，与真实工作量一致。
 *  - 输入输出均为 Int32Array / ArrayBuffer，零拷贝交给 Worker。
 */
(function (global) {
  'use strict';

  var RUN = 16;               // 小数组用插入排序预处理
  var BATCH_RUNS = 64;        // 初始阶段每个时间片最多处理多少个 run
  var BATCH_WRITES = 4096;    // 归并阶段每个时间片最多写入多少元素

  function insertionSort(arr, lo, hi) {
    for (var i = lo + 1; i < hi; i++) {
      var v = arr[i];
      var j = i - 1;
      while (j >= lo && arr[j] > v) {
        arr[j + 1] = arr[j];
        j--;
      }
      arr[j + 1] = v;
    }
  }

  function MergeSortEngine(buffer) {
    this.input = new Int32Array(buffer);
    this.n = this.input.length;
    this.src = this.input;

    // 双缓冲：自底向上归并，结果可能落在 dst 或 src 中
    this.dstArr = new Int32Array(new ArrayBuffer(this.n << 2));
    this.phase = 'init';
    this.done = false;
    this.i = 0;
    this.pass = 0;
    this.passes = Math.max(1, Math.ceil(Math.log2(Math.ceil(this.n / RUN))));

    // 总写入数 = 初始插入排序写入 n + 每趟归并写入 n
    this.totalWrites = this.n * (this.passes + 1);
    this.writes = 0;

    // 当前归并块的游标
    this.lo = 0;
    this.mid = 0;
    this.end = 0;
    this.i1 = 0;
    this.i2 = 0;
  }

  MergeSortEngine.prototype.progress = function () {
    return this.totalWrites ? this.writes / this.totalWrites : 1;
  };

  // 推进一个时间片；返回 {done, paused}
  MergeSortEngine.prototype.step = function (timeBudget) {
    var deadline = (typeof performance !== 'undefined' ? performance.now() : Date.now()) + timeBudget;
    var n = this.n;

    if (this.phase === 'init') {
      var runsDone = 0;
      while (this.i < n && runsDone < BATCH_RUNS) {
        var hi = Math.min(this.i + RUN, n);
        insertionSort(this.src, this.i, hi);
        this.writes += hi - this.i;
        this.i = hi;
        runsDone++;
        if (runsDone % 4 === 0 && now() >= deadline) return { done: false, paused: true };
      }
      if (this.i >= n) {
        this.phase = 'merge';
        this.pass = 0;
      } else {
        return { done: false, paused: true };
      }
    }

    var a = this.src;
    var b = this.dstArr;
    var width;

    while (this.phase === 'merge') {
      width = RUN << this.pass;

      if (this.lo === 0 && this.i1 === 0 && this.i2 === 0 && this.mid === 0 && this.end === 0) {
        // 新一趟开始（lo 等游标均为初始值）
      }
      if (this.mid === 0 && this.end === 0) {
        this.mid = Math.min(this.lo + width, n);
        this.end = Math.min(this.lo + 2 * width, n);
        this.i1 = this.lo;
        this.i2 = this.mid;

        // 右半为空：整段直接搬运（单尾块也要写入目标缓冲）
        if (this.i2 === this.end) {
          var copied = 0;
          while (this.i1 < this.end) {
            b[this.i1] = a[this.i1];
            this.i1++;
            this.writes++;
            copied++;
            if (copied === BATCH_WRITES) {
              if (now() >= deadline) return { done: false, paused: true };
              copied = 0;
            }
          }
          this.advanceBlock(n, width);
          if (this.phase === 'done') break;
          a = this.src;
          b = this.dstArr;
          continue;
        }
      }

      var wrote = 0;
      while (this.i1 < this.mid && this.i2 < this.end) {
        if (a[this.i1] <= a[this.i2]) {
          b[this.lo++] = a[this.i1++];
        } else {
          b[this.lo++] = a[this.i2++];
        }
        this.writes++;
        wrote++;
        if (wrote === BATCH_WRITES) {
          if (now() >= deadline) return { done: false, paused: true };
          wrote = 0;
        }
      }
      while (this.i1 < this.mid) {
        b[this.lo++] = a[this.i1++];
        this.writes++;
        wrote++;
        if (wrote === BATCH_WRITES) {
          if (now() >= deadline) return { done: false, paused: true };
          wrote = 0;
        }
      }
      while (this.i2 < this.end) {
        b[this.lo++] = a[this.i2++];
        this.writes++;
        wrote++;
        if (wrote === BATCH_WRITES) {
          if (now() >= deadline) return { done: false, paused: true };
          wrote = 0;
        }
      }

      this.advanceBlock(n, width);
      if (this.phase === 'done') break;
      a = this.src;
      b = this.dstArr;
    }

    this.done = true;
    return { done: true, paused: false };
  };

  MergeSortEngine.prototype.advanceBlock = function (n, width) {
    var nextLo = this.end;
    if (nextLo >= n) {
      // 本趟结束，交换双缓冲
      var tmp = this.src;
      this.src = this.dstArr;
      this.dstArr = tmp;
      this.pass++;
      this.lo = 0;
      this.mid = 0;
      this.end = 0;
      this.i1 = 0;
      this.i2 = 0;
      if (this.pass >= this.passes) {
        this.phase = 'done';
      }
    } else {
      this.lo = nextLo;
      this.mid = 0;
      this.end = 0;
    }
  };

  MergeSortEngine.prototype.getResultBuffer = function () {
    return this.src.buffer;
  };

  function now() {
    return typeof performance !== 'undefined' ? performance.now() : Date.now();
  }

  global.MergeSortEngine = MergeSortEngine;
})(typeof self !== 'undefined' ? self : globalThis);
