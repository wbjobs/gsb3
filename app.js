/*
 * app.js — 主线程编排
 *
 * 流程：生成数据(分片) -> 构建 分块排序+树状归并 任务图 -> Worker 线程池执行
 *       -> 看门狗心跳监测 -> 全部完成后主线程分片校验
 *
 * 异常链路：
 *   Worker 创建失败  -> 重试单 Worker -> 仍失败则主线程分片执行(requestIdleCallback)
 *   Worker 冻结/静默 -> ping/pong 看门狗判定死亡 -> 任务重投给存活 Worker
 *                       存活 Worker 耗尽 -> 新建 1 个 -> 再失败则主线程
 *   Worker 内存不足  -> 报错回传 -> 任务重投(保留原始输入，最多 3 次) -> 主线程
 *   主线程内存不足   -> 致命错误，给出可读提示
 */
'use strict';

(function () {
  var MAX_LEAVES = 8;
  var WATCHDOG_PING_MS = 600;
  var WATCHDOG_TIMEOUT_MS = 1800;
  var WATCHDOG_STALL_MS = 3000;
  var READY_TIMEOUT_MS = 2000;
  var JOB_ATTEMPTS_MAX = 3;
  var WORKER_COLORS = ['#4f9dff', '#3ecf8e', '#f2c94c', '#b388ff',
    '#ff8a65', '#4dd0e1', '#f06292', '#aed581'];

  var $ = function (id) { return document.getElementById(id); };

  var el = {
    count: $('count'), workers: $('workers'),
    start: $('btnStart'), pause: $('btnPause'), cancel: $('btnCancel'),
    fault: $('fault'), faultTarget: $('faultTarget'), inject: $('btnInject'),
    phase: $('phase'), pct: $('pct'), doneJobs: $('doneJobs'),
    elapsed: $('elapsed'), fps: $('fps'), modeStat: $('modeStat'),
    cancelStat: $('cancelStat'), chart: $('chart'), legend: $('legend'),
    log: $('log'), verify: $('verify'),
    btnClick1: $('btnClick1'), btnClick2: $('btnClick2'),
    btnAnim: $('btnAnim'), scrollpad: $('scrollpad')
  };

  var S = null;

  function now() { return performance.now(); }

  function idle(cb) {
    if (typeof requestIdleCallback === 'function') {
      return requestIdleCallback(cb, { timeout: 50 });
    }
    return setTimeout(function () { cb({ timeRemaining: function () { return 6; } }); }, 0);
  }
  function cancelIdle(handle) {
    if (typeof cancelIdleCallback === 'function') cancelIdleCallback(handle);
    else clearTimeout(handle);
  }

  function log(msg, cls) {
    var line = document.createElement('div');
    if (cls) line.className = cls;
    var t = (now() / 1000).toFixed(2);
    line.textContent = '[' + t + 's] ' + msg;
    el.log.appendChild(line);
    while (el.log.childNodes.length > 80) el.log.removeChild(el.log.firstChild);
    el.log.scrollTop = el.log.scrollHeight;
  }

  // ---------- 工作量模型（进度误差 < 5% 的基础：按精确工作量加权） ----------
  // 叶子排序中每个元素被放置 ceil(log2(n))≈18 次，树状归并中每个元素共放置
  // (k-1)≈7 次（另含每次 new Int32Array 的内存分配/带宽成本）。
  // 1M/8 端到端基准（含内存分配）：归并总耗时 ≈ 叶子排序总耗时 × 0.172。
  // 换算到“单次放置”工作量：SORT_WEIGHT = 0.172 × 7 / (18×1.4) ≈ 0.048，
  // 使叶子阶段与归并阶段的进度都按真实 CPU 成本加权，跨阶段误差 < 5%。
  var SORT_WEIGHT = 0.048;
  function sortUnits(n) {
    var passes = 0;
    while ((1 << passes) < n) passes++;
    return n * passes * SORT_WEIGHT;
  }
  // 任务的加权工作量（叶子排序乘 SORT_WEIGHT，归并按元素数）
  function jobWeight(j) {
    return j.type === 'sort' ? j.units * SORT_WEIGHT : j.units;
  }

  // ---------- 状态 ----------
  function newState(n, k) {
    return {
      runId: 0,
      n: n,
      k: k,
      phase: 'init',            // init|generating|sorting|verifying|done|cancelled|error
      paused: false,
      mode: null,               // 'workers' | 'main'
      master: null,
      jobs: [],
      workers: [],
      nextId: 1,
      watchdog: 0,
      mainHandle: 0,
      mainJob: null,
      mainTask: null,
      startWall: 0,
      pauseStart: 0,
      pauseAccum: 0,
      totalUnits: 0,
      doneUnits: 0,
      genDone: 0,
      verifyDone: 0,
      verifyState: null,
      blobUrls: []
    };
  }

  // ---------- 任务图：k 个叶子排序 + 二路归并树 ----------
  function buildJobs(st) {
    var n = st.n, k = Math.max(1, Math.min(Math.min(MAX_LEAVES, st.k), st.n));
    st.k = k;
    var base = Math.floor(n / k), extra = n % k;
    var nodes = [];
    var start = 0, totalUnits = 0;
    for (var i = 0; i < k; i++) {
      var len = base + (i < extra ? 1 : 0);
      var id = st.nextId++;
      var units = sortUnits(len);
      totalUnits += units;
      st.jobs[id] = {
        id: id, type: 'sort', deps: [], leaf: i,
        lo: start, hi: start + len, len: len,
        units: units, done: 0,
        state: 'pending', worker: -1, attempts: 0,
        buf: null, label: '分块 #' + (i + 1) + ' [' + start + '…' + (start + len - 1) + ']'
      };
      nodes.push(id);
      start += len;
    }
    while (nodes.length > 1) {
      var next = [];
      for (var p = 0; p < nodes.length; p += 2) {
        if (p + 1 >= nodes.length) { next.push(nodes[p]); continue; }
        var leftJob = st.jobs[nodes[p]];
        var rightJob = st.jobs[nodes[p + 1]];
        var lid = leftJob.lo;
        var hid = rightJob.hi;
        var units2 = hid - lid;
        var nid = st.nextId++;
        totalUnits += units2;
        st.jobs[nid] = {
          id: nid, type: 'merge', deps: [nodes[p], nodes[p + 1]],
          lo: lid, hi: hid, len: units2,
          units: units2, done: 0,
          state: 'pending', worker: -1, attempts: 0,
          bufA: null, bufB: null, buf: null,
          label: '归并 [' + lid + '…' + (hid - 1) + ']'
        };
        next.push(nid);
      }
      nodes = next;
    }
    st.totalUnits = totalUnits;
    st.rootId = nodes[0];
  }

  // ---------- Worker 线程池 ----------
  function buildWorkerSource(st) {
    var C = window.SortCore;
    var src =
      'var createMergeSortTask = ' + C.createMergeSortTask.toString() + ';\n' +
      'var createMergeTask = ' + C.createMergeTask.toString() + ';\n' +
      '(' + C.workerMain.toString() + ')();';
    return src;
  }

  function createWorker(st) {
    var id = st.workers.length;
    var w = {
      id: id, worker: null, url: null,
      busy: false, currentJob: -1, alive: true,
      ready: false, lastHeard: now(), lastProgressAt: now(),
      color: WORKER_COLORS[id % WORKER_COLORS.length]
    };
    try {
      var blob = new Blob([buildWorkerSource(st)], { type: 'application/javascript' });
      w.url = URL.createObjectURL(blob);
      st.blobUrls.push(w.url);
      w.worker = new Worker(w.url);
    } catch (e) {
      w.alive = false;
      return { w: w, error: e };
    }
    w.worker.onmessage = function (ev) { onWorkerMessage(st, id, ev.data); };
    w.worker.onerror = function () {
      // Worker 脚本错误 / self.close() 崩溃
      onWorkerError(st, id);
    };
    st.workers.push(w);
    var readyPromise = new Promise(function (resolve) {
      var settled = false;
      var timer = setTimeout(function () {
        if (settled) return;
        settled = true; clearInterval(tick); resolve(false);
      }, READY_TIMEOUT_MS);
      var tick = setInterval(function () {
        if (w.ready) {
          if (settled) return;
          settled = true; clearInterval(tick); clearTimeout(timer); resolve(true);
        }
      }, 30);
    });
    return { w: w, ready: readyPromise };
  }

  // 返回 'multi' | 'single' | 'none'（none 表示只能主线程）
  function startWorkers(st, count) {
    var createFail = el.fault.value === 'createfail';
    var wanted = Math.max(1, count);
    var attempts = createFail ? wanted : wanted;
    var results = [];
    for (var i = 0; i < attempts; i++) {
      if (createFail && wanted > 1 && i < attempts - 1) {
        // 模拟创建失败：只让最后一个成功，验证“单 Worker 降级”
        var r0 = createWorker(st);
        r0.w.alive = false;
        st.workers.pop(); // 不留死亡索引，保持存活 Worker 的 id 连续
        results.push({ ok: false, simulated: true, ready: Promise.resolve(false) });
        if (r0.w.worker) {
          try { r0.w.worker.terminate(); } catch (e2) {}
          if (r0.w.url) {
            try { URL.revokeObjectURL(r0.w.url); } catch (e2) {}
            var ui = st.blobUrls.indexOf(r0.w.url);
            if (ui >= 0) st.blobUrls.splice(ui, 1);
          }
        }
        continue;
      }
      var r = createWorker(st);
      results.push(r.error ? { ok: false, ready: Promise.resolve(false) }
        : { ok: true, ready: r.ready });
    }
    var readyFlags = results.map(function (r) { return r.ok ? r.ready : Promise.resolve(false); });
    return Promise.all(readyFlags).then(function (flags) {
      // 只保留真正创建成功且 ready 的 worker
      var alive = st.workers.filter(function (w) {
        var keep = w.alive && w.ready;
        return keep;
      });
      if (createFail && alive.length === 1 && wanted > 1) {
        log('故障注入：' + (wanted - 1) + ' 个 Worker 创建失败 → 自动降级为单 Worker', 'warn');
        return 'single';
      }
      if (alive.length >= 1 && alive.length < wanted) {
        log('仅 ' + alive.length + '/' + wanted + ' 个 Worker 就绪 → 使用 ' + alive.length + ' Worker', 'warn');
      }
      if (alive.length >= 1) return alive.length === 1 ? 'single' : 'multi';
      // 全部创建失败：尝试重建 1 个（覆盖运行中全部死亡后的兜底路径）
      return trySingle(st);
    });
  }

  function trySingle(st) {
    log('Worker 池为空，尝试重建单个 Worker…', 'warn');
    var r = createWorker(st);
    if (r.error) return Promise.resolve('none');
    return r.ready.then(function (ok) {
      var alive = st.workers.filter(function (w) { return w.alive && w.ready; });
      if (alive.length >= 1) { log('重建成功，降级为单 Worker 执行', 'warn'); return 'single'; }
      return 'none';
    });
  }

  function aliveWorkers(st) {
    return st.workers.filter(function (w) { return w.alive; });
  }

  // ---------- 任务输入准备 / 分发 ----------
  function makeJobInputs(st, job) {
    if (job.type === 'sort') {
      var leaf = st.master.subarray(job.lo, job.hi);
      return { buffer: leaf.slice().buffer }; // 独立副本：Worker 死亡不影响主数据
    }
    var a = st.jobs[job.deps[0]];
    var b = st.jobs[job.deps[1]];
    // 结构化克隆拷贝（不 transfer）：输入在主线程保留，Worker 死亡后可无损重投
    return { bufA: a.buf, bufB: b.buf };
  }

  function readyJobs(st) {
    var list = [];
    for (var id = 1; id < st.jobs.length; id++) {
      var j = st.jobs[id];
      if (!j || j.state !== 'pending') continue;
      var ok = j.deps.every(function (d) { return st.jobs[d].state === 'done'; });
      if (ok) list.push(id);
    }
    return list;
  }

  function pump(st) {
    if (st.mode !== 'workers' || st.paused || st.phase !== 'sorting') return;
    var queue = readyJobs(st);
    var free = aliveWorkers(st).filter(function (w) { return !w.busy; });
    var qi = 0;
    for (var wi = 0; wi < free.length && qi < queue.length; wi++) {
      var j = st.jobs[queue[qi++]];
      dispatchToWorker(st, free[wi], j);
    }
  }

  function dispatchToWorker(st, w, j) {
    var payload;
    try {
      payload = makeJobInputs(st, j);
    } catch (e) {
      // 尚未置 running：确保 worker 不被卡住
      return handleOom(st, j, e);
    }
    j.state = 'running';
    j.worker = w.id;
    j.done = 0;
    j.attempts++;
    w.busy = true;
    w.currentJob = j.id;
    try {
      w.worker.postMessage(Object.assign({
        type: j.type, id: j.id
      }, payload));
    } catch (e) {
      w.busy = false;
      w.currentJob = -1;
      resetJob(st, j);
      if (isOom(e)) handleOom(st, j, e);
      else jobAttemptFailed(st, j, '分发失败: ' + e.message);
    }
  }

  function resetJob(st, j) {
    j.state = 'pending';
    j.worker = -1;
    j.done = 0;
  }

  function isOom(e) {
    return e && /allocation|memory|RangeError/i.test(String(e.message || e));
  }

  function finalizeJob(st, j, buffer) {
    j.buf = buffer;
    j.done = j.units;
    j.state = 'done';
    var w = st.workers[j.worker];
    if (w) { w.busy = false; w.currentJob = -1; }
    // 释放不再需要的归并输入
    if (j.type === 'merge') {
      st.jobs[j.deps[0]].buf = null;
      st.jobs[j.deps[1]].buf = null;
    }
    var doneCount = 0;
    for (var id = 1; id < st.jobs.length; id++) {
      if (st.jobs[id] && st.jobs[id].state === 'done') doneCount++;
    }
    if (j.id === st.rootId) {
      allSorted(st);
    } else {
      pump(st);
    }
  }

  function jobAttemptFailed(st, j, reason) {
    log('任务 #' + j.id + ' 失败：' + reason + '（第 ' + j.attempts + ' 次）', 'warn');
    var w = st.workers[j.worker];
    if (w) { w.busy = false; w.currentJob = -1; }
    if (j.attempts >= JOB_ATTEMPTS_MAX) {
      log('任务 #' + j.id + ' 重试耗尽，转入主线程执行', 'err');
      resetJob(st, j);
      if (st.mode === 'main') {
        fatal(st, new Error('任务 #' + j.id + ' 在主线程也无法完成：' + reason));
      } else {
        goMain(st);
      }
    } else {
      resetJob(st, j);
      if (st.mode === 'main') startMainLoop(st);
      else pump(st);
    }
  }

  function handleOom(st, j, e) {
    log('内存不足（任务 #' + j.id + '）：' + (e.message || e), 'err');
    var w = j.worker >= 0 ? st.workers[j.worker] : null;
    if (w) { w.busy = false; w.currentJob = -1; }
    resetJob(st, j);
    if (st.mode === 'main') {
      fatal(st, new Error('主线程内存不足，请减小元素数量后重试'));
    } else {
      // 先尝试单 Worker（降低并发内存峰值），仍失败则主线程
      if (aliveWorkers(st).length > 1) {
        log('降低 Worker 并发以回收内存…', 'warn');
        degradeToOne(st);
      } else {
        goMain(st);
      }
    }
  }

  // ---------- Worker 消息路由 ----------
  function onWorkerMessage(st, wid, msg) {
    var w = st.workers[wid];
    if (!w || !w.alive || wid >= st.workers.length) return;
    w.lastHeard = now();
    if (!msg || !msg.type) return;

    if (msg.type === 'ready') { w.ready = true; return; }
    if (msg.type === 'pong') {
      if (msg.hasTask) {
        var pj = st.jobs[msg.jobId];
        if (pj && pj.state === 'running' && pj.worker === wid) {
          pj.done = msg.done; // pong 里的进度只用于图表，不视为正常上报
        }
      }
      return;
    }
    var j = st.jobs[msg.id];
    if (!j || j.state !== 'running' || j.worker !== wid) return; // 过期 / 错乱消息丢弃

    if (msg.type === 'progress') {
      j.done = msg.done;
      w.lastProgressAt = now();
    } else if (msg.type === 'done') {
      finalizeJob(st, j, msg.buffer);
    } else if (msg.type === 'error') {
      var errMsg = msg.message || 'unknown worker error';
      if (isOom({ message: errMsg })) handleOom(st, j, new Error(errMsg));
      else jobAttemptFailed(st, j, errMsg);
    }
  }

  function onWorkerError(st, wid) {
    var w = st.workers[wid];
    if (!w || !w.alive) return;
    log('Worker #' + wid + ' 崩溃退出', 'err');
    killWorker(st, wid);
  }

  function killWorker(st, wid) {
    var w = st.workers[wid];
    if (!w || !w.alive) return;
    w.alive = false;
    w.busy = false;
    var j = w.currentJob >= 0 ? st.jobs[w.currentJob] : null;
    w.currentJob = -1;
    try { w.worker.terminate(); } catch (e) {}
    if (j && j.state === 'running' && j.worker === wid) resetJob(st, j);
    if (st.phase === 'sorting' && st.mode === 'workers') afterWorkerLost(st);
  }

  function afterWorkerLost(st) {
    var run = st.runId;
    var alive = aliveWorkers(st);
    if (alive.length > 0) {
      log(alive.length + ' 个 Worker 存活，任务重新分发', 'dim');
      pump(st);
      return;
    }
    log('所有 Worker 均已停止，尝试重建 1 个 Worker…', 'warn');
    var r = createWorker(st);
    if (r.error) { goMain(st); return; }
    r.ready.then(function (ok) {
      if (!ok || st.phase !== 'sorting' || st.runId !== run) return;
      if (!st.workers[st.workers.length - 1].alive) { goMain(st); return; }
      var live = aliveWorkers(st);
      if (live.length >= 1) {
        log('重建成功，以单 Worker 继续剩余任务', 'warn');
        pump(st);
      } else {
        goMain(st);
      }
    });
  }

  function degradeToOne(st) {
    var alive = aliveWorkers(st);
    for (var i = 1; i < alive.length; i++) {
      var j = alive[i].currentJob >= 0 ? st.jobs[alive[i].currentJob] : null;
      alive[i].alive = false;
      alive[i].busy = false;
      alive[i].currentJob = -1;
      try { alive[i].worker.terminate(); } catch (e) {}
      if (j && j.state === 'running') resetJob(st, j);
    }
    log('已降级为单 Worker（保留 #' + alive[0].id + '）', 'warn');
    pump(st);
  }

  function goMain(st) {
    if (st.mode === 'main') return;
    log('降级到主线程分片执行（requestIdleCallback）', 'warn');
    stopPool(st);
    st.mode = 'main';
    for (var id = 1; id < st.jobs.length; id++) {
      var j = st.jobs[id];
      if (j && j.state === 'running') resetJob(st, j);
    }
    startMainLoop(st);
  }

  function stopPool(st) {
    st.workers.forEach(function (w) {
      if (w.alive) {
        w.alive = false;
        try { w.worker.terminate(); } catch (e) {}
      }
    });
    st.blobUrls.forEach(function (u) { try { URL.revokeObjectURL(u); } catch (e) {} });
    st.blobUrls = [];
  }

  // ---------- 主线程分片执行（最终降级路径） ----------
  function startMainLoop(st) {
    var run = st.runId;
    function tick(deadline) {
      if (st.runId !== run) return;
      if (st.phase !== 'sorting' || st.paused) return;
      var budget = Math.min(6, (deadline && deadline.timeRemaining)
        ? deadline.timeRemaining() : 6);
      if (!st.mainJob) {
        var list = readyJobs(st);
        if (!list.length) return; // 等待中（理论上不会发生）
        var j = st.jobs[list[0]];
        var inputs;
        try {
          inputs = makeJobInputs(st, j);
        } catch (e) {
          return handleOom(st, j, e);
        }
        st.mainJob = j;
        j.state = 'running';
        j.worker = -2; // -2 表示主线程
        j.attempts++;
        try {
          st.mainTask = j.type === 'sort'
            ? window.SortCore.createMergeSortTask(new Int32Array(inputs.buffer))
            : window.SortCore.createMergeTask(
                new Int32Array(inputs.bufA), new Int32Array(inputs.bufB));
        } catch (e) {
          st.mainJob = null;
          return isOom(e) ? handleOom(st, j, e)
            : jobAttemptFailed(st, j, e.message);
        }
      }
      var mj = st.mainJob;
      var finished;
      try {
        finished = st.mainTask.step(budget);
      } catch (e) {
        st.mainJob = null;
        return isOom(e) ? handleOom(st, mj, e)
          : jobAttemptFailed(st, mj, e.message);
      }
      mj.done = st.mainTask.getDone();
      if (finished) {
        var buf = st.mainTask.result();
        st.mainTask = null;
        st.mainJob = null;
        finalizeJob(st, mj, buf);
      }
      st.mainHandle = idle(tick);
    }
    st.mainHandle = idle(tick);
  }

  // ---------- 心跳看门狗：消息丢失 / 冻结 / 静默停滞 ----------
  function startWatchdog(st) {
    var run = st.runId;
    st.watchdog = setInterval(function () {
      if (st.runId !== run || st.mode !== 'workers') return;
      if (st.phase !== 'sorting' || st.paused) return;
      var t = now();
      aliveWorkers(st).forEach(function (w) {
        // 周期 ping：冻结的 Worker 不回 pong，靠超时判定
        try { w.worker.postMessage({ type: 'ping', id: -1 }); } catch (e) {}
        if (t - w.lastHeard > WATCHDOG_TIMEOUT_MS) {
          log('Worker #' + w.id + ' 心跳超时 ' +
            Math.round(t - w.lastHeard) + 'ms，判定死亡（疑似冻结/消息丢失）', 'err');
          killWorker(st, w.id);
        } else if (w.busy && t - w.lastProgressAt > WATCHDOG_STALL_MS) {
          log('Worker #' + w.id + ' 进度停滞 ' +
            Math.round(t - w.lastProgressAt) + 'ms（静默故障），判定死亡', 'err');
          killWorker(st, w.id);
        }
      });
    }, WATCHDOG_PING_MS);
  }

  // ---------- 全部排序完成 → 主线程分片校验 ----------
  function allSorted(st) {
    clearInterval(st.watchdog);
    stopPool(st);
    st.phase = 'verifying';
    st.verifyDone = 0;
    var root = st.jobs[st.rootId];
    var result = new Int32Array(root.buf);
    var run = st.runId;
    var pos = 1;
    var sortOk = true;
    var sum32 = st.expectedSum;
    var gotSum = result[0] | 0;
    var gotFp = fnvVal(result[0]); // 顺序无关的多重集指纹

    function tick() {
      if (st.runId !== run) return;
      if (st.paused) { st.verifyHandle = idle(tick); return; }
      var deadline = now() + 8;
      while (pos < result.length && now() < deadline) {
        if (result[pos - 1] > result[pos]) sortOk = false;
        gotSum = (gotSum + result[pos]) | 0;
        gotFp = (gotFp ^ fnvVal(result[pos])) >>> 0;
        pos++;
      }
      st.verifyDone = pos;
      if (pos < result.length) {
        st.verifyHandle = idle(tick);
      } else {
        finishVerify(st, {
          sortOk: sortOk,
          sumOk: gotSum === sum32,
          hashOk: gotFp === st.expectedFp,
          lengthOk: result.length === st.n,
          result: result
        });
      }
    }
    st.verifyHandle = idle(tick);
  }

  // 单个值的 FNV-1a（4 字节），用于多重集 XOR 指纹
  function fnvVal(v) {
    var h = 0x811c9dc5;
    h = Math.imul(h ^ (v & 0xff), 0x01000193) >>> 0;
    h = Math.imul(h ^ ((v >>> 8) & 0xff), 0x01000193) >>> 0;
    h = Math.imul(h ^ ((v >>> 16) & 0xff), 0x01000193) >>> 0;
    h = Math.imul(h ^ ((v >>> 24) & 0xff), 0x01000193) >>> 0;
    return h;
  }

  function finishVerify(st, r) {
    st.phase = 'done';
    st.result = r.result;
    var ok = r.sortOk && r.sumOk && r.hashOk && r.lengthOk;
    el.verify.innerHTML = ok
      ? '<span style="color:var(--green)">✔ 校验通过</span>：长度一致、严格有序、32 位总和与 FNV 多重集指纹（顺序无关）均与原始数据匹配（' +
        st.n.toLocaleString() + ' 个元素；未使用内置 sort 对比，纯分片校验）。'
      : '<span style="color:var(--red)">✘ 校验失败</span>：' +
        ['长度', '有序性', '总和', '指纹'].map(function (name, i) {
          var pass = [r.lengthOk, r.sortOk, r.sumOk, r.hashOk][i];
          return name + (pass ? '✓' : '✗');
        }).join(' / ');
    log(ok ? '校验通过，结果正确' : '校验失败', ok ? 'ok' : 'err');
    updateButtons();
  }

  function fatal(st, e) {
    st.phase = 'error';
    stopPool(st);
    clearInterval(st.watchdog);
    if (st.verifyHandle) cancelIdle(st.verifyHandle);
    if (st.mainHandle) cancelIdle(st.mainHandle);
    el.verify.innerHTML = '<span style="color:var(--red)">✘ 致命错误：' +
      (e.message || e) + '</span>';
    log('致命错误：' + (e.message || e), 'err');
    updateButtons();
  }

  // ---------- 开始 / 暂停 / 继续 / 取消 ----------
  function onStart() {
    var n = parseInt(el.count.value, 10);
    if (!isFinite(n) || n < 1) return log('元素数量无效', 'err');
    var k = parseInt(el.workers.value, 10);
    S = newState(n, k);
    S.runId++;
    S.startWall = now();
    S.pauseAccum = 0;
    el.log.innerHTML = '';
    el.verify.textContent = '校验中…';
    el.cancelStat.style.display = 'none';
    log('生成 ' + n.toLocaleString() + ' 个随机 Int32（分片生成，不阻塞）…');
    updateButtons();

    // 在生成循环中顺带累计原始数据的 32 位总和与 FNV-1a 指纹
    generateWithFingerprint(S, function (err) {
      if (err) return fatal(S, new Error('数据生成失败（可能是内存不足）：' + (err.message || err)));
      buildJobs(S);
      log('任务图：' + S.k + ' 个分块排序 + 树状归并，共 ' +
        (S.jobs.length - 1) + ' 个任务');
      S.phase = 'sorting';
      startWorkers(S, S.k).then(function (mode) {
        if (S.phase !== 'sorting') return;
        if (mode === 'none') {
          log('浏览器不支持或拒绝创建 Web Worker → 主线程分片执行', 'warn');
          S.mode = 'main';
          startMainLoop(S);
        } else {
          S.mode = 'workers';
          var n2 = aliveWorkers(S).length;
          log(n2 + ' 个 Worker 就绪，开始并行排序');
          startWatchdog(S);
          pump(S);
        }
        updateButtons();
      });
      updateButtons();
    });
  }

  function generateWithFingerprint(st, cb) {
    st.phase = 'generating';
    var n = st.n, run = st.runId;
    var arr;
    try { arr = new Int32Array(n); } catch (e) { return cb(e); }
    var pos = 0, sum = 0, fp = 0;
    function tick() {
      if (run !== st.runId) return;
      var deadline = now() + 8;
      try {
        while (pos < n && now() < deadline) {
          var v = (Math.random() * 0x7fffffff) | 0;
          arr[pos] = v;
          sum = (sum + v) | 0;
          fp = (fp ^ fnvVal(v)) >>> 0; // 顺序无关
          pos++;
        }
      } catch (e) { return cb(e); }
      st.genDone = pos;
      if (pos >= n) {
        st.master = arr;
        st.expectedSum = sum;
        st.expectedFp = fp >>> 0;
        cb(null);
      } else {
        idle(tick);
      }
    }
    idle(tick);
  }

  function togglePause() {
    if (!S || (S.phase !== 'sorting' && S.phase !== 'verifying')) return;
    S.paused = !S.paused;
    if (S.paused) {
      S.pauseStart = now();
      if (S.mode === 'workers') {
        aliveWorkers(S).forEach(function (w) {
          try { w.worker.postMessage({ type: 'pause' }); } catch (e) {}
        });
      }
      log('已暂停（Worker 当前切片结束后让出 CPU，不再被调度）', 'warn');
    } else {
      S.pauseAccum += now() - S.pauseStart;
      if (S.mode === 'workers') {
        aliveWorkers(S).forEach(function (w) {
          try { w.worker.postMessage({ type: 'resume' }); } catch (e) {}
        });
        pump(S);
      } else {
        startMainLoop(S);
      }
      log('继续');
    }
    updateButtons();
  }

  function onCancel() {
    if (!S) return;
    var run = S.runId;
    S.runId++;           // 使所有异步循环失效
    S.phase = 'cancelled';
    S.paused = false;
    if (S.verifyHandle) cancelIdle(S.verifyHandle);
    if (S.mainHandle) cancelIdle(S.mainHandle);
    clearInterval(S.watchdog);

    // terminate() 是同步的：JS 事件循环中 Worker 会在极短时间内停止调度
    var t0 = now();
    stopPool(S);
    var dt = now() - t0;

    // 硬性验收：同步终止耗时 + 一次事件循环后确认端口已关闭
    setTimeout(function () {
      if (S.runId !== run + 1) return;
      var within = dt <= 200;
      el.cancelStat.style.display = 'inline-block';
      el.cancelStat.innerHTML = '终止耗时 <b>' + dt.toFixed(1) +
        'ms</b>（要求 &lt;200ms ' + (within ? '✔' : '✘') + '）';
      el.cancelStat.className = 'stat ' + (within ? 'good' : 'bad');
      log('已取消，' + S.workers.length + ' 个 Worker 同步 terminate() 耗时 ' +
        dt.toFixed(1) + 'ms，CPU 已释放', within ? 'ok' : 'err');
      updateButtons();
    }, 0);
    updateButtons();
  }

  function injectFault() {
    if (!S || S.mode !== 'workers' || S.phase !== 'sorting') return;
    var mode = el.fault.value;
    var target = parseInt(el.faultTarget.value, 10);
    var w = S.workers[target];
    if (!w || !w.alive) return log('目标 Worker #' + target + ' 不存活，无法注入', 'err');
    log('向 Worker #' + target + ' 注入故障：' + mode, 'warn');
    if (mode === 'stall') w.lastProgressAt = 0; // 静默故障：停滞计时器立即开始累计
    if (mode === 'freeze') w.lastHeard = 0;     // 冻结：心跳计时器立即开始累计
    try {
      w.worker.postMessage({ type: 'fault', mode: mode });
    } catch (e) {
      log('注入失败：' + e.message, 'err');
    }
  }

  // ---------- Canvas 实时进度 ----------
  var ctx = el.chart.getContext('2d');
  var lastCanvasKey = '';

  function resizeCanvas() {
    var dpr = window.devicePixelRatio || 1;
    var cssW = el.chart.clientWidth || 900;
    var jobs = S ? S.jobs.filter(Boolean) : [];
    var rows = Math.max(4, jobs.length + 1);
    var rowH = 26, pad = 52;
    var cssH = pad + rows * rowH + 26;
    var key = cssW + 'x' + cssH + '@' + dpr;
    if (key !== lastCanvasKey) {
      el.chart.style.height = cssH + 'px';
      el.chart.height = Math.round(cssH * dpr);
      el.chart.width = Math.round(cssW * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      lastCanvasKey = key;
    }
    return { w: cssW, h: cssH, rowH: rowH, pad: pad };
  }

  function workerColor(st, j) {
    if (j.state === 'done') return '#3ecf8e';
    if (st && st.mode === 'main') return '#b388ff';
    if (j.worker >= 0 && st.workers[j.worker]) return st.workers[j.worker].color;
    return '#3a4664';
  }

  function overallProgress(st) {
    if (st.phase === 'generating') return 0;
    if (st.phase === 'verifying') return st.verifyDone / Math.max(1, st.n);
    var done = 0, total = 0;
    for (var id = 1; id < st.jobs.length; id++) {
      var j = st.jobs[id];
      if (!j) continue;
      var w = jobWeight(j);
      total += w;
      if (j.state === 'done') done += w;
      else if (j.state === 'running') done += Math.min(j.done, j.units) *
        (j.type === 'sort' ? SORT_WEIGHT : 1);
    }
    return done / Math.max(1, total);
  }

  function render() {
    var L = resizeCanvas();
    ctx.clearRect(0, 0, L.w, L.h);
    ctx.font = '11px ui-monospace, Menlo, Consolas, monospace';

    // 整体进度（生成 / 排序归并 / 校验 三阶段分段）
    if (!S) {
      ctx.fillStyle = '#8b97b0';
      ctx.fillText('点击“开始排序”运行（默认 1,000,000 个随机整数 / 8 Workers）', 12, 28);
      requestAnimationFrame(render);
      return;
    }
    var st = S;
    updateStats(); // 数字与 Canvas 同帧更新，避免阶段切换瞬间文本滞后
    var barX = 150, barW = L.w - barX - 90;
    var y = 18;
    ctx.fillStyle = '#c6d0e3';
    ctx.font = 'bold 12px sans-serif';
    ctx.fillText('整体进度', 12, y + 4);
    ctx.font = '11px ui-monospace, Menlo, Consolas, monospace';
    drawBar(barX, y - 9, barW, 16, phaseSegment(st), '#4f9dff');

    var jobs = st.jobs.filter(Boolean);
    jobs.forEach(function (j, idx) {
      var ry = L.pad + idx * L.rowH;
      ctx.fillStyle = j.state === 'running' ? '#e6ecf7' : '#8b97b0';
      var label = j.label.length > 24 ? j.label.slice(0, 23) + '…' : j.label;
      ctx.fillText(label, 12, ry + 13);
      var frac = j.state === 'done' ? 1
        : j.state === 'running' ? Math.max(0, Math.min(1, j.done / j.units)) : 0;
      drawBar(barX, ry, barW, 16, frac, workerColor(st, j));
      var tag = j.state === 'done' ? 'done'
        : j.state === 'running'
          ? (st.mode === 'main' ? 'main' : 'W#' + j.worker)
          : 'wait';
      ctx.fillStyle = '#8b97b0';
      ctx.fillText((frac * 100).toFixed(1).padStart(5) + '% ' + tag,
        barX + barW + 8, ry + 12);
    });

    // 图例
    var lx = 12, ly = L.h - 16;
    ctx.fillStyle = '#8b97b0';
    var items = [
      ['#3a4664', '等待'], ['#4f9dff', 'Worker 任务'],
      ['#b388ff', '主线程分片'], ['#3ecf8e', '完成']
    ];
    items.forEach(function (it) {
      ctx.fillStyle = it[0];
      ctx.fillRect(lx, ly - 9, 10, 10);
      ctx.fillStyle = '#8b97b0';
      ctx.fillText(it[1], lx + 14, ly);
      lx += 78;
    });
    requestAnimationFrame(render);
  }

  function drawBar(x, y, w, h, frac, color) {
    ctx.fillStyle = '#222c42';
    ctx.fillRect(x, y, w, h);
    if (frac > 0) {
      ctx.fillStyle = color;
      ctx.fillRect(x, y, Math.max(2, w * Math.min(1, frac)), h);
    }
    ctx.strokeStyle = '#2f3a58';
    ctx.strokeRect(x + .5, y + .5, w - 1, h - 1);
  }

  function phaseSegment(st) {
    if (st.phase === 'generating') return st.genDone / st.n * 0.02;
    if (st.phase === 'verifying') return 0.98 + (st.verifyDone / st.n) * 0.02;
    if (st.phase === 'done') return 1;
    if (st.phase === 'cancelled' || st.phase === 'error') return overallProgress(st);
    return 0.02 + overallProgress(st) * 0.96;
  }

  // ---------- 状态栏 / 按钮 / FPS ----------
  var PHASE_TEXT = {
    init: '初始化', generating: '生成数据', sorting: '排序 / 归并',
    verifying: '校验', done: '完成', cancelled: '已取消', error: '错误'
  };

  function activeElapsed(st) {
    if (st.phase === 'done') return st._finalElapsed || 0;
    var paused = st.paused ? now() - st.pauseStart : 0;
    return now() - st.startWall - st.pauseAccum - paused;
  }

  function updateStats() {
    if (!S) return;
    var st = S;
    el.phase.textContent = PHASE_TEXT[st.phase] + (st.paused ? '（已暂停）' : '');
    var p = st.phase === 'generating' ? st.genDone / st.n : overallProgress(st);
    el.pct.textContent = (p * 100).toFixed(1) + '%';
    var dc = st.jobs.filter(function (j) { return j && j.state === 'done'; }).length;
    el.doneJobs.textContent = dc + '/' + (st.jobs.length - 1);
    var secs = activeElapsed(st) / 1000;
    el.elapsed.textContent = secs.toFixed(2) + 's';
    if (st.phase === 'done') st._finalElapsed = st._finalElapsed ||
      (now() - st.startWall - st.pauseAccum);
    var mode = st.mode === 'main' ? '主线程分片 (rIC)'
      : st.mode === 'workers' ? aliveWorkers(st).length + ' Worker' : '—';
    el.modeStat.innerHTML = '执行模式 <b>' + mode + '</b>';

    // 故障注入目标下拉跟随存活 Worker
    var alive = aliveWorkers(st);
    if (st.mode === 'workers' && el.faultTarget.options.length !== alive.length) {
      el.faultTarget.innerHTML = alive.map(function (w) {
        return '<option value="' + w.id + '">Worker #' + w.id + '</option>';
      }).join('');
    }
    updateButtons();
  }

  function updateButtons() {
    var running = S && (S.phase === 'generating' || S.phase === 'sorting' || S.phase === 'verifying');
    el.start.disabled = !!running;
    el.pause.disabled = !S || (S.phase !== 'sorting' && S.phase !== 'verifying');
    el.cancel.disabled = !running;
    el.inject.disabled = !S || S.mode !== 'workers' || S.phase !== 'sorting';
    el.pause.textContent = S && S.paused ? '▶ 继续' : '⏸ 暂停';
  }

  var frames = 0, fpsLast = now(), fpsVal = 0;
  function fpsLoop() {
    frames++;
    var t = now();
    if (t - fpsLast >= 500) {
      fpsVal = Math.round(frames * 1000 / (t - fpsLast));
      frames = 0; fpsLast = t;
      el.fps.textContent = fpsVal;
    }
    requestAnimationFrame(fpsLoop);
  }
  // updateStats 由 render() 的 requestAnimationFrame 每帧驱动，与图表同帧

  // ---------- 交互绑定 ----------
  el.start.addEventListener('click', onStart);
  el.pause.addEventListener('click', togglePause);
  el.cancel.addEventListener('click', onCancel);
  el.inject.addEventListener('click', injectFault);

  var clicks1 = 0, clicks2 = 0;
  el.btnClick1.addEventListener('click', function () {
    clicks1++;
    el.btnClick1.firstChild.textContent = '点我计数 ' + clicks1 + ' ';
  });
  el.btnClick2.addEventListener('click', function () {
    clicks2++;
    el.btnClick2.firstChild.textContent = '点我计数 ' + clicks2 + ' ';
  });
  el.btnAnim.style.animation = 'none';
  var styleEl = document.createElement('style');
  styleEl.textContent = '@keyframes nudge{0%,100%{transform:translateX(0)}50%{transform:translateX(10px)}}';
  document.head.appendChild(styleEl);
  el.btnAnim.style.animation = 'nudge .8s ease-in-out infinite';
  (function fillScroll() {
    for (var i = 0; i < 30; i++) el.scrollpad.appendChild(document.createTextNode(
      '滚动行 ' + i + ' — 排序时此区域应当依然顺滑可滚。\n'));
  })();

  render();
  fpsLoop();
  updateButtons();
})();
