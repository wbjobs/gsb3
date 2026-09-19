/*
 * 主线程控制器。
 * 阶段：idle -> generating -> ready -> sorting -> merging -> verifying -> done
 *                 任意阶段可暂停/继续/取消。
 *
 * 降级链路：
 *   Worker 创建失败 -> 减少 Worker 数 / 单 Worker
 *   全部失败 / 运行中全部死亡 / OOM 重试耗尽 -> 主线程 requestIdleCallback 分片
 *
 * 主线程绝不做长同步任务：生成、排序、归并、校验全部按时间片推进。
 */
(function () {
  'use strict';

  var SLICE_MS = 9;             // 每个时间片最多占用主线程 9ms
  var SORT_WEIGHT = 0.8;        // 整体进度中“排序”的权重
  var MERGE_WEIGHT = 0.18;
  var VERIFY_WEIGHT = 0.02;
  var MAX_SPAWN_ATTEMPTS = 2;

  // -------- requestIdleCallback 垫片 --------
  var ric = window.requestIdleCallback || function (cb) {
    var start = performance.now();
    return setTimeout(function () {
      cb({ didTimeout: false, timeRemaining: function () { return Math.max(0, 16 - (performance.now() - start)); } });
    }, 1);
  };
  var cic = window.cancelIdleCallback || function (id) { clearTimeout(id); };

  // -------- DOM --------
  var $ = function (id) { return document.getElementById(id); };
  var els = {
    size: $('size'), workerCount: $('workerCount'),
    btnGenerate: $('btnGenerate'), btnStart: $('btnStart'),
    btnPause: $('btnPause'), btnCancel: $('btnCancel'),
    state: $('stateText'), sortPhase: $('sortPhaseText'), mergePhase: $('mergePhaseText'),
    overall: $('overallText'), elapsed: $('elapsedText'), stopLatency: $('stopLatencyText'),
    mode: $('modeText'), result: $('resultText'),
    chFailCreate: $('chFailCreate'), chFailCreateAll: $('chFailCreateAll'),
    chStall: $('chStall'), chOom: $('chOom'),
    btnTickle: $('btnTickle'), tickleCount: $('tickleCount'), frameMs: $('frameMsText'),
    log: $('log'), canvas: $('chart')
  };

  var canvas = els.canvas;
  var ctx = canvas.getContext('2d');

  // -------- 运行时状态 --------
  var phase = 'idle';
  var paused = false;
  var cancelled = false;
  var n = 0;
  var master = null;            // Int32Array 主数据（生成后保留到开始排序）
  var inputChecksum = 0;
  var chunks = [];              // {id, size, progress, status, attempts}
  var sortedChunks = [];        // 按 id 存放 Int32Array
  var pool = null;
  var gen = null;               // 生成驱动器
  var mainDrivers = [];         // 主线程降级排序引擎
  var driverCursor = 0;
  var merger = null;
  var verifier = null;
  var idleHandle = null;
  var startTime = 0;
  var pauseAccum = 0;
  var pauseStart = 0;
  var elapsedTimer = null;
  var tickles = 0;
  var lastFrame = performance.now();
  var frameBudget = 0;
  var genProgress = 0;

  // ===================================================================
  // 工具
  // ===================================================================
  function log(msg) {
    var line = document.createElement('div');
    var t = new Date().toLocaleTimeString('zh-CN', { hour12: false }) + '.' +
            String(Date.now() % 1000).padStart(3, '0');
    line.textContent = '[' + t + '] ' + msg;
    els.log.appendChild(line);
    while (els.log.childNodes.length > 200) els.log.removeChild(els.log.firstChild);
    els.log.scrollTop = els.log.scrollHeight;
  }

  function schedule(fn) {
    idleHandle = ric(function (deadline) {
      idleHandle = null;
      fn(deadline);
    }, { timeout: 30 });
  }

  function desiredWorkers() {
    var v = parseInt(els.workerCount.value, 10);
    if (v >= 4 && v <= 8) return v;
    var hw = navigator.hardwareConcurrency || 4;
    return Math.max(4, Math.min(8, hw));
  }

  function splitRanges(total, parts) {
    var ranges = [];
    var base = Math.floor(total / parts);
    var rem = total % parts;
    var start = 0;
    for (var i = 0; i < parts; i++) {
      var size = base + (i < rem ? 1 : 0);
      ranges.push({ start: start, size: size });
      start += size;
    }
    return ranges;
  }

  // ===================================================================
  // 数据生成（主线程时间片）
  // ===================================================================
  function startGenerate() {
    resetRun();
    n = parseInt(els.size.value, 10);
    phase = 'generating';
    paused = false;
    cancelled = false;
    updateButtons();
    log('开始生成 ' + n.toLocaleString() + ' 个随机整数…');

    var buf;
    try {
      buf = new ArrayBuffer(n * 4);
    } catch (e) {
      failFatal('内存不足，无法分配数据缓冲: ' + e.message);
      return;
    }
    master = new Int32Array(buf);
    inputChecksum = 0;

    var cursor = 0;
    gen = {
      step: function () {
        var deadline = performance.now() + SLICE_MS;
        while (cursor < n && performance.now() < deadline) {
          var end = Math.min(cursor + 8192, n);
          for (var i = cursor; i < end; i++) master[i] = (Math.random() * 0x7fffffff) | 0;
          cursor = end;
        }
        // 分批算校验和（避免额外一整趟同步遍历）
        return cursor / n;
      }
    };

    startTime = performance.now();
    pauseAccum = 0;
    startElapsedTimer();
    runGenerateLoop(0);
  }

  function runGenerateLoop(p) {
    if (cancelled) return;
    if (paused) { schedule(function () { runGenerateLoop(p); }); return; }
    schedule(function () {
      if (cancelled) return;
      if (paused) { runGenerateLoop(p); return; }
      var prog;
      try { prog = gen.step(); } catch (e) { failFatal('生成失败: ' + e.message); return; }
      genProgress = prog;
      if (prog >= 1) finishGenerate();
      else runGenerateLoop(prog);
    });
  }

  function finishGenerate() {
    // 一整趟校验和仍可能 ~10ms，分批做
    var cur = 0;
    function batch() {
      if (cancelled) return;
      if (paused) { schedule(batch); return; }
      schedule(function () {
        if (cancelled) return;
        if (paused) { batch(); return; }
        var deadline = performance.now() + SLICE_MS;
        while (cur < n && performance.now() < deadline) {
          var end = Math.min(cur + 100000, n);
          for (var i = cur; i < end; i++) inputChecksum = (inputChecksum + master[i]) >>> 0;
          cur = end;
        }
        if (cur < n) { batch(); return; }
        phase = 'ready';
        gen = null;
        log('数据就绪，校验和 0x' + inputChecksum.toString(16) + '，点击“开始排序”');
        updateButtons();
        draw();
      });
    }
    batch();
  }

  // ===================================================================
  // 开始排序：切片 + Worker 池
  // ===================================================================
  function startSort() {
    if (!master) return;
    phase = 'sorting';
    paused = false;
    cancelled = false;

    var wc = desiredWorkers();
    var ranges = splitRanges(n, wc);
    chunks = ranges.map(function (r, i) {
      return { id: i, start: r.start, size: r.size, progress: 0, status: 'pending', attempts: 0 };
    });
    sortedChunks = new Array(wc);

    // 每块一份拷贝（零拷贝 transfer 给 Worker，主数据保留用于“卡死丢数据”重切与取消）
    var chunkPayloads = [];
    for (var i = 0; i < chunks.length; i++) {
      var c = chunks[i];
      var cb;
      try {
        cb = new ArrayBuffer(c.size * 4);
      } catch (e) {
        failFatal('内存不足，无法切分分块: ' + e.message);
        return;
      }
      new Int32Array(cb).set(master.subarray(c.start, c.start + c.size));
      c.status = 'queued';
      chunkPayloads.push({
        id: c.id,
        attempt: 0,
        buffer: cb,
        simulate: simulationFor(c.id)
      });
    }

    startTime = performance.now();
    pauseAccum = 0;
    startElapsedTimer();

    pool = new WorkerPool(wc, {
      rawFactory: makeWorkerFactory(),
      log: log,
      onProgress: onChunkProgress,
      onChunkRetried: function (id, attempt) {
        var c = chunks[id];
        if (c && c.status !== 'done') { c.status = 'queued'; c.progress = 0; c.attempts = attempt; }
      },
      onSorted: onChunkSorted,
      onChunkLost: onChunkLost,
      onChunkFatal: onChunkFatal,
      onAllWorkersDead: function () { fallbackToMain('所有 Worker 已死亡'); }
    });

    var alive = pool.init();
    if (alive === 0) {
      // 整体降级：分块缓冲还在 pending 里，取回来交给主线程
      var pending = pool.pending.splice(0, pool.pending.length);
      pool = null;
      fallbackToMainWith('Worker 全部创建失败', pending);
      return;
    }
    els.mode.textContent = alive + ' 个 Worker';
    pool.addChunks(chunkPayloads);
    updateButtons();
    draw();
  }

  // 故障注入：只针对分块 #0
  function simulationFor(id) {
    if (id !== 0) return {};
    var sim = {};
    if (els.chOom.checked) sim.oom = true;
    if (els.chStall.checked) { sim.stall = true; sim.stallMs = 600000; }
    return sim;
  }

  var spawnAttempts = 0;
  function makeWorkerFactory() {
    spawnAttempts = 0;
    var all = els.chFailCreateAll.checked;
    var partial = els.chFailCreate.checked && !all;
    return function () {
      spawnAttempts++;
      if (all) throw new Error('simulated worker creation failure');
      if (partial && spawnAttempts <= MAX_SPAWN_ATTEMPTS) {
        throw new Error('simulated transient worker creation failure #' + spawnAttempts);
      }
      return new Worker('js/sort-worker.js');
    };
  }

  // ---------- Worker 回调 ----------
  function onChunkProgress(id, attempt, p) {
    var c = chunks[id];
    if (!c || c.status === 'done') return;
    c.status = 'sorting';
    c.progress = Math.max(c.progress, p);
  }

  function onChunkSorted(id, attempt, buffer, ms) {
    var c = chunks[id];
    if (!c || c.status === 'done') return;
    c.status = 'done';
    c.progress = 1;
    sortedChunks[id] = new Int32Array(buffer);
    log('分块 #' + id + ' 排序完成（' + c.size.toLocaleString() + ' 个，' + ms.toFixed(0) + 'ms）');
    if (chunks.every(function (x) { return x.status === 'done'; })) finishSorting();
  }

  // Worker 卡死被强杀：buffer 随之丢失，用主数据重新切片重排
  function onChunkLost(id, attempt) {
    var c = chunks[id];
    if (!c || c.status === 'done') return;
    c.attempts = attempt + 1;
    if (c.attempts > 4) {
      onChunkFatal(id, '分块多次丢失');
      return;
    }
    c.status = 'queued';
    c.progress = 0;
    var cb;
    try {
      cb = new ArrayBuffer(c.size * 4);
      new Int32Array(cb).set(master.subarray(c.start, c.start + c.size));
    } catch (e) {
      // 内存不足也走降级
      fallbackToMain('分块 #' + id + ' 重新切片内存不足: ' + e.message);
      return;
    }
    if (pool) pool.addChunks([{ id: id, attempt: c.attempts, buffer: cb, simulate: {} }]);
  }

  // Worker 内部 OOM 等致命错误且重试耗尽：整体降级主线程，剩余分块由主线程完成
  function onChunkFatal(id, reason) {
    log('分块 #' + id + ' 失败: ' + reason + '，整体降级主线程');
    fallbackToMain('分块 #' + id + ' 致命错误: ' + reason);
  }

  // ===================================================================
  // 主线程降级：剩余分块用 MergeSortEngine 时间片执行
  // ===================================================================
  function fallbackToMain(reason) {
    if (!pool || phase !== 'sorting') return;
    // 收走尚未派发的分块缓冲
    var pending = pool.pending.splice(0, pool.pending.length);
    pool.shutdown();
    pool = null;
    fallbackToMainWith(reason, pending);
  }

  function fallbackToMainWith(reason, pendingPayloads) {
    phase = 'sorting';
    els.mode.textContent = '主线程分片（已降级）';
    log('降级到主线程执行: ' + reason);

    var byId = {};
    pendingPayloads.forEach(function (p) { byId[p.id] = p.buffer; });

    mainDrivers = [];
    driverCursor = 0;

    try {
      for (var i = 0; i < chunks.length; i++) {
        var c = chunks[i];
        if (c.status === 'done') continue;
        var buf = byId[c.id];
        if (!buf) {
          // 缓冲已转给死掉的 Worker：从主数据重切
          buf = new ArrayBuffer(c.size * 4);
          new Int32Array(buf).set(master.subarray(c.start, c.start + c.size));
        }
        c.status = 'sorting';
        var engine = new MergeSortEngine(buf);
        engine.chunkId = c.id;
        mainDrivers.push(engine);
      }
    } catch (e) {
      failFatal('降级到主线程时内存不足: ' + (e.message || e));
      return;
    }
    updateButtons();
    runMainDrivers();
  }

  function runMainDrivers() {
    if (cancelled) return;
    if (paused) { schedule(runMainDrivers); return; }
    schedule(function () {
      if (cancelled) return;
      if (phase !== 'sorting') return;
      if (paused) { runMainDrivers(); return; }

      var engine = mainDrivers[driverCursor];
      if (!engine) { finishSorting(); return; }
      var c = chunks[engine.chunkId];
      var res = engine.step(SLICE_MS);
      if (c) c.progress = engine.progress();
      if (res.done) {
        var outBuf = engine.getResultBuffer();
        sortedChunks[engine.chunkId] = new Int32Array(outBuf);
        c.status = 'done';
        c.progress = 1;
        log('分块 #' + engine.chunkId + ' 主线程排序完成');
        mainDrivers.splice(driverCursor, 1);
        if (mainDrivers.length === 0) { finishSorting(); return; }
        driverCursor = driverCursor % mainDrivers.length;
      } else {
        // 轮换分块，避免单块独占所有时间片
        driverCursor = (driverCursor + 1) % mainDrivers.length;
      }
      runMainDrivers();
    });
  }

  // ===================================================================
  // k 路归并（主线程时间片）
  // ===================================================================
  function finishSorting() {
    if (cancelled) return;
    // 排序结束立即回收全部 Worker（归并在主线程做），避免取消时遗留线程
    if (pool) { pool.shutdown(); pool = null; }
    phase = 'merging';
    log('所有分块排序完成，开始 ' + sortedChunks.length + ' 路归并…');
    var outBuf;
    try {
      outBuf = new ArrayBuffer(n * 4);
    } catch (e) {
      failFatal('内存不足，无法分配归并输出缓冲: ' + e.message);
      return;
    }
    try {
      merger = new KWayMerger(sortedChunks, outBuf);
    } catch (e) {
      failFatal('归并初始化失败: ' + e.message);
      return;
    }
    updateButtons();
    runMerge();
  }

  function runMerge() {
    if (cancelled) return;
    if (paused) { schedule(runMerge); return; }
    schedule(function () {
      if (cancelled) return;
      if (phase !== 'merging') return;
      if (paused) { runMerge(); return; }
      var res;
      try { res = merger.step(SLICE_MS); }
      catch (e) { failFatal('归并失败: ' + e.message); return; }
      if (res.done) finishMerge();
      else runMerge();
    });
  }

  function finishMerge() {
    phase = 'verifying';
    // 排序结果已在一块连续缓冲；各分块缓冲可释放
    var finalArr = merger.out;
    merger = null;
    sortedChunks = [];
    log('归并完成，开始校验…');

    var cursor = 0;
    var gotSum = 0;
    var orderOk = true;
    verifier = {
      step: function () {
        var deadline = performance.now() + SLICE_MS;
        while (cursor < n && performance.now() < deadline) {
          var end = Math.min(cursor + 50000, n);
          for (var i = cursor; i < end; i++) {
            gotSum = (gotSum + finalArr[i]) >>> 0;
            if (orderOk && i > 0 && finalArr[i - 1] > finalArr[i]) orderOk = false;
          }
          cursor = end;
        }
        return cursor / n;
      },
      finish: function () {
        var sumOk = gotSum === inputChecksum;
        return { finalArr: finalArr, orderOk: orderOk, sumOk: sumOk, gotSum: gotSum };
      }
    };
    updateButtons();
    runVerify();
  }

  function runVerify() {
    if (cancelled) return;
    if (paused) { schedule(runVerify); return; }
    schedule(function () {
      if (cancelled) return;
      if (phase !== 'verifying') return;
      if (paused) { runVerify(); return; }
      var p = verifier.step();
      if (p >= 1) {
        var r = verifier.finish();
        verifier = null;
        phase = 'done';
        stopElapsedTimer();
        var secs = ((performance.now() - startTime - pauseAccum) / 1000).toFixed(2);
        els.result.textContent =
          (r.orderOk && r.sumOk ? '✅ 正确（升序 + 校验和一致）' : '❌ 数据异常') +
          '  长度=' + n.toLocaleString() +
          '  校验和=0x' + r.gotSum.toString(16) +
          '  总耗时=' + secs + 's';
        if (!r.orderOk) els.result.textContent += ' [顺序错误]';
        if (!r.sumOk) els.result.textContent += ' [校验和不一致]';
        log('完成：' + (r.orderOk && r.sumOk ? '结果正确' : '结果异常') + '，耗时 ' + secs + 's');
        updateButtons();
        draw();
      } else {
        runVerify();
      }
    });
  }

  // ===================================================================
  // 暂停 / 继续 / 取消
  // ===================================================================
  function pauseAll() {
    if (paused || cancelled) return;
    paused = true;
    pauseStart = performance.now();
    if (pool) pool.pause();
    log('已请求暂停（时间片边界生效）');
    updateButtons();
  }

  function resumeAll() {
    if (!paused || cancelled) return;
    paused = false;
    pauseAccum += performance.now() - pauseStart;
    if (pool) pool.resume();
    log('继续');
    updateButtons();
  }

  function cancelAll() {
    if (cancelled) return;
    if (phase === 'ready') {
      phase = 'idle';
      releaseData();
      log('已取消（尚未开始排序，无 Worker）');
      els.state.textContent = '已取消';
      updateButtons();
      return;
    }
    if (phase === 'idle' || phase === 'done') return;
    cancelled = true;
    paused = false;
    if (idleHandle) { cic(idleHandle); idleHandle = null; }
    var t0 = performance.now();
    log('取消中…');

    if (pool) {
      var p = pool;
      pool = null;
      p.cancel().then(function (latency) {
        reportCancelled(performance.now() - t0, latency);
      });
    } else {
      // 无 Worker（主线程模式或生成阶段）：rIC 已撤销，0 CPU 残留
      reportCancelled(performance.now() - t0, 0);
    }
  }

  function reportCancelled(total, workerLatency) {
    phase = 'idle';
    stopElapsedTimer();
    chunks.forEach(function (c) { if (c.status !== 'done') c.status = 'cancelled'; });
    log('已取消，Worker 停止耗时 ' + workerLatency.toFixed(1) + 'ms（上限 200ms，含 100ms 强杀保底）');
    els.stopLatency.textContent = workerLatency.toFixed(1) + ' ms';
    els.state.textContent = '已取消';
    releaseData();
    updateButtons();
    draw();
  }

  function failFatal(msg) {
    cancelled = true;
    phase = 'idle';
    stopElapsedTimer();
    if (pool) { var p = pool; pool = null; p.shutdown(); }
    if (idleHandle) { cic(idleHandle); idleHandle = null; }
    els.state.textContent = '致命错误';
    els.result.textContent = '❌ ' + msg;
    log('致命错误: ' + msg);
    releaseData();
    updateButtons();
  }

  function releaseData() {
    master = null;
    gen = null;
    mainDrivers = [];
    merger = null;
    verifier = null;
    sortedChunks = [];
  }

  function resetRun() {
    cancelled = false;
    paused = false;
    chunks = [];
    sortedChunks = [];
    mainDrivers = [];
    merger = null;
    verifier = null;
    genProgress = 0;
    els.result.textContent = '-';
    els.stopLatency.textContent = '-';
    els.mode.textContent = '-';
  }

  // ===================================================================
  // 计时与按钮
  // ===================================================================
  function startElapsedTimer() {
    stopElapsedTimer();
    elapsedTimer = setInterval(function () {
      var ms = performance.now() - startTime - pauseAccum - (paused ? performance.now() - pauseStart : 0);
      els.elapsed.textContent = (ms / 1000).toFixed(1) + ' s';
    }, 100);
  }
  function stopElapsedTimer() {
    if (elapsedTimer) { clearInterval(elapsedTimer); elapsedTimer = null; }
  }

  function updateButtons() {
    var busy = ['generating', 'sorting', 'merging', 'verifying'].indexOf(phase) >= 0;
    els.btnGenerate.disabled = busy;
    els.btnStart.disabled = phase !== 'ready';
    els.btnPause.disabled = !busy || paused;
    els.btnCancel.disabled = !busy;
    var labels = {
      idle: '空闲', generating: '生成数据中', ready: '就绪',
      sorting: '分块排序中', merging: '归并中', verifying: '校验中', done: '完成'
    };
    els.state.textContent = labels[phase] + (paused ? '（已暂停）' : '');
  }

  // ===================================================================
  // 进度与 Canvas
  // ===================================================================
  function overallProgress() {
    if (phase === 'generating') return { total: genProgress, sort: 0, merge: 0, label: '生成' };
    if (phase === 'sorting' || phase === 'ready') {
      if (phase === 'ready') return { total: 0, label: '排序' };
      var sum = 0;
      for (var i = 0; i < chunks.length; i++) sum += chunks[i].progress;
      var sp = chunks.length ? sum / chunks.length : 0;
      return { total: sp * SORT_WEIGHT, sort: sp, merge: 0, label: '排序' };
    }
    if (phase === 'merging') {
      var mp = merger ? merger.progress() : 0;
      return { total: SORT_WEIGHT + mp * MERGE_WEIGHT, sort: 1, merge: mp, label: '归并' };
    }
    if (phase === 'verifying') {
      return { total: SORT_WEIGHT + MERGE_WEIGHT + VERIFY_WEIGHT, sort: 1, merge: 1, label: '校验' };
    }
    if (phase === 'done') return { total: 1, sort: 1, merge: 1, label: '完成' };
    return { total: 0, sort: 0, merge: 0, label: '-' };
  }

  var COLORS = {
    pending: '#3a4356', queued: '#4a5568', sorting: '#3b82f6',
    done: '#22c55e', cancelled: '#6b7280'
  };

  function resizeCanvas() {
    var dpr = window.devicePixelRatio || 1;
    var w = canvas.clientWidth, h = canvas.clientHeight;
    if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
      canvas.width = w * dpr;
      canvas.height = h * dpr;
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { w: w, h: h };
  }

  function draw() {
    var size = resizeCanvas();
    var W = size.w, H = size.h;
    ctx.clearRect(0, 0, W, H);

    var pad = 14;
    var labelW = 70;
    var plotW = W - pad * 2 - labelW;

    // 背景网格
    ctx.strokeStyle = 'rgba(255,255,255,0.06)';
    ctx.lineWidth = 1;
    for (var g = 0; g <= 10; g++) {
      var gx = pad + labelW + plotW * g / 10;
      ctx.beginPath(); ctx.moveTo(gx, pad); ctx.lineTo(gx, H - pad); ctx.stroke();
    }

    var prog = overallProgress();

    if (chunks.length > 0) {
      var topPad = 8;
      var overallH = 22;
      var gap = 10;
      var areaTop = pad + topPad + overallH + gap;
      var areaH = H - areaTop - pad;
      var barH = Math.max(10, Math.min(34, areaH / chunks.length - 6));

      // 整体进度条
      ctx.fillStyle = '#1f2937';
      ctx.fillRect(pad + labelW, pad + topPad, plotW, overallH);
      var grad = ctx.createLinearGradient(0, 0, plotW, 0);
      grad.addColorStop(0, '#60a5fa');
      grad.addColorStop(1, '#34d399');
      ctx.fillStyle = grad;
      ctx.fillRect(pad + labelW, pad + topPad, plotW * prog.total, overallH);
      ctx.fillStyle = '#e5e7eb';
      ctx.font = '12px ui-monospace, monospace';
      ctx.textBaseline = 'middle';
      ctx.fillText('整体 ' + (prog.total * 100).toFixed(1) + '%', pad + labelW + 8, pad + topPad + overallH / 2);

      // 分块柱
      for (var i = 0; i < chunks.length; i++) {
        var c = chunks[i];
        var y = areaTop + i * (barH + 6);
        ctx.fillStyle = '#9ca3af';
        ctx.textAlign = 'right';
        ctx.fillText('#' + i, pad + labelW - 8, y + barH / 2);
        ctx.textAlign = 'left';

        ctx.fillStyle = '#111827';
        ctx.fillRect(pad + labelW, y, plotW, barH);
        ctx.fillStyle = COLORS[c.status] || COLORS.pending;
        ctx.fillRect(pad + labelW, y, plotW * c.progress, barH);

        ctx.fillStyle = 'rgba(255,255,255,0.75)';
        var pct = (c.progress * 100).toFixed(0) + '%';
        var label = pct + '  ' + (c.status === 'sorting' ? '排序中' :
          c.status === 'done' ? '完成' : c.status === 'queued' ? '排队' :
          c.status === 'cancelled' ? '已取消' : '等待');
        ctx.fillText(label, pad + labelW + 8, y + barH / 2);
      }
    } else {
      ctx.fillStyle = '#6b7280';
      ctx.font = '13px sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText('生成数据后显示分块进度', W / 2, H / 2);
      ctx.textAlign = 'left';
    }

    // 文字进度
    els.overall.textContent = (prog.total * 100).toFixed(2) + '%';
    els.sortPhase.textContent = phase === 'merging' || phase === 'verifying' || phase === 'done'
      ? '100%'
      : (chunks.length ? ((prog.sort || 0) * 100).toFixed(1) + '%' : '-');
    els.mergePhase.textContent = phase === 'verifying' || phase === 'done'
      ? '100%'
      : (phase === 'merging' ? ((prog.merge || 0) * 100).toFixed(1) + '%' : '-');
  }

  // 常驻 rAF：界面始终以 ~60fps 重绘；帧间隔超过 50ms 计入卡顿
  function frameLoop(ts) {
    var dt = ts - lastFrame;
    lastFrame = ts;
    if (dt > 50) frameBudget += dt;
    els.frameMs.textContent = dt.toFixed(1) + ' ms' + (dt > 50 ? ' ⚠' : '');
    draw();
    requestAnimationFrame(frameLoop);
  }

  // ===================================================================
  // 事件绑定与启动
  // ===================================================================
  els.btnGenerate.addEventListener('click', startGenerate);
  els.btnStart.addEventListener('click', startSort);
  els.btnPause.addEventListener('click', pauseAll);
  els.btnCancel.addEventListener('click', cancelAll);
  els.btnTickle.addEventListener('click', function () {
    tickles++;
    els.tickleCount.textContent = tickles + ' 次（点击始终即时响应，证明主线程未卡死）';
  });
  window.addEventListener('resize', draw);

  updateButtons();
  requestAnimationFrame(frameLoop);
  log('页面就绪。生成 100 万数据 → 开始排序，可随时暂停/取消。');
})();
