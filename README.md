# 百万随机整数 · Web Worker 并行排序

100 万个随机整数，使用 4–8 个 Web Worker 分块并行排序 + 主线程 k 路归并，
全程可暂停 / 继续 / 取消，Canvas 柱状图实时展示分块与整体进度，
并覆盖 Worker 创建失败、消息丢失/卡死、内存不足等异常降级链路。

## 运行

必须通过 HTTP 提供（Worker 受同源限制，不能用 `file://`）：

```bash
cd /home/wangbo/gsbProject/gsb3/A
python3 -m http.server 8000
# 浏览器打开 http://localhost:8000
```

## 操作流程

1. 选择数据量（默认 100 万）与 Worker 数（默认按 `hardwareConcurrency` 取 4–8）。
2. 点击 **生成数据**（主线程 `requestIdleCallback` 分片生成，不卡顿）。
3. 点击 **开始排序**；排序中可随时 **暂停 / 继续 / 取消**。
4. 分块全部完成后自动进入主线程 k 路归并，最后做升序 + 校验和双重校验。
5. “点我测试主线程响应”按钮和页面滚动可在全程验证主线程未卡死；
   状态栏实时显示帧间隔。

### 故障注入（验收用）

- **前 2 次创建 Worker 失败**：池自动重试，日志可见降级为更少 Worker。
- **Worker 创建全部失败**：整体降级到主线程 `requestIdleCallback` 分片执行，结果仍正确。
- **#0 块 Worker 内存不足**：Worker 报错归还 buffer，分块带新 attempt 重排。
- **#0 块 Worker 卡死**：心跳看门狗（1.5s）强杀重排；点取消时 100ms 强杀保底。

## 架构

```
主线程 app.js
  ├─ 生成 / 降级排序 / 归并 / 校验：全部时间片化（每片 ≤9ms），requestIdleCallback 调度
  ├─ WorkerPool(pool.js)：建池重试、派发、看门狗、暂停广播、取消强杀
  ├─ KWayMerger(kway.js)：二叉堆 k 路归并（主线程时间片，可暂停/取消）
  └─ Canvas：常驻 requestAnimationFrame，分块柱 + 整体条
        │  ArrayBuffer Transferable（零拷贝）
        ▼
sort-worker.js
  └─ MergeSortEngine(mergesort.js)：自底向上归并，插入排序预处理 16 长度 run
        step(8ms) 时间片推进，片间检查 pause / cancel
```

### 关键设计

- **可中断算法**：自底向上归并排序，状态机为「16 长度 run 插入排序 → 逐趟归并」，
  双缓冲交替，`step(timeBudget)` 在任意片界返回，暂停/取消无锁、无忙等。
- **进度精确**：进度按真实元素写入数计算（初始一趟 + 每趟归并），
  整体进度 = 排序 80% + 归并 18% + 校验 2%（按工作量加权），误差远小于 5%。
- **取消保证**：先广播协作式 `cancel`（~5ms 轮询确认），
  100ms 后仍存活的 Worker 一律 `terminate()`，实测停止耗时 ≤ ~101ms < 200ms。
- **消息丢失/乱序**：每次派发带自增 `attempt`，过期回复直接丢弃；
  看门狗 750ms 巡检、1.5s 心跳超时即 terminate 并用主数据重新切片重排。
- **内存不足**：分块拷贝、归并输出等所有 `new ArrayBuffer` 均被 try/catch，
  失败后走主线程降级或致命错误提示；取消/完成后及时释放缓冲。
- **主线程零长任务**：数据生成、校验和、排序降级、归并、校验均 ≤9ms 切片，
  100 万整数单块纯排序约 120ms 总量，分摊后页面保持可点击、可滚动。

## 验收对照

| 验收项 | 实现 |
| --- | --- |
| 100 万条时页面可点击、滚动 | 所有重活时间片化（≤9ms）+ Worker 并行；rAF 帧间隔实时显示 |
| 取消后 200ms 内 Worker 停止 | 协作取消 + 100ms `terminate()` 保底，实测 5–101ms |
| 进度误差 < 5% | 进度按真实写入量计算，非时间估算 |
| Worker 创建失败自动降级且结果正确 | 建池重试 → 少 Worker/单 Worker → 全失败则主线程分片；归并后升序+校验和校验 |

## 自动化测试（Node，无需浏览器）

`/tmp/test-engine.js`：排序引擎边界尺寸、重复/逆序数据、暂停继续、100 万、k 路归并。
`/tmp/test-e2e.js`：基于 `worker_threads` 跑真实 Worker 代码，覆盖
建池失败重试、全失败降级、OOM 重排、暂停无进展、取消耗时（普通/卡死）断言。
