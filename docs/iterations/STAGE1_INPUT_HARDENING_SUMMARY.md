# 1. 本轮目标

把主链输入从“靠人盯文件、猜路径、试运行”升级成“固定目录、固定命名、自动识别、提前报错、运行可追溯”。

# 2. 实际完成项

- 新增正式输入契约配置：`config/input_sources.json`
- 新增输入发现模块：`src/oae/ingest/source_registry.py`
- 新增输入字段校验模块：`src/oae/ingest/input_validator.py`
- 新增输入发现与 input manifest 生成模块：`src/oae/ingest/input_discovery.py`
- `daily_pipeline` 已在最前面接入输入发现与前置校验
- `daily_pipeline` 已把本次实际消费的输入写入 `input_manifest`、`run_manifest`、`quality_report`
- `build_fact` 默认行为已对齐固定源文件目录 `源文件/`
- 新增输入说明文档：`docs/INPUT_GUIDE.md`

# 3. 修改/新增文件清单

## 配置
- `config/input_sources.json`

## 输入模块
- `src/oae/ingest/__init__.py`
- `src/oae/ingest/source_registry.py`
- `src/oae/ingest/input_validator.py`
- `src/oae/ingest/input_discovery.py`

## 共享 IO
- `src/oae/rules/io_utils.py`

## 作业层
- `src/oae/jobs/daily_pipeline.py`
- `src/oae/cli/build_fact.py`

## 文档
- `docs/INPUT_GUIDE.md`
- `docs/iterations/STAGE1_INPUT_HARDENING_SUMMARY.md`

# 4. 输入发现与校验如何工作

主链开始时，系统先读取 `config/input_sources.json`，把输入分成两类：

1. 动态源文件
   - `直播进度表`
   - `线索明细`
   - `成交明细`

2. 固定配置文件
   - `monthly_targets.csv`
   - `daily_spend.csv`

然后执行以下步骤：

1. 扫描固定目录
2. 根据 glob 规则找到候选文件
3. 用正式命名规则过滤异常命名
4. 从文件名里提取业务日期 / 业务月份
5. 自动选择本次实际使用文件
6. 在真正跑链前检查关键字段
7. 生成 `input_manifest`

如果出现以下问题，会在主链开头直接中止：
- 缺文件
- 命名不合规
- 同业务日期多候选
- 缺关键字段

# 5. 新增产物

- `artifacts/runs/input_manifest_{run_id}.json`
- `run_manifest` 新增 `input_manifest` 和 `inputs`
- `quality_report` 新增 `inputs` 章节

# 6. 主链运行状态

本轮改造后，正式主链仍要求保持：
- fact
- snapshot
- ledger
- analysis
- export
- quality
- run manifest

最新验证运行：
- `run_id = run-20260317T121527Z`
- `run_manifest = artifacts/runs/run_manifest_run-20260317T121527Z.json`
- `quality_report = artifacts/runs/quality_report_run-20260317T121527Z.json`
- `input_manifest = artifacts/runs/input_manifest_run-20260317T121527Z.json`

验证结果：
- 输入发现成功，已锁定本次实际输入：
  - `源文件/2026年直播进度表3月.xlsx`
  - `源文件/总部新媒体线索2026-03-17.csv`
  - `源文件/总部新媒体成交2026-03-17.csv`
  - `config/monthly_targets.csv`
  - `config/daily_spend.csv`
- 主链成功跑通：fact -> snapshot -> ledger -> analysis -> export -> quality -> run manifest
- 日报线索质量口径已正确读取直播进度表 `全场景线索人数=4405`，不再出现“直播进度表为 0”的旧问题
- `quality_report` 当前为 `fail`，原因不是输入校验失败，而是当前经营结果与历史 baseline 存在真实业务漂移（数据日期已推进到 2026-03-16）

# 7. 仍未解决的输入风险

- 当前 `export_feishu_report`、`verify_report_tsv` 仍保留自己的默认 `live-file` 参数，只是主链已改为显式传入正式路径
- `monthly_targets.csv` 和 `daily_spend.csv` 仍由各自 loader 保留自动补模板逻辑；主链因前置校验已不会再默默依赖这层兜底，但独立调用 loader 时仍存在旧行为
- 目前只治理了正式主链，raw 单独分析链尚未接入同一套输入契约
- 当前 baseline 仍冻结在旧日期样本上，因此质量报告会把最新经营数据增长识别为 drift；这不是输入层错误，但会影响“整链是否 pass”的表面结果

# 8. 下一步建议

- 把 raw 单独分析链也接到同一套输入契约
- 把输入发现结果进一步转成更适合运营看的输入检查摘要
- 在阶段1下一任务中，把输入异常与经营异常串成“先查输入，再查结果”的排查路径
