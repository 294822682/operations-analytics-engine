# N12.0 开源 UI Benchmark 报告

- 任务节点：`N12.0-OPEN-SOURCE-UI-BENCHMARK`
- 当前页面：`/dashboard/daily/trends/prototype?end_date=2026-05-22#account-trends`
- 任务边界：只读研究和设计建议；不改代码、不新增依赖、不复制外部源码、不复制外部素材、不改变 KPI 口径。
- 本项目当前入口：`GET /dashboard/daily/trends` 和 `GET /dashboard/daily/trends/prototype`。
- 当前页面生成位置：`src/oae/exports/feishu_dashboard_interactive_html.py` 的 `render_trend_dashboard_html`、`_trend_js`、`_CSS`。

## 1. 当前页面 UI 问题清单

1. 顶部筛选区已有开始日期、结束日期、快捷范围和范围提示，但快捷项数量偏多，缺少当前激活范围状态；“本月 / 上月 / 近 7 天 / 近 15 天 / 近 30 天 / 近三个月 / 本季度 / 上季度”在一个平面里并列，业务用户需要额外判断哪一种适合当前复盘。
2. 首屏信息已经从技术原型转向经营复盘，但日期范围之外的页面状态较少：可用日期、缺失日期、质量提示、人工复核边界如果不在首屏稳定露出，容易让用户把页面理解成正式发布面。
3. KPI 卡片以六列展示，适合桌面大屏快速扫数，但在 8 天到季度范围下，卡片之间的主次、目标参考、当前 / 目标、成本比值层级还不够统一。
4. 历史趋势图为自绘 SVG，能保留缺失点断线，但每个指标独立缩放，横向比较时不易判断“同一周期内哪类指标变化更明显”；tooltip 的内容结构可读，但缺少固定的“本期 / 上期 / 差值 / 变化率 / 数据状态”顺序规范。
5. 月度对比目前偏文字卡片，适合读取数值，不适合比较月与月之间的相对差距；CPL、CPS 等成本指标和规模指标应使用不同的视觉解释方式。
6. 账号表现模块以卡片流为主，适合看单个账号，但当账号数量增加时，搜索、排序、筛选和展开详情不够显性；当前存在账号展示过滤逻辑，后续需要把业务排除原因文档化，避免被误解为为了规避高比率而隐藏账号。
7. 主播表现模块与账号模块结构相近，但缺少按线索、到店、成交、CPL、CPS、EX7 明细快速排序的统一控制；卡片内细项多时，用户需要反复滚动。
8. 种草曝光模块展示账号总曝光和主播曝光，但曝光完成率、目标参考、缺失值和未接入字段的显示规则需要与 KPI 卡片完全一致，避免不同模块出现同一状态不同文案。
9. 搜索 / 排序 / 筛选 / 展开目前没有形成跨模块统一模式；趋势页内有账号线索 / 成交切换，但缺少全局搜索和面向账号 / 主播列表的轻量筛选。
10. 颜色体系已有 teal、green、amber、red、blue、ink，但用途没有完全 token 化；“实际值、目标参考、上一周期、缺失点、未接入、人工复核”应各自有固定语义。
11. 动画用于趋势线绘制，但未形成“只帮助定位变化、不改变数据读取”的规范；后续需要考虑 `prefers-reduced-motion`。
12. 页面服务对象是业务复盘，不是技术验收；因此 API、source、prototype 相关信息应继续弱化为边界提示，而不是作为主视觉内容。

## 2. 参考项目清单

1. shadcn/ui：组件组合、Card、Tabs、Tooltip、Command、Sheet、Dashboard Blocks、Chart tooltip、设计 token。
2. Apache ECharts：折线图、柱状图、坐标轴、legend、tooltip、dataZoom、null / missing data 表达、多序列比较。
3. Metabase：Dashboard filters、click behavior、drill-through、cross-filtering、业务卡片组织。
4. Grafana：dashboard time range、panel、legend、tooltip、time series、panel-level time settings、URL range 参数。
5. Ant Design / ProComponents：企业筛选区、Table / ProTable、排序、筛选、搜索、展开详情、列状态、密度控制、数据展示原则。

参考来源：

- [shadcn/ui Dashboard Blocks](https://ui.shadcn.com/blocks?category=dashboard)
- [shadcn/ui Card](https://ui.shadcn.com/docs/components/base/card)
- [shadcn/ui Tabs](https://ui.shadcn.com/docs/components/tabs)
- [shadcn/ui Tooltip](https://ui.shadcn.com/docs/components/base/tooltip)
- [shadcn/ui Chart](https://ui.shadcn.com/docs/components/chart)
- [Apache ECharts Legend handbook](https://apache.github.io/echarts-handbook/en/concepts/legend/)
- [Apache ECharts Axis handbook](https://apache.github.io/echarts-handbook/en/concepts/axis/)
- [Apache ECharts dataZoom tutorial](https://apache.googlesource.com/echarts-doc/+/refs/heads/v4/en/tutorial/data-zoom.md)
- [Apache ECharts option manual](https://echarts.apache.org/en/option.html#series-line.connectNulls)
- [Metabase Dashboard filters](https://www.metabase.com/docs/latest/dashboards/filters)
- [Metabase Dashboard interactivity](https://www.metabase.com/docs/latest/dashboards/interactive)
- [Grafana dashboard controls](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/create-dashboard/dashboard-controls/)
- [Grafana time series visualization](https://grafana.com/docs/grafana/latest/visualizations/panels-visualizations/visualizations/time-series/)
- [Grafana use dashboards](https://grafana.com/docs/grafana/latest/dashboards/use-dashboards/)
- [Ant Design Data Display](https://ant.design/docs/spec/data-display-cn/)
- [Ant Design Visualization Page](https://ant.design/docs/spec/visualization-page/)
- [Ant Design Table](https://ant.design/components/table/?locale=en)
- [ProComponents ProTable](https://procomponents.ant.design/en-US/components/table/)

## 3. 每个项目可借鉴点

shadcn/ui：

- 借鉴 Card 的 header / content / footer / action 分层，让 KPI 卡片固定为“指标名、主值、目标参考、当前 / 目标、状态说明、趋势入口”的稳定结构。
- 借鉴 Tabs 的分层内容模式，把账号 / 主播列表中的“总览、趋势、明细”做成同卡片内切换，而不是继续拉长卡片。
- 借鉴 Tooltip 的触发原则：hover 和 keyboard focus 都要能看到解释；tooltip 内容只解释字段、口径、数据状态，不加入主观判断。
- 借鉴 Dashboard Blocks 的“侧栏 / 顶部栏 / 区块卡 / 图表 / 数据表”组合思路，但保留本项目当前单页本地 HTML 结构。
- 借鉴 Chart tooltip 的信息分层：label、series name、indicator、value 分开，便于多序列对齐读取。

Apache ECharts：

- 借鉴 legend 的交互原则：多序列图允许显示 / 隐藏系列，单序列图用标题代替冗余 legend。
- 借鉴 axis 规范：长日期范围下减少 tick，必要时旋转或分层显示日期，避免横轴拥挤。
- 借鉴 tooltip 的 axis trigger 思路：同一日期下显示多个系列，按“本期、上一周期、差值、变化率、状态”排序。
- 借鉴 dataZoom 的“先总览，后查看细节”模式；本项目可先用日期快捷项和局部图表缩放控件表达，不需要引入图表库。
- 借鉴 null data 处理思路：缺失点保持断线或明确标注为缺失，不补 0，不把真实 0 当缺失。

Metabase：

- 借鉴 dashboard-level filters：顶部日期筛选应明确影响哪些模块，列表内筛选只影响当前模块。
- 借鉴 click behavior：点击账号 / 主播 / 图表点位后，可以打开详情层或筛选当前页面，而不是跳到技术 API。
- 借鉴 cross-filtering：点击某账号后，账号卡、趋势图、主播 / 种草关联模块可以进入同一筛选上下文，但必须显示“当前筛选：账号名”。
- 借鉴 dashboard narrative：用模块标题和短说明解释“这块数据回答什么经营问题”，不写技术验收词。

Grafana：

- 借鉴统一时间范围控制：顶部日期区是全局控制，图表标题区显示当前范围和是否对比上一周期。
- 借鉴 panel header：每个图表面板应有标题、当前值、范围、legend / tooltip 状态，而不是只给图。
- 借鉴 time series tooltip / legend：legend 可以提供当前值、最小值、最大值、合计等可选展示，但本项目 N12.1 应优先只做当前值和上一周期对比，避免新增指标。
- 借鉴 URL 参数保留范围：`start_date`、`end_date` 应继续反映当前查看状态，便于业务复盘转发同一视图。

Ant Design / ProComponents：

- 借鉴“摘要在前、筛选在中、详情在后”的可视化页结构。
- 借鉴 Table 适合结构化对比：账号、主播这类多行、多指标数据应有表格或紧凑列表模式，卡片用于重点对象。
- 借鉴 ProTable 的 toolbar：搜索、筛选、排序、密度、列显示可以作为同一工具条模式，但本项目只实现必要交互，不引入 ProTable。
- 借鉴 QueryFilter / LightFilter 的折叠筛选：高级筛选默认收起，常用日期范围常驻。

## 4. 不可直接复制的原因

1. 本项目是本地只读经营趋势页，当前技术栈是 FastAPI 返回内联 HTML / CSS / JS；shadcn/ui、Ant Design、ProComponents 是 React 生态模式，不能照搬组件实现。
2. Apache ECharts 是完整图表库，本轮不允许新增依赖；只能借鉴 tooltip、legend、axis、dataZoom、missing data 的设计行为。
3. Metabase 和 Grafana 的交互建立在其查询模型、权限、仪表板编辑器和数据源体系上，本项目不能复制它们的后端能力或权限模型。
4. 外部项目的默认色彩、间距、阴影、动画、素材和代码都不能作为本项目实现来源；本项目应从现有 CSS token 和业务语义继续演进。
5. 外部 BI 产品常带有 drill-down、query、auto-refresh、alert 等能力，本项目当前页面只读读取现有 dashboard source，不能扩展为数据处理或新指标计算。
6. 外部示例中的“隐藏空数据、压缩异常值、平滑缺失点”等做法若与本项目业务原则冲突，一律不能采用。

## 5. 可落地到当前项目的 UI 模式

1. 全局筛选栏：日期范围常驻，快捷范围分组，显示当前范围和 92 天上限。
2. 模块级工具条：账号、主播、种草模块各自提供搜索、排序、筛选、展开，不影响全局日期。
3. KPI 卡片统一结构：指标名、主值、单位、目标参考、当前 / 目标、趋势入口、数据状态。
4. 趋势面板统一结构：标题、当前值、范围、legend、SVG 图、tooltip、缺失说明。
5. 重点对象卡 + 明细表：账号和主播保留重点对象卡，长列表使用紧凑表格或列表。
6. 详情 Sheet 模式：点击账号 / 主播后展示当前对象的趋势、指标组、原始状态说明；实现上可以是本页内侧滑层或展开区，不需要新依赖。
7. 空值状态规范：真实 0 显示 `0`；缺失显示 `未提供`；字段未接入显示 `未接入`；超过 100% 如实显示。
8. URL 状态保留：日期范围和模块锚点继续可通过 URL 复现。

## 6. 顶部筛选区优化建议

1. 将快捷范围分成三组：常用范围（近 7 天、近 15 天、近 30 天）、自然周期（本月、上月、本季度、上季度）、扩展范围（近三个月）。
2. 当前激活范围按钮使用稳定样式，不只依赖 input 值；当用户手动输入日期后，显示为“自定义范围”。
3. 顶部固定显示：`当前范围：YYYY-MM-DD 至 YYYY-MM-DD`、`天数：N / 92`、`缺失日期：无 / N 天`。
4. 提交按钮文案建议为“应用范围”，避免只写“查看”。
5. 范围超限提示应保留，但建议放在筛选栏下方固定位置，避免因提示出现导致布局跳动。
6. 筛选栏只负责日期，不承载 source、API、prototype 文案；这些边界信息放页脚或信息 tooltip。

## 7. KPI 卡片优化建议

1. 固定卡片信息顺序：指标名、主值、单位、目标参考、当前 / 目标、最近趋势入口、数据状态。
2. 成本类指标 CPL / CPS 与规模类指标分开视觉语义：规模类用 `actual / target`，成本类用“当前成本 / 目标参考”，不要让“达成率”误导为越高越好。
3. 进度条可以继续截断到 100% 宽度，但旁边必须显示真实比例文本；超过 100% 不隐藏、不改文案。
4. 真实 0 直接显示 `0`；缺失才显示 `未提供`；未接入字段显示 `未接入`。
5. 卡片高度建议统一，主值区域预留长数值换行空间，避免大数、金额、百分比挤压说明文字。
6. 卡片颜色只表示指标类别或状态，不表示主观好坏。

## 8. 历史趋势图优化建议

1. 图表面板 header 固定展示：指标名、最新值、范围、上一周期是否存在。
2. 同一指标的本期和上一周期使用固定色：本期蓝色实线，上一周期灰色虚线；缺失点不连线。
3. 单序列图不显示冗余 legend；有上一周期时显示 legend。
4. Tooltip 顺序固定为：日期、本期值、上一周期值、差值、变化率、数据状态。
5. 缺失日期应在图下方以小标签或说明显示，不在趋势图里补点。
6. 92 天范围内横轴 tick 建议控制在 6 到 8 个；超过 31 天显示月-日和月初/月中/月末关键点。
7. 动画只用于首次渲染，且应支持减少动态效果；动画不得改变数值理解。

## 9. 月度对比优化建议

1. 月度对比应从“文字卡片”升级为“指标小表 + 轻量条形对比”的混合模式。
2. 每个指标保留月度数值，不用只给百分比变化。
3. 规模指标可用横向条形比较；成本指标 CPL / CPS 用单独色系和说明，避免与规模指标同一语义。
4. 月度对比只比较当前已有 dashboard source 数据，不补历史缺口，不扩大数据来源。
5. 如果某月只有部分日期，必须显示覆盖范围，例如 `2026-05-13 至 2026-05-22`。

## 10. 账号表现模块优化建议

1. 保留“线索组汇总”和重点账号卡，但普通账号改为紧凑列表或表格模式，减少卡片堆叠。
2. 增加账号搜索框，支持按账号名匹配。
3. 增加排序：线索数、成交数、CPL、CPS、当前 / 目标。
4. 增加筛选：有目标参考、未提供目标、字段未接入、超过 100% 比率。
5. 点击账号进入详情展开：线索趋势、成交趋势、目标参考、数据状态、相关说明。
6. 如果存在业务排除账号，必须显示在“未展示账号说明”中，说明排除原因；不能用隐藏账号规避高比率。

## 11. 主播表现模块优化建议

1. 主播列表优先使用表格 / 紧凑卡片，支持搜索主播名。
2. 排序项建议保留：线索、到店、成交、CPL、CPS、EX7 明细。
3. 主播卡片内只保留首要指标和趋势入口，次级指标进入展开详情。
4. 对未开播、未提供、未接入状态使用统一状态标签。
5. 主播详情中保留所有真实数据，包括超过 100% 的比率；不得用状态文案覆盖真实值。

## 12. 种草曝光模块优化建议

1. 账号总曝光与主播曝光分开两个子区：账号总览、主播明细。
2. 曝光指标使用人次单位和万级格式，但 tooltip 中保留完整数值。
3. 完成率超过 100% 时如实显示，例如 `123.45%`，进度条可满格但文字必须显示真实比例。
4. 缺失目标显示 `未提供`，未接入字段显示 `未接入`。
5. 主播曝光明细支持按曝光、人次目标、完成率排序。

## 13. Tooltip / hover / animation 规范

1. Tooltip 必须同时支持鼠标 hover 和键盘 focus。
2. Tooltip 内容只解释字段、单位、日期、来源状态、缺失状态和计算关系，不出现主观判断词。
3. Tooltip 不遮挡关键数据，靠近图表边缘时应自动向内定位。
4. 多序列 tooltip 固定顺序：本期、上一周期、差值、变化率、状态。
5. 卡片 tooltip 用于解释口径和字段状态；图表 tooltip 用于解释点位。
6. Hover 只能加强定位，例如高亮点位、显示辅助线；不能隐藏其他真实数据。
7. 动画最长不超过 800ms；支持减少动态效果；禁止用动画表达业务好坏。

## 14. 搜索 / 排序 / 筛选 / 展开交互规范

1. 搜索：账号、主播、种草主播模块各自有本模块搜索框；搜索只过滤当前模块可见项。
2. 排序：排序按钮或表头排序必须显示当前排序字段和方向。
3. 筛选：筛选条件显示为可清除标签，例如 `字段未接入`、`目标未提供`、`比率超过 100%`。
4. 展开：点击卡片或行进入本对象详情；展开状态不改变原始列表数据。
5. URL：全局日期范围进 URL；模块级搜索、排序、展开可先不进 URL，除非 N12.1 明确需要可分享状态。
6. 空结果：搜索 / 筛选后无匹配时显示“无匹配项”，不要显示“未提供”，避免混淆数据缺失。
7. 性能：当前数据规模下优先前端本地过滤和排序；不触发 daily_pipeline，不写输出目录。

## 15. 颜色 / 字体 / 间距 / 圆角 / 阴影设计 token 建议

颜色 token：

- `--color-bg`: 页面背景。
- `--color-surface`: 卡片和面板背景。
- `--color-border`: 边框和分隔线。
- `--color-text`: 主文本。
- `--color-text-muted`: 次级文本。
- `--color-series-current`: 本期趋势线。
- `--color-series-previous`: 上一周期趋势线。
- `--color-series-target`: 目标参考。
- `--color-status-missing`: 缺失值 / 未提供。
- `--color-status-not-connected`: 未接入。
- `--color-status-review`: 需人工复核。
- `--color-focus`: 键盘焦点和 hover 辅助线。

字体 token：

- `--font-base`: `"PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", sans-serif`。
- `--font-number`: `"DIN Alternate", "Avenir Next Condensed", "PingFang SC", sans-serif`。
- `--font-size-title`: 28-34px，只用于页面标题。
- `--font-size-section`: 20-24px，用于模块标题。
- `--font-size-card-title`: 13-15px，用于卡片指标名。
- `--font-size-value`: 26-32px，用于 KPI 主值。
- `--font-size-body`: 13-14px，用于说明。
- `--font-size-caption`: 11-12px，用于 tooltip 和状态。

间距 / 圆角 / 阴影 token：

- `--space-1`: 4px。
- `--space-2`: 8px。
- `--space-3`: 12px。
- `--space-4`: 16px。
- `--space-5`: 20px。
- `--space-6`: 24px。
- `--radius-control`: 6px。
- `--radius-card`: 8px。
- `--shadow-panel`: 0 8px 20px rgba(24, 32, 38, 0.08)。
- `--shadow-tooltip`: 0 12px 28px rgba(24, 32, 38, 0.16)。

使用规则：

- 圆角不超过 8px，除进度条和小状态标签外不使用胶囊式大圆角。
- 阴影只用于浮层、tooltip、重点面板；普通区块优先用边框和留白。
- 颜色不表达主观判断；只表达类别、状态和可交互焦点。

## 16. 后续 N12.1 实施拆分建议

N12.1 建议只做 UI 层落地，不改 KPI、不改 API 数据口径、不新增依赖。

建议拆分为 4 个小步：

1. N12.1-A：抽出本页设计 token，并统一顶部筛选区、KPI 卡片、tooltip 文案规则。
2. N12.1-B：优化历史趋势图和月度对比，包括 tooltip 顺序、legend 规则、缺失点说明、月度条形对比。
3. N12.1-C：账号 / 主播 / 种草模块增加搜索、排序、筛选、展开详情的统一模式。
4. N12.1-D：补充 focused tests 和浏览器验收，确认真实 0、未提供、未接入、超过 100% 比率、92 天上限、缺失点断线全部保留。

第一优先级建议从 N12.1-A 开始，因为它不触碰数据结构，能先稳定页面规范。

## 17. 风险清单

1. 过度借鉴外部 UI 库会诱导新增依赖；N12.1 必须先以现有内联 HTML / CSS / JS 落地。
2. 表格化账号 / 主播模块时，可能误把卡片里已有的说明信息删掉；实施时必须保留所有真实业务字段。
3. 进度条视觉截断到 100% 容易被误解为真实比例封顶；必须同时展示真实百分比文本。
4. 搜索 / 筛选可能导致账号或主播暂时不可见；必须显示当前筛选条件和清除入口，避免被误认为数据被删除。
5. 月度对比若不显示覆盖日期，容易把部分月份当完整月份；必须展示每个月的数据覆盖范围。
6. Tooltip 若只支持 hover，会影响键盘用户和截图验收；必须支持 focus。
7. 动画和 hover 高亮若过重，会干扰业务复盘；应保持克制并支持减少动态效果。
8. 当前仓库已有大量未清理的既有改动，N12.1 实施前应继续只改明确范围内文件，避免混入产物目录。

## 18. 验收标准

1. 页面继续只读读取 `/dashboard/daily/trends`，不运行 `python -m oae.jobs.daily_pipeline`。
2. 不新增依赖，不修改 dependency 文件。
3. 不写 `output/`、`outputs/`、`artifacts/`、`源文件/`、`历史文件/`。
4. 不改变 KPI 口径，不新增业务指标。
5. 真实 0 显示为 `0`。
6. 缺失值显示为 `未提供`。
7. 字段未接入显示为 `未接入`。
8. 超过 100% 的真实比率如实显示，不替换成其他文案。
9. 趋势图缺失点不补 0，不连成虚假连续趋势。
10. 日期范围最大 92 天。
11. 搜索 / 排序 / 筛选后必须显示当前条件和清除入口。
12. 账号 / 主播不可因高比率被删除或隐藏；如有业务排除名单，必须在页面说明中披露。
13. Tooltip 支持 hover 和 focus。
14. 页面不出现技术验收词作为主视觉内容。
15. Focused tests 覆盖趋势 HTML、日期范围、缺失值、未接入、超过 100% 比率、搜索 / 排序 / 筛选 / 展开交互。
16. 浏览器验收覆盖桌面和移动宽度，确认无文字重叠、图表非空、tooltip 不遮挡关键数值。
