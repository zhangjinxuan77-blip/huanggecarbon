# 水厂诊断策略生成器

该目录保存诊断决策、工艺优化和关联预警的完整生成逻辑。程序直接读取仓库
`data/real-time output/` 中其他计算模块发布的公共结果，不再重复读取原始 SCADA
文件、旧台账 Excel 或默认业务数据。

## 运行

在仓库根目录执行：

```powershell
python -m pip install -r scripts/strategy_engine/requirements.txt
python scripts/strategy_engine/generate_scada_report.py
```

不指定日期时，策略使用最新完整日报；实时接口使用公共结果最新行。也可以指定
一个已形成完整日报的日期：

```powershell
python scripts/strategy_engine/generate_scada_report.py 2026-05-10
```

生成文件默认写入 `scripts/strategy_engine/output/`。其中固定文件名 JSON 会先读取
仓库现有接口 JSON，再只覆盖本策略生成器负责的接口键，保留其他工程师的接口。

## 周期规则

- 实时看板：公共日报最新行，允许部分日，并输出 `periodStatus`。
- 日报策略：最新完整 UTC 日桶。
- 分时分析：公共滚动 24 小时结果。
- PAC、NaClO 和电耗基准：报告日前三个完整月的公共月度结果。
- 管网策略：继续由独立管网模块和管网 Excel 维护，不与厂区日报强行对齐日期。
