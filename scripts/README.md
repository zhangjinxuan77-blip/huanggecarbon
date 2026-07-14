# 黄阁碳排自动计算脚本

这个目录用于把原来 Colab 里的计算过程放到水厂服务器自动运行。

## 文件说明

- `source/process_carbon_colab.py`：原“黄阁水厂工艺段碳排.py”计算脚本，保留计算公式。
- `source/network_carbon_colab.py`：原“管网监测点管段信息匹配及碳排计算.py”计算脚本，保留计算公式。
- `run_process_calc.py`：工艺段计算包装脚本，负责替换 Colab 路径、运行计算、发布 `real-time output` 和 `history`。
- `run_network_calc.py`：管网计算包装脚本，负责替换 Colab 路径、运行计算、发布管网 Excel。
- `run_all_calc.sh`：一键运行工艺段计算和管网计算。
- `sync_existing_outputs.py`：已有计算结果同步脚本，不重新计算，只把 `中台一年历史数据` 和 `管网计算` 里的结果发布到后端 `data`。

## 服务器推荐目录

```text
/opt/huanggecarbon/              # FastAPI 项目目录
/srv/huanggecarbon/input/process # 工艺段原始 CSV
/srv/huanggecarbon/input/network # 管网原始文件
/srv/huanggecarbon/work          # 工艺段计算中间结果
```

## 需要按部署环境确认的路径

脚本会优先自动识别当前这种本地目录：

```text
/Users/a/Desktop/黄阁/huanggecarbon 2026
/Users/a/Desktop/黄阁/中台一年历史数据
/Users/a/Desktop/黄阁/管网计算
```

如果部署到水厂服务器，建议使用下面这些环境变量，不需要改计算源码：

```bash
export PROCESS_INPUT_FILE=/srv/huanggecarbon/input/process/20260511.tar.gz
export PROCESS_WORK_DIR=/srv/huanggecarbon/work/process_latest
export NETWORK_WORK_DIR=/srv/huanggecarbon/input/network
export CARBON_API_DATA_DIR=/opt/huanggecarbon/data
```

其中：

- `PROCESS_INPUT_FILE`：工艺段原始 CSV，或仅包含一个 CSV 的 `.tar.gz`/`.tar` 压缩包。脚本可直接流式读取压缩包，不要求先解压出约 10.8GB 的 CSV。
- `PROCESS_WORK_DIR`：工艺段计算输出目录，脚本会从这里发布 `real-time output` 和 `report_history`。
- `NETWORK_WORK_DIR`：管网计算目录，目录下需要包含 `管网监测点信息匹配` 和 `管网压力流量区域匹配`。
- `CARBON_API_DATA_DIR`：后端接口实际读取的 `data` 目录。

## 手动运行示例

如果两个解压后的文件夹已经包含计算结果，只需要刷新后端 `data`，优先用这个命令：

```bash
cd "/Users/a/Desktop/黄阁/huanggecarbon 2026"
.venv/bin/python scripts/sync_existing_outputs.py
```

它会自动按下面关系同步：

```text
../中台一年历史数据/real-time output -> data/real-time output
../中台一年历史数据/report_history   -> data/history
../管网计算/管网压力流量区域匹配/管网碳排_按压力监测点_坐标匹配.xlsx
                                  -> data/管网碳排_按压力监测点_坐标匹配.xlsx
```

如果要重新执行完整计算，再运行：

```bash
cd /opt/huanggecarbon

PROCESS_INPUT_FILE=/srv/huanggecarbon/input/process/20260511.tar.gz \
NETWORK_WORK_DIR=/srv/huanggecarbon/input/network \
./scripts/run_all_calc.sh
```

如果只想先测试计算结果，不发布到 `data`，可以单独运行：

```bash
.venv/bin/python scripts/run_process_calc.py \
  --input /srv/huanggecarbon/input/process/20260511.tar.gz \
  --work-dir /srv/huanggecarbon/work/process_latest

.venv/bin/python scripts/run_network_calc.py \
  --work-dir /srv/huanggecarbon/input/network
```

如果原始数据扫描已经生成 `碳排放核算/*.parquet`，但后续计算中断，可复用中间文件继续，避免再次扫描大型压缩包：

```bash
.venv/bin/python scripts/run_process_calc.py \
  --input /srv/huanggecarbon/input/process/20260511.tar.gz \
  --work-dir /srv/huanggecarbon/work/process_latest \
  --reuse-extracted
```

## 定时任务示例

确认手动运行成功后，再加入 cron：

```cron
30 1 * * * cd /opt/huanggecarbon && ./scripts/run_all_calc.sh >> /var/log/huanggecarbon-calc.log 2>&1
```
