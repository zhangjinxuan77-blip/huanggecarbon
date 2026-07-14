# 黄阁碳排自动计算脚本

这个目录用于把原来 Colab 里的计算过程放到水厂服务器自动运行。

## 文件说明

- `source/process_carbon_colab.py`：原“黄阁水厂工艺段碳排.py”计算脚本，保留计算公式。
- `source/network_carbon_colab.py`：原“管网监测点管段信息匹配及碳排计算.py”计算脚本，保留计算公式。
- `run_process_calc.py`：工艺段计算包装脚本，负责替换 Colab 路径、运行计算、发布 `real-time output` 和 `history`。
- `run_network_calc.py`：管网计算包装脚本，负责替换 Colab 路径、运行计算、发布管网 Excel。
- `run_all_calc.sh`：一键运行工艺段计算和管网计算。

## 服务器推荐目录

```text
/opt/huanggecarbon/              # FastAPI 项目目录
/srv/huanggecarbon/input/process # 工艺段原始 CSV
/srv/huanggecarbon/input/network # 管网原始文件
/srv/huanggecarbon/work          # 工艺段计算中间结果
```

## 手动运行示例

```bash
cd /opt/huanggecarbon

PROCESS_INPUT_FILE=/srv/huanggecarbon/input/process/20260511.csv \
NETWORK_WORK_DIR=/srv/huanggecarbon/input/network \
./scripts/run_all_calc.sh
```

如果只想先测试计算结果，不发布到 `data`，可以单独运行：

```bash
.venv/bin/python scripts/run_process_calc.py \
  --input /srv/huanggecarbon/input/process/20260511.csv \
  --work-dir /srv/huanggecarbon/work/process_latest

.venv/bin/python scripts/run_network_calc.py \
  --work-dir /srv/huanggecarbon/input/network
```

## 定时任务示例

确认手动运行成功后，再加入 cron：

```cron
30 1 * * * cd /opt/huanggecarbon && ./scripts/run_all_calc.sh >> /var/log/huanggecarbon-calc.log 2>&1
```
