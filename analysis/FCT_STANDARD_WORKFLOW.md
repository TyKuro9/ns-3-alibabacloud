# FCT 标准分析流程

本文档定义 SimAI ns-3 仿真 **Flow Completion Time (FCT)** 的标准分析步骤。后续对比不同拓扑时，按同一流程执行即可得到与实验报告一致的结构（全局表 + FCT 范围 + 按 `m_size` 分组）。

## 1. 输入：FCT 日志格式

仿真输出的 `*_fct.txt` 每行 **8 列**（与 `fct_analysis.cpp` 中 `fscanf` 一致）：

| 列号 | 字段 | 说明 |
|------|------|------|
| 1–2 | sip, dip | 十六进制节点地址 |
| 3 | sport | 源端口 |
| 4 | **dport** | `100` = normal 流；`200` = incast 流 |
| 5 | **m_size** | 消息字节数；训练分析时仅保留下表三类，**排除 64/128 系统小包** |

**训练消息 m_size（GPT-22B / TP8-PP8 等）**：

| m_size (B) | 约等于 | 通信含义 |
|------------|--------|----------|
| 393216 | 393 KB | **TP** All-Reduce（占训练流 ~99%+） |
| 15925248 | 15.9 MB | **PP/DP** 等中消息 |
| 63700992 | 63.7 MB | 大消息（激活/权重传输等） |

64、128 等为仿真/协议层小包，**不参与**标准训练 FCT 对比（可用 `--all-sizes` 查看）。
| 6 | start_time | 流开始时间 (ns) |
| 7 | **fct** | 实际完成时间 (ns) |
| 8 | **standalone_fct** | 无竞争理想完成时间 (ns) |

示例：

```
0b00b401 0b00b501 10000 100 393216 802024 7041 1018
```

## 2. 过滤规则（与 `fct_analysis.py` / `.cpp` 对齐）

| 参数 | 默认 | 含义 |
|------|------|------|
| `-t 0` | ✓ | 只统计 `dport == 100`（normal） |
| `-t 1` | | 只统计 `dport == 200`（incast） |
| `-t 2` | | 全部流 |
| `-T 10000000000` | ✓ | 丢弃 `start_time + fct >= T` 的流（**默认 10s**；3s 会漏掉 15.9MB/63.7MB 训练消息） |

**慢化比（slowdown）**：

\[
\text{slowdown} = \max\left(\frac{\text{fct}}{\text{standalone\_fct}},\, 1.0\right)
\]

## 3. 标准工具

推荐使用：

```bash
cd SimAI/ns-3-alibabacloud/analysis
python3 fct_standard_report.py ...
```

| 工具 | 用途 |
|------|------|
| `fct_standard_report.py` | **标准报告**：全局统计、FCT 范围、按 `m_size`、多拓扑对比 |
| `fct_analysis.py` / `fct_analysis.cpp` | 按**流大小百分位**分桶的 P50/P95/P99 **慢化**（论文 Figure 11 风格） |
| `analyze_single_fct.py` | 单文件调试：慢化分桶 + 按 size 的慢化分位 |

大文件（数千万行）请用 Python 脚本流式读取；避免对整文件 `cat | awk`。

## 4. 单拓扑分析流程

### 4.1 确认 FCT 路径

典型位置：`SimAI/simulation_output/<拓扑名>/<拓扑名>_fct.txt`

### 4.2 运行标准报告

```bash
python3 fct_standard_report.py single \
  -n ROFT256 \
  -t 0 -T 3000000000 \
  /path/to/ROFT256_fct.txt
```

可选：加上 `--buckets` 输出与 `fct_analysis.py` 一致的分位分桶表。

### 4.3 报告应包含的三块

1. **全局统计**：流数、平均实际 FCT、平均 standalone、平均慢化  
2. **实际 FCT 范围**：`[min, max]`（用于观察尾延迟）  
3. **按训练 m_size 分组**（393216 / 15925248 / 63700992）：条数、占训练流占比、平均 FCT、平均慢化、通信类型备注  

## 5. 多拓扑对比流程

```bash
# 全局参数 -t/-T 须写在子命令 compare/single 之前
python3 fct_standard_report.py -t 0 -T 3000000000 compare --detail \
  -t 0 -T 3000000000 \
  --case HPN256:../../simulation_output/HPN256/HPN256_fct.txt \
  --case meta256:../../simulation_output/meta256/meta256_fct.txt \
  --case Zcube256:../../simulation_output/Zcube256/Zcube256_fct.txt \
  --case DeepSeek256:../../simulation_output/DeepSeek256/DeepSeek256_fct.txt \
  --case ROFT256:../../simulation_output/ROFT256/ROFT256_fct.txt
```

- 不加 `--detail`：仅全局对比表 + FCT 范围（与截图上半部分一致）  
- 加 `--detail`：每个拓扑再输出按 `m_size` 的分组表（与截图下半部分一致）  

将终端 Markdown 表复制到实验记录或 Confluence 即可。

## 6. 给 Agent 的使用约定

当用户提供 **一个或多个 `*_fct.txt`** 并要求 FCT 对比时：

1. 使用 `fct_standard_report.py compare`（多文件）或 `single`（单文件）  
2. 默认 `-t 0 -T 10000000000`（仅训练 m_size，排除 64/128），除非用户指定 `--all-sizes` 或更短窗口  
3. 汇报时按顺序给出：**全局表 → FCT 范围 → 各拓扑 m_size 表**  
4. 需要论文式慢化 CDF 分桶时，额外运行：  
   `python3 fct_analysis.py -f <path>` 或 `single ... --buckets`  

## 7. 指标含义速查

| 指标 | 计算 |
|------|------|
| 平均实际 FCT | 所有保留流的 `fct` 算术平均 |
| 平均 standalone | 所有保留流的 `standalone_fct` 算术平均 |
| 平均慢化 | 各流 slowdown 的算术平均 |
| 条数占比 | 该 `m_size` 流数 / 总流数 × 100% |

**解读提示**（与常见实验结论一致）：

- 平均慢化 ≈ 1：网络接近无拥塞（如 meta / Zcube 类结果）  
- 平均慢化 ≫ 1 且小 `m_size` 占绝大多数：AllReduce 等小消息被严重拖慢（如 HPN 类结果）  
- 极大 `max_fct`：存在长尾，需结合 `qlen` / `pfc` 日志  

## 8. 参考实现

- 过滤与慢化：`fct_analysis.py` L33–44，`fct_analysis.cpp` L90–94  
- 分位分桶：`fct_analysis.py` L49–58  
- 标准表格式：`fct_standard_report.py`  
