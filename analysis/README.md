# Analysis
This folder includes code and scripts for analysis.

## FCT analysis

**标准流程（推荐）**：见 [FCT_STANDARD_WORKFLOW.md](./FCT_STANDARD_WORKFLOW.md)，使用：

```bash
python3 fct_standard_report.py single -n <拓扑名> <path/to/*_fct.txt>
python3 fct_standard_report.py compare --detail --case A:pathA --case B:pathB ...
```

输出：全局统计表、实际 FCT 范围、按 `m_size` 分组（含平均 FCT / 慢化 / 备注），便于多拓扑对比。

`fct_analysis.py` / `fct_analysis.cpp`：按流大小百分位分桶的 P50/P95/P99 **慢化**，用于复现 [HPCC paper](https://liyuliang001.github.io/publications/hpcc.pdf) Figure 11 类图。用法：`python fct_analysis.py -h`（或 `../fct_analysis_with_paths.py -f <fct.txt>`）。

`analyze_single_fct.py`：单文件调试，逻辑与 `fct_analysis.py` 一致。

## Trace reader
`trace_reader` is used to parse the .tr files output by the simulation.

### Usage: 
1. `make trace_reader`

2. `./trace_reader <.tr file> [filter_expr]`. The filter_expr is used to filter events. For example, `time > 2000010000` will display only events after 2000010000, `sip=0x0b000101&dip=0x0b000201` will display only events with sip=0x0b000101 and dip=0x0b000201. Feel free to play with it (we may come up with more detailed descriptions in the future. For now, please read trace_filter.hpp for more details).

### Output:
Each line is like:

`2000055540 n:338 4:3 100608 Enqu ecn:0 0b00d101 0b012301 10000 100 U 161000 0 3 1048(1000)`

It means: at time 2000055540ns, at node 338, port 4, queue #3, the queue length is 100608B, and a packet is enqueued; the packet does not have ECN marked, is from 11.0.209.1:10000 to 11.1.35.1:100, is a data packet (U), sequence number 161000, tx timestamp 0, priority group 3, packet size 1048B, payload 1000B.

There are other types of packets. Please refer to print_trace() in utils.hpp for details.
