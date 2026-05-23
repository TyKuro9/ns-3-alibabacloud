#!/usr/bin/env python3
"""
SimAI / ns-3 FCT 标准分析报告。

与 fct_analysis.py / fct_analysis.cpp 使用相同的过滤与慢化比定义：
  - 列格式（8 列）：sip dip sport dport m_size start_time fct standalone_fct
  - dport==100 为 normal（-t 0），dport==200 为 incast（-t 1），-t 2 为全部
  - 仅保留 start_time + fct < time_limit 的流
  - slowdown = max(fct / standalone_fct, 1.0)

输出结构对齐实验对比表：全局统计、FCT 范围、按训练消息 m_size 分组、可选多拓扑对比。

默认仅统计训练过程中的通信（排除 64/128 等系统小包）：
  - 393216 B (~393 KB)：TP All-Reduce
  - 15925248 B (~15.9 MB)：PP/DP 等中消息
  - 63700992 B (~63.7 MB)：大消息（激活/权重等）
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

# GPT-22B / TP8-PP8 等 workload 在 FCT 日志中的训练消息尺寸（字节）
TRAINING_M_SIZE_ORDER: Tuple[int, ...] = (393216, 15925248, 63700992)

TRAINING_M_SIZE_LABEL: Dict[int, str] = {
    393216: "TP All-Reduce (~393KB)",
    15925248: "中消息 (~15.9MB, PP/DP 等)",
    63700992: "大消息 (~63.7MB)",
}

SYSTEM_M_SIZES: frozenset = frozenset({64, 128})


# ---------------------------------------------------------------------------
# FCT 文件解析（与 fct_analysis.cpp fscanf 字段一致）
# ---------------------------------------------------------------------------

@dataclass
class FlowRecord:
    m_size: int
    fct: int
    standalone_fct: int

    @property
    def Slowdown(self) -> float:
        if self.standalone_fct <= 0:
            return 1.0
        s = self.fct / self.standalone_fct
        return s if s >= 1.0 else 1.0


def GetDportFilter(flow_type: int) -> Optional[int]:
    if flow_type == 0:
        return 100
    if flow_type == 1:
        return 200
    return None


def LoadFlowRecords(
    filepath: str,
    dport_filter: Optional[int],
    time_limit: int,
) -> List[FlowRecord]:
    records: List[FlowRecord] = []
    with open(filepath, "r") as f:
        for line in f:
            parts = line.split()
            if len(parts) != 8:
                continue
            dport = int(parts[3])
            if dport_filter is not None and dport != dport_filter:
                continue
            m_size = int(parts[4])
            start = int(parts[5])
            fct = int(parts[6])
            standalone = int(parts[7])
            if start + fct >= time_limit:
                continue
            records.append(FlowRecord(m_size=m_size, fct=fct, standalone_fct=standalone))
    return records


def GetPctl(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    i = int(len(sorted_vals) * p)
    if i >= len(sorted_vals):
        i = len(sorted_vals) - 1
    return sorted_vals[i]


# ---------------------------------------------------------------------------
# 统计与备注
# ---------------------------------------------------------------------------

@dataclass
class GlobalSummary:
    name: str
    flows: int = 0
    avg_fct: float = 0.0
    avg_standalone: float = 0.0
    avg_slowdown: float = 0.0
    min_fct: int = 0
    max_fct: int = 0
    slow_p50: float = 0.0
    slow_p95: float = 0.0
    slow_p99: float = 0.0

    def Compute(self, records: Iterable[FlowRecord]) -> None:
        recs = list(records)
        if not recs:
            return
        self.flows = len(recs)
        fcts = [r.fct for r in recs]
        standalones = [r.standalone_fct for r in recs]
        slowdowns = sorted(r.Slowdown for r in recs)
        self.avg_fct = sum(fcts) / self.flows
        self.avg_standalone = sum(standalones) / self.flows
        self.avg_slowdown = sum(slowdowns) / self.flows
        self.min_fct = min(fcts)
        self.max_fct = max(fcts)
        self.slow_p50 = GetPctl(slowdowns, 0.5)
        self.slow_p95 = GetPctl(slowdowns, 0.95)
        self.slow_p99 = GetPctl(slowdowns, 0.99)


@dataclass
class SizeBucketSummary:
    m_size: int
    comm_label: str
    count: int
    pct: float
    avg_fct: float
    avg_standalone: float
    avg_slowdown: float
    slow_p50: float
    slow_p95: float
    remark: str = ""


@dataclass
class FilterSummary:
    total_raw: int = 0
    training: int = 0
    system_excluded: int = 0
    other_excluded: int = 0


def IsTrainingMSize(m_size: int) -> bool:
    return m_size in TRAINING_M_SIZE_LABEL


def GetCommLabel(m_size: int) -> str:
    return TRAINING_M_SIZE_LABEL.get(m_size, f"其他 ({m_size} B)")


def PartitionFlows(records: List[FlowRecord], training_only: bool) -> Tuple[List[FlowRecord], FilterSummary]:
    stats = FilterSummary(total_raw=len(records))
    if not training_only:
        stats.training = len(records)
        return records, stats
    training: List[FlowRecord] = []
    for r in records:
        if r.m_size in SYSTEM_M_SIZES:
            stats.system_excluded += 1
        elif IsTrainingMSize(r.m_size):
            training.append(r)
        else:
            stats.other_excluded += 1
    stats.training = len(training)
    return training, stats


def BuildRemark(m_size: int, count: int, pct: float, avg_slow: float, total: int) -> str:
    if count == 0:
        return ""
    label = TRAINING_M_SIZE_LABEL.get(m_size, "")
    if m_size == 393216 and pct >= 50.0 and avg_slow >= 2.0:
        return "TP 主流 All-Reduce，拥塞相较 standalone 明显"
    if m_size == 15925248 and avg_slow >= 3.0:
        return "PP/DP 中消息，显著慢于理想"
    if m_size == 63700992 and count < max(5000, total * 0.001):
        if avg_slow < 1.2:
            return "大消息样本少，均值接近理想"
        return "大消息样本少，尾延迟需单独看 max FCT"
    if pct >= 50.0 and avg_slow >= 2.0:
        return "训练主流通信，拥塞明显"
    if pct < 1.0 and count < max(5000, total * 0.001) and avg_slow < 1.2:
        return "样本少，均值接近理想"
    if avg_slow < 1.15:
        return "接近理想，拥塞很轻"
    if avg_slow >= 3.0:
        return f"{label or '该尺寸'}慢化明显"
    return "中等慢化，可结合 qlen/pfc 进一步看"


def PerSizeSummaries(records: List[FlowRecord], training_only: bool = True) -> List[SizeBucketSummary]:
    by_size: Dict[int, List[FlowRecord]] = defaultdict(list)
    for r in records:
        if training_only and not IsTrainingMSize(r.m_size):
            continue
        by_size[r.m_size].append(r)
    total = len(records) if not training_only else sum(len(v) for v in by_size.values())
    rows: List[SizeBucketSummary] = []
    size_order = list(TRAINING_M_SIZE_ORDER) if training_only else sorted(by_size)
    for m_size in size_order:
        if m_size not in by_size:
            continue
        recs = by_size[m_size]
        n = len(recs)
        pct = 100.0 * n / total if total else 0.0
        fcts = [r.fct for r in recs]
        stands = [r.standalone_fct for r in recs]
        slowdowns = sorted(r.Slowdown for r in recs)
        avg_slow = sum(slowdowns) / n
        rows.append(
            SizeBucketSummary(
                m_size=m_size,
                comm_label=GetCommLabel(m_size),
                count=n,
                pct=pct,
                avg_fct=sum(fcts) / n,
                avg_standalone=sum(stands) / n,
                avg_slowdown=avg_slow,
                slow_p50=GetPctl(slowdowns, 0.5),
                slow_p95=GetPctl(slowdowns, 0.95),
                remark=BuildRemark(m_size, n, pct, avg_slow, total),
            )
        )
    if not training_only:
        for m_size in sorted(by_size):
            if m_size in TRAINING_M_SIZE_LABEL:
                continue
            recs = by_size[m_size]
            n = len(recs)
            slowdowns = sorted(r.Slowdown for r in recs)
            rows.append(
                SizeBucketSummary(
                    m_size=m_size,
                    comm_label=GetCommLabel(m_size),
                    count=n,
                    pct=100.0 * n / total if total else 0.0,
                    avg_fct=sum(r.fct for r in recs) / n,
                    avg_standalone=sum(r.standalone_fct for r in recs) / n,
                    avg_slowdown=sum(slowdowns) / n,
                    slow_p50=GetPctl(slowdowns, 0.5),
                    slow_p95=GetPctl(slowdowns, 0.95),
                    remark="系统/其它" if m_size in SYSTEM_M_SIZES else "未归类",
                )
            )
    return rows


def PrintFilterNote(training_only: bool, stats: FilterSummary) -> None:
    if not training_only:
        print("统计范围：**全部** m_size（含 64/128 系统小包）。")
    else:
        print(
            "统计范围：**仅训练消息** "
            f"{list(TRAINING_M_SIZE_ORDER)} B；"
            f"已排除系统小包 64/128 共 {stats.system_excluded:,} 条"
            + (
                f"，其它尺寸 {stats.other_excluded:,} 条"
                if stats.other_excluded
                else ""
            )
            + f"（原始 {stats.total_raw:,} 条，保留 {stats.training:,} 条）。"
        )
    print()


def RunFctAnalysisBuckets(records: List[FlowRecord], step: int) -> List[dict]:
    """按流大小百分位分桶，复现 fct_analysis.py 的 P50/P95/P99 慢化输出。"""
    pairs = sorted((r.Slowdown, r.m_size) for r in records)
    n = len(pairs)
    if n == 0:
        return []
    res = []
    for i in range(0, 100, step):
        l = i * n // 100
        r = (i + step) * n // 100
        if r <= l:
            r = min(l + 1, n)
        bucket = pairs[l:r]
        slowdowns = sorted(x[0] for x in bucket)
        res.append(
            {
                "pct": i / 100.0,
                "max_size": bucket[-1][1],
                "p50": GetPctl(slowdowns, 0.5),
                "p95": GetPctl(slowdowns, 0.95),
                "p99": GetPctl(slowdowns, 0.99),
                "count": len(bucket),
            }
        )
    return res


# ---------------------------------------------------------------------------
# 报告输出
# ---------------------------------------------------------------------------

def FormatInt(x: float) -> str:
    return f"{int(round(x)):,}"


def PrintGlobalTable(summaries: List[GlobalSummary]) -> None:
    print("## 全局统计（训练流）")
    print()
    hdr = f"| {'拓扑':<14} | {'流数':>12} | {'平均实际 FCT':>14} | {'平均 standalone':>16} | {'平均慢化':>8} |"
    sep = f"|{'-'*16}|{'-'*14}|{'-'*16}|{'-'*18}|{'-'*10}|"
    print(hdr)
    print(sep)
    for g in summaries:
        print(
            f"| {g.name:<14} | {g.flows:>12,} | {FormatInt(g.avg_fct):>14} | "
            f"{FormatInt(g.avg_standalone):>16} | {g.avg_slowdown:>8.2f} |"
        )
    print()


def PrintFctRanges(summaries: List[GlobalSummary]) -> None:
    parts = [
        f"{g.name} 实际 FCT [{g.min_fct:,}, {g.max_fct:,}]"
        for g in summaries
        if g.flows > 0
    ]
    if parts:
        print("实际 FCT 范围（ns）：" + "；".join(parts) + "。")
        print()


def PrintSizeTable(name: str, rows: List[SizeBucketSummary]) -> None:
    print(f"## 按训练消息大小分类 — {name}")
    print()
    print("（占比相对于**训练流**总数；不含 64/128 系统小包）")
    print()
    print(
        f"| {'m_size (B)':>12} | {'通信类型':<28} | {'条数':>10} | {'占比%':>7} | "
        f"{'平均实际 FCT':>14} | {'平均 standalone':>14} | {'平均慢化':>8} | 备注 |"
    )
    print(f"|{'-'*14}|{'-'*30}|{'-'*12}|{'-'*9}|{'-'*16}|{'-'*16}|{'-'*10}|{'-'*24}|")
    for r in rows:
        pct_str = f"{r.pct:.2f}" if r.pct >= 0.01 else f"{r.pct:.4f}"
        print(
            f"| {r.m_size:>12,} | {r.comm_label:<28} | {r.count:>10,} | {pct_str:>7} | "
            f"{FormatInt(r.avg_fct):>14} | {FormatInt(r.avg_standalone):>14} | "
            f"{r.avg_slowdown:>8.2f} | {r.remark} |"
        )
    print()


def PrintSingleReport(
    name: str,
    records: List[FlowRecord],
    step: int,
    show_buckets: bool,
    training_only: bool,
    filter_stats: FilterSummary,
) -> GlobalSummary:
    g = GlobalSummary(name=name)
    g.Compute(records)
    PrintFilterNote(training_only, filter_stats)
    PrintGlobalTable([g])
    PrintFctRanges([g])
    print(
        f"慢化比分位（{name}，训练流）：P50={g.slow_p50:.4f}  P95={g.slow_p95:.4f}  P99={g.slow_p99:.4f}"
    )
    print()
    PrintSizeTable(name, PerSizeSummaries(records, training_only=training_only))
    if show_buckets:
        print(f"### fct_analysis 分桶（训练流，按流大小百分位，step={step}）")
        print("# pct  max_size  P50_slow  P95_slow  P99_slow  (count)")
        for row in RunFctAnalysisBuckets(records, step):
            print(
                "{:.3f} {:10d} {:9.3f} {:9.3f} {:9.3f}  ({})".format(
                    row["pct"],
                    row["max_size"],
                    row["p50"],
                    row["p95"],
                    row["p99"],
                    row["count"],
                )
            )
        print()
    return g


def _MergeFilterStats(stats: List[FilterSummary]) -> FilterSummary:
    merged = FilterSummary()
    for s in stats:
        merged.total_raw += s.total_raw
        merged.training += s.training
        merged.system_excluded += s.system_excluded
        merged.other_excluded += s.other_excluded
    return merged


def AddTrainingFilterArgs(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--all-sizes",
        action="store_true",
        help="统计全部 m_size（含 64/128 系统小包）；默认仅训练消息 393216/15925248/63700992",
    )


def ParseCaseArg(s: str) -> Tuple[str, str]:
    if ":" not in s:
        raise argparse.ArgumentTypeError(f"需要 name:path，收到: {s}")
    name, path = s.split(":", 1)
    return name.strip(), path.strip()


def CmdSingle(args: argparse.Namespace) -> None:
    dport = GetDportFilter(args.type)
    raw = LoadFlowRecords(args.fct_file, dport, args.time_limit)
    training_only = not args.all_sizes
    records, fstats = PartitionFlows(raw, training_only)
    name = args.name or args.fct_file
    PrintSingleReport(name, records, args.step, args.buckets, training_only, fstats)


def CmdCompare(args: argparse.Namespace) -> None:
    dport = GetDportFilter(args.type)
    training_only = not args.all_sizes
    globals_list: List[GlobalSummary] = []
    size_tables: List[Tuple[str, List[SizeBucketSummary]]] = []
    filter_stats: List[FilterSummary] = []
    for name, path in args.cases:
        raw = LoadFlowRecords(path, dport, args.time_limit)
        records, fstats = PartitionFlows(raw, training_only)
        filter_stats.append(fstats)
        g = GlobalSummary(name=name)
        g.Compute(records)
        globals_list.append(g)
        if args.detail:
            size_tables.append((name, PerSizeSummaries(records, training_only=training_only)))

    print("# FCT 多拓扑对比")
    print()
    PrintFilterNote(training_only, _MergeFilterStats(filter_stats))
    PrintGlobalTable(globals_list)
    PrintFctRanges(globals_list)
    for name, rows in size_tables:
        PrintSizeTable(name, rows)
    if not args.detail:
        print("提示：加 --detail 输出每个拓扑的按 m_size 分组表。")
        print()


def BuildParser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SimAI FCT 标准分析报告（全局 + 按 m_size + 多拓扑对比）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 单个 FCT 文件
  python3 fct_standard_report.py single -n ROFT256 ../../simulation_output/ROFT256/ROFT256_fct.txt

  # 多拓扑对比（与论文/实验表一致）
  python3 fct_standard_report.py compare --detail \\
    --case HPN256:../../simulation_output/HPN256/HPN256_fct.txt \\
    --case meta256:../../simulation_output/meta256/meta256_fct.txt \\
    --case Zcube256:../../simulation_output/Zcube256/Zcube256_fct.txt \\
    --case DeepSeek256:../../simulation_output/DeepSeek256/DeepSeek256_fct.txt

  # 仅 normal 流（默认 -t 0），与 fct_analysis.py 一致
  python3 fct_standard_report.py compare -t 0 -T 3000000000 ...
        """,
    )
    parser.add_argument(
        "-t",
        dest="type",
        type=int,
        default=0,
        help="0: dport==100 (normal), 1: dport==200 (incast), 2: all",
    )
    parser.add_argument(
        "-T",
        dest="time_limit",
        type=int,
        default=10_000_000_000,
        help="仅统计 start_time+fct < T 的流（默认 10e9 ns；GPT-22B 中/大消息常在 3s 后完成）",
    )
    parser.add_argument("-s", dest="step", type=int, default=5, help="分桶步长（percentile step）")
    AddTrainingFilterArgs(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    p_single = sub.add_parser("single", help="分析单个 FCT 文件")
    p_single.add_argument("fct_file", help="FCT 日志路径")
    p_single.add_argument("-n", "--name", default=None, help="拓扑/实验显示名")
    p_single.add_argument(
        "--buckets",
        action="store_true",
        help="额外输出 fct_analysis 风格的分位分桶表",
    )
    p_single.set_defaults(func=CmdSingle)

    p_cmp = sub.add_parser("compare", help="对比多个拓扑的 FCT")
    p_cmp.add_argument(
        "--case",
        dest="cases",
        action="append",
        type=ParseCaseArg,
        metavar="NAME:PATH",
        required=True,
        help="拓扑名与 FCT 路径，可重复",
    )
    p_cmp.add_argument(
        "--detail",
        action="store_true",
        help="为每个拓扑打印按 m_size 分组表",
    )
    p_cmp.set_defaults(func=CmdCompare)

    return parser


def main() -> None:
    parser = BuildParser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
