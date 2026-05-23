#!/usr/bin/env python3
"""按 fct_analysis.py 逻辑分析单个 FCT 文件（慢化比按流大小分桶）。"""

import argparse
import sys


def GetPctl(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    i = int(len(sorted_vals) * p)
    if i >= len(sorted_vals):
        i = len(sorted_vals) - 1
    return sorted_vals[i]


def LoadSlowdownPairs(filepath, dport_filter, time_limit):
    """返回 [(slowdown, m_size), ...] 按 m_size 已排序"""
    pairs = []
    with open(filepath, "r") as f:
        for line in f:
            parts = line.split()
            if len(parts) != 8:
                continue
            dport = int(parts[3])
            m_size = int(parts[4])
            start = int(parts[5])
            fct = int(parts[6])
            standalone = int(parts[7])
            if start + fct >= time_limit:
                continue
            if dport_filter is not None and dport != dport_filter:
                continue
            slow = fct / standalone if standalone > 0 else 1.0
            if slow < 1.0:
                slow = 1.0
            pairs.append((slow, m_size))
    pairs.sort(key=lambda x: x[1])
    return pairs


def RunFctAnalysis(pairs, step):
    """复现 fct_analysis.py 输出：按流大小分位桶的 P50/P95/P99 慢化"""
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
        max_size = bucket[-1][1]
        res.append({
            "pct": i / 100.0,
            "size": max_size,
            "p50": GetPctl(slowdowns, 0.5),
            "p95": GetPctl(slowdowns, 0.95),
            "p99": GetPctl(slowdowns, 0.99),
            "count": len(bucket),
        })
    return res


def PerSizeStats(pairs):
    from collections import defaultdict
    by_size = defaultdict(list)
    for slow, sz in pairs:
        by_size[sz].append(slow)
    rows = []
    for sz in sorted(by_size):
        s = sorted(by_size[sz])
        rows.append({
            "size": sz,
            "count": len(s),
            "p50": GetPctl(s, 0.5),
            "p95": GetPctl(s, 0.95),
            "p99": GetPctl(s, 0.99),
            "avg": sum(s) / len(s),
        })
    return rows


def GlobalStats(pairs):
    slowdowns = sorted(x[0] for x in pairs)
    fcts = []
    return {
        "flows": len(pairs),
        "slow_p50": GetPctl(slowdowns, 0.5),
        "slow_p95": GetPctl(slowdowns, 0.95),
        "slow_p99": GetPctl(slowdowns, 0.99),
        "slow_avg": sum(slowdowns) / len(slowdowns),
        "sizes": sorted(set(x[1] for x in pairs)),
    }


def main():
    parser = argparse.ArgumentParser(description="单文件 FCT 分析（fct_analysis 风格）")
    parser.add_argument("fct_file", help="FCT 文件路径")
    parser.add_argument("-s", dest="step", type=int, default=5)
    parser.add_argument("-t", dest="type", type=int, default=0,
                        help="0: dport==100, 1: dport==200, 2: all")
    parser.add_argument("-T", dest="time_limit", type=int, default=3000000000)
    args = parser.parse_args()

    if args.type == 0:
        dport_filter = 100
    elif args.type == 1:
        dport_filter = 200
    else:
        dport_filter = None

    print(f"文件: {args.fct_file}")
    print(f"类型: {'normal(dport=100)' if dport_filter == 100 else 'incast(dport=200)' if dport_filter == 200 else 'all'}")
    print(f"time_limit: {args.time_limit}")
    print()

    pairs = LoadSlowdownPairs(args.fct_file, dport_filter, args.time_limit)
    g = GlobalStats(pairs)
    print("=== 全局 ===")
    print(f"  流数量: {g['flows']}")
    print(f"  消息大小集合: {g['sizes']}")
    print(f"  慢化比 avg / P50 / P95 / P99: {g['slow_avg']:.4f} / {g['slow_p50']:.4f} / {g['slow_p95']:.4f} / {g['slow_p99']:.4f}")
    print()

    print("=== 按消息大小 ===")
    print(f"{'m_size':>14} {'count':>12} {'avg_slow':>10} {'P50':>8} {'P95':>8} {'P99':>8}")
    for r in PerSizeStats(pairs):
        print(f"{r['size']:14d} {r['count']:12d} {r['avg']:10.4f} {r['p50']:8.4f} {r['p95']:8.4f} {r['p99']:8.4f}")
    print()

    print("=== fct_analysis 分桶 (按流大小百分位, step={}) ===".format(args.step))
    print("# pct  max_size  P50_slow  P95_slow  P99_slow  (count)")
    for row in RunFctAnalysis(pairs, args.step):
        print("{:.3f} {:10d} {:9.3f} {:9.3f} {:9.3f}  ({})".format(
            row["pct"], row["size"], row["p50"], row["p95"], row["p99"], row["count"]))


if __name__ == "__main__":
    main()
