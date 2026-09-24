#!/usr/bin/env python3
"""CI PROBE: summarise downloaded probe artifacts.

Usage: analyze.py <dir> [slow_secs]

<dir> holds one sub-directory per job, as `gh run download` lays them out
(probe-fedora-clang-Debug/, ...). For each job prints the machine in a few
lines, the fsync canary's distribution and worst moments, UDP canary loss, and
every cache-test wait slower than slow_secs (default 3) with what the sampler
saw during that wait: the worst fsync, peak dirty and writeback memory, peak IO
pressure.
"""

import glob
import os
import sys


def tsv(path):
    rows = []
    try:
        with open(path) as f:
            for line in f:
                if line.startswith("#") or not line.strip():
                    continue
                rows.append(line.rstrip("\n").split("\t"))
    except OSError:
        pass
    return rows


def fnum(s, default=0.0):
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def census_lines(path):
    keep = ("nproc:", "model name", "MemTotal", "SwapTotal", "vm.dirty_ratio",
            "vm.dirty_background_ratio", "fstype", "size_GiB", "memory.max:",
            "io.max:", "/sys/class/net/lo/mtu", "rmem_default", "getaddrinfo",
            "::1 on lo", "ms: min", "ImageVersion")
    out = []
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return ["(no census)"]
    for i, line in enumerate(lines):
        if any(k in line for k in keep):
            out.append(line.strip())
        if line.startswith("=== filesystem"):
            out += [l.strip() for l in lines[i + 1:i + 4] if l.strip()]
    return out


def job(d, slow):
    name = os.path.basename(d.rstrip("/"))
    print("=" * 78)
    print(name)
    print("-" * 78)
    for line in census_lines(os.path.join(d, "census.txt")):
        print("  " + line)

    fs = tsv(os.path.join(d, "fsync.tsv"))
    lat = [(fnum(r[0]), fnum(r[2])) for r in fs if len(r) >= 3]
    ms = [x for _, x in lat]
    print("\n  fsync canary: n=%d median=%.1f p99=%.1f max=%.1f ms"
          % (len(ms), pct(ms, 0.5), pct(ms, 0.99), max(ms) if ms else float("nan")))
    for t, x in sorted(lat, key=lambda r: -r[1])[:5]:
        print("    worst: t=%.1f fsync %.0f ms" % (t, x))
    # A stalled fsync also shows as a gap between samples.
    gaps = [(lat[i][0], lat[i + 1][0] - lat[i][0]) for i in range(len(lat) - 1)]
    big = [g for g in gaps if g[1] > 2.0]
    if big:
        print("    sample gaps > 2 s: %d, largest %.1f s at t=%.1f"
              % (len(big), max(g[1] for g in big), max(big, key=lambda g: g[1])[0]))

    sent = tsv(os.path.join(d, "udp_send.tsv"))
    recv = tsv(os.path.join(d, "udp_recv.tsv"))
    got = {r[1] for r in recv if len(r) >= 2}
    errs = [r for r in sent if len(r) >= 4 and r[3]]
    for size in ("300", "24000"):
        s = [r for r in sent if len(r) >= 3 and r[2] == size]
        lost = [r for r in s if r[1] not in got]
        print("  UDP %5s B: sent %d lost %d%s" % (size, len(s), len(lost),
              (", first lost at t=%s" % lost[0][0]) if lost else ""))
    if errs:
        print("  UDP send errors: %d, e.g. %s" % (len(errs), errs[0]))
    delays = [fnum(r[0]) - fnum(r[3]) for r in recv if len(r) >= 4]
    if delays:
        print("  UDP delay: median %.1f ms, max %.1f ms" % (pct(delays, 0.5) * 1e3, max(delays) * 1e3))

    st = tsv(os.path.join(d, "stats.tsv"))

    def window(t0, t1):
        f = [x for t, x in lat if t0 - 0.5 <= t <= t1]
        s = [r for r in st if t0 <= fnum(r[0]) <= t1]
        return (max(f) if f else float("nan"),
                max((fnum(r[1]) for r in s), default=0) / 1024,
                max((fnum(r[2]) for r in s), default=0) / 1024,
                max((fnum(r[6]) for r in s), default=0),
                max((fnum(r[14]) for r in s), default=0))

    waits = []
    try:
        with open(os.path.join(d, "waits", "waits.log")) as f:
            for line in f:
                p = line.split()
                if len(p) >= 5:
                    waits.append((fnum(p[0]), p[1], p[2], fnum(p[3]), " ".join(p[4:])))
    except OSError:
        pass
    n_to = sum(1 for w in waits if w[2] != "ok")
    print("\n  waits: %d, timeouts %d, slower than %.0f s: %d"
          % (len(waits), n_to, slow, sum(1 for w in waits if w[3] > slow)))
    for t, test, outcome, dt, what in sorted(waits, key=lambda w: -w[3]):
        if dt <= slow:
            break
        fmax, dmax, wbmax, iofull, blocked = window(t - dt, t)
        print("    %-8s %6.1f s  %-15s %s" % (outcome, dt, test, what))
        print("             during it: fsync max %.0f ms, dirty max %.0f MB, "
              "writeback max %.0f MB, io full avg10 max %.1f, blocked max %.0f"
              % (fmax, dmax, wbmax, iofull, blocked))
    kept = sorted(glob.glob(os.path.join(d, "waits", "*-xrootd.log")))
    if kept:
        print("  kept server logs: %d (in %s/waits/)" % (len(kept), name))
    print()


def main():
    root = sys.argv[1]
    slow = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
    dirs = sorted(d for d in glob.glob(os.path.join(root, "*")) if os.path.isdir(d))
    for d in dirs:
        job(d, slow)


if __name__ == "__main__":
    main()
