#!/usr/bin/env python3
"""CI PROBE: sample the machine while the test suite runs.

Three threads, each writing its own line-buffered TSV into <outdir>, all on the
wall clock (time.time()) so they line up with each other, with the probe's
waits.log and with the timestamps GitHub puts on every log line:

  stats.tsv   every 1 s: dirty and writeback memory, PSI pressure, run queue,
              swap and page-out counters, cgroup dirty/writeback if present
  fsync.tsv   every 0.5 s: pwrite 4 kB + fsync of a file in <workdir>, which
              is on the filesystem the tests write to; the latency is the
              question, so a stalled fsync simply shows up as a long one
  udp_send.tsv / udp_recv.tsv
              every 0.5 s one 300 B and one 24000 B numbered datagram to a
              local receiver, the two sizes bracketing a file_close record
              and the tests' g-stream maxlen

The fsync canary answers "does fsync stall on these machines while the suite
runs, and for how long". The UDP canary answers "does loopback UDP lose
datagrams here at all". Neither decides anything: this only records.

Usage: sampler.py <outdir> <workdir> [udp_port]
Stops on SIGTERM or SIGINT.
"""

import os
import signal
import socket
import sys
import threading
import time

outdir, workdir = sys.argv[1], sys.argv[2]
udp_port = int(sys.argv[3]) if len(sys.argv) > 3 else 7199
os.makedirs(outdir, exist_ok=True)
os.makedirs(workdir, exist_ok=True)

stop = threading.Event()


def out(name, header):
    f = open(os.path.join(outdir, name), "a", buffering=1)
    f.write("# " + header + "\n")
    return f


def read_kv(path, sep=None):
    d = {}
    try:
        with open(path) as f:
            for line in f:
                parts = line.split(sep) if sep else line.split()
                if len(parts) >= 2:
                    d[parts[0].rstrip(":")] = parts[1]
    except OSError:
        pass
    return d


def psi(res):
    # "some avg10=0.00 avg60=0.00 avg300=0.00 total=0" -> some_avg10, some_total, full_...
    r = {}
    try:
        with open("/proc/pressure/" + res) as f:
            for line in f:
                kind, *fields = line.split()
                for fld in fields:
                    k, v = fld.split("=")
                    if k in ("avg10", "total"):
                        r[kind + "_" + k] = v
    except OSError:
        pass
    return r


def stats_loop():
    cols = ["t", "dirty_kB", "writeback_kB", "memavail_kB", "swapused_kB",
            "io_some_avg10", "io_full_avg10", "io_some_total_us", "io_full_total_us",
            "mem_some_total_us", "mem_full_total_us", "cpu_some_total_us",
            "load1", "procs_running", "procs_blocked",
            "pgpgout", "pswpin", "pswpout", "cg_file_dirty", "cg_file_writeback"]
    f = out("stats.tsv", "\t".join(cols))
    while not stop.is_set():
        t = time.time()
        mi = read_kv("/proc/meminfo")
        vm = read_kv("/proc/vmstat")
        st = read_kv("/proc/stat")
        cg = read_kv("/sys/fs/cgroup/memory.stat")
        io, mem, cpu = psi("io"), psi("memory"), psi("cpu")
        try:
            load1 = open("/proc/loadavg").read().split()[0]
        except OSError:
            load1 = ""
        swapused = ""
        if "SwapTotal" in mi and "SwapFree" in mi:
            swapused = str(int(mi["SwapTotal"]) - int(mi["SwapFree"]))
        row = ["%.3f" % t, mi.get("Dirty", ""), mi.get("Writeback", ""),
               mi.get("MemAvailable", ""), swapused,
               io.get("some_avg10", ""), io.get("full_avg10", ""),
               io.get("some_total", ""), io.get("full_total", ""),
               mem.get("some_total", ""), mem.get("full_total", ""),
               cpu.get("some_total", ""), load1,
               st.get("procs_running", ""), st.get("procs_blocked", ""),
               vm.get("pgpgout", ""), vm.get("pswpin", ""), vm.get("pswpout", ""),
               cg.get("file_dirty", ""), cg.get("file_writeback", "")]
        f.write("\t".join(row) + "\n")
        stop.wait(max(0.0, 1.0 - (time.time() - t)))


def fsync_loop():
    f = out("fsync.tsv", "t_start\twrite_ms\tfsync_ms")
    path = os.path.join(workdir, "fsync-canary.dat")
    fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
    buf = os.urandom(4096)
    while not stop.is_set():
        t = time.time()
        m0 = time.monotonic()
        os.pwrite(fd, buf, 0)
        m1 = time.monotonic()
        os.fsync(fd)
        m2 = time.monotonic()
        f.write("%.3f\t%.3f\t%.3f\n" % (t, (m1 - m0) * 1e3, (m2 - m1) * 1e3))
        stop.wait(max(0.0, 0.5 - (time.monotonic() - m0)))
    os.close(fd)
    os.unlink(path)


def udp_recv_loop(sock):
    f = out("udp_recv.tsv", "t_recv\tseq\tsize\tt_send")
    sock.settimeout(0.5)
    while not stop.is_set():
        try:
            data, _ = sock.recvfrom(65536)
        except socket.timeout:
            continue
        t = time.time()
        seq, size, t_send = data[:64].split(b" ")[:3]
        f.write("%.6f\t%s\t%d\t%s\n" % (t, seq.decode(), len(data), t_send.decode()))


def udp_send_loop():
    f = out("udp_send.tsv", "t_send\tseq\tsize\terror")
    # Resolve the way the tests' config names the collector.
    fam, _, _, _, addr = socket.getaddrinfo("localhost", udp_port, 0, socket.SOCK_DGRAM)[0]
    f.write("# destination %s family %s\n" % (addr, fam))
    sock = socket.socket(fam, socket.SOCK_DGRAM)
    seq = 0
    while not stop.is_set():
        t0 = time.monotonic()
        for size in (300, 24000):
            seq += 1
            t = time.time()
            head = b"%d %d %.6f " % (seq, size, t)
            err = ""
            try:
                sock.sendto(head + b"x" * (size - len(head)), addr)
            except OSError as e:
                err = e.strerror or str(e)
            f.write("%.6f\t%d\t%d\t%s\n" % (t, seq, size, err))
        stop.wait(max(0.0, 0.5 - (time.monotonic() - t0)))


def main():
    rsock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    rsock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    rsock.bind(("::", udp_port))

    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    threads = [threading.Thread(target=stats_loop, daemon=True),
               threading.Thread(target=fsync_loop, daemon=True),
               threading.Thread(target=udp_recv_loop, args=(rsock,), daemon=True),
               threading.Thread(target=udp_send_loop, daemon=True)]
    for th in threads:
        th.start()
    while not stop.is_set():
        stop.wait(1.0)
    # Give a stalled fsync a moment to report, but do not hang the job on it.
    for th in threads:
        th.join(timeout=5)


if __name__ == "__main__":
    main()
