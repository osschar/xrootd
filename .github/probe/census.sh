#!/usr/bin/env bash
#
# CI PROBE: describe the machine the tests run on. Reads /proc and /sys rather
# than calling lsblk, ip or free, which minimal containers do not have. Every
# section is best effort: a missing file prints as such and the census goes on.
#
# Usage: census.sh <dir>   -- the dir whose filesystem the tests write to

dir="${1:-.}"

section() { printf '\n=== %s\n' "$*"; }
show() { for f in "$@"; do printf '%s: ' "$f"; cat "$f" 2>/dev/null | tr '\n' ' ' || printf '(absent)'; echo; done; }

section "identity"
date -u '+%Y-%m-%dT%H:%M:%SZ'
uname -a
cat /etc/os-release 2>/dev/null | grep -E '^(PRETTY_NAME|VERSION_ID)='
echo "container: $( [[ -f /.dockerenv ]] && echo docker || echo none-detected )"
echo "RUNNER_NAME=${RUNNER_NAME:-} ImageOS=${ImageOS:-} ImageVersion=${ImageVersion:-}"

section "cpu"
echo "nproc: $(nproc)"
grep -m1 'model name' /proc/cpuinfo
show /sys/fs/cgroup/cpu.max

section "memory"
grep -E '^(MemTotal|MemAvailable|Dirty|Writeback|SwapTotal|SwapFree):' /proc/meminfo
show /proc/swaps /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max /sys/fs/cgroup/memory.high

section "writeback tunables"
for k in dirty_ratio dirty_background_ratio dirty_bytes dirty_background_bytes \
         dirty_expire_centisecs dirty_writeback_centisecs swappiness; do
	echo "vm.${k} = $(cat /proc/sys/vm/${k} 2>/dev/null)"
done

section "filesystem under ${dir}"
df -hT "${dir}" 2>/dev/null || df -h "${dir}"
stat -f -c 'fstype %T, bsize %s' "${dir}"
# The mount the dir lives on: longest mount-point prefix in mountinfo.
real="$(realpath "${dir}")"
awk -v p="${real}" '{ mp = $5; if (index(p, mp) == 1 && length(mp) >= best) { best = length(mp); line = $0 } }
                    END { print line }' /proc/self/mountinfo

section "block devices"
for b in /sys/block/*; do
	n="$(basename "$b")"
	case "$n" in loop*|ram*) continue ;; esac
	printf '%s: size_GiB=%s rotational=%s scheduler=%s nr_requests=%s model=%s\n' "$n" \
		"$(( $(cat "$b/size" 2>/dev/null || echo 0) / 2097152 ))" \
		"$(cat "$b/queue/rotational" 2>/dev/null)" "$(cat "$b/queue/scheduler" 2>/dev/null)" \
		"$(cat "$b/queue/nr_requests" 2>/dev/null)" "$(cat "$b/device/model" 2>/dev/null | tr -s ' ')"
done
show /sys/fs/cgroup/io.max

section "pressure (PSI) at start"
for r in cpu memory io; do echo "$r: $(tr '\n' ' ' < /proc/pressure/$r 2>/dev/null || echo absent)"; done

section "network: loopback and UDP"
show /sys/class/net/lo/mtu /proc/sys/net/core/rmem_default /proc/sys/net/core/rmem_max \
     /proc/sys/net/core/netdev_max_backlog /proc/sys/net/ipv6/conf/all/disable_ipv6 \
     /proc/sys/net/ipv6/conf/lo/disable_ipv6
echo "::1 on lo: $(grep -q '^0\{31\}1 .* lo$' /proc/net/if_inet6 2>/dev/null && echo yes || echo no)"
echo "/etc/hosts localhost lines:"; grep -w localhost /etc/hosts
python3 - <<'PY' 2>/dev/null || echo "python3 getaddrinfo check failed"
import socket
print("getaddrinfo(localhost, UDP):",
      [a[4][0] for a in socket.getaddrinfo("localhost", 7097, 0, socket.SOCK_DGRAM)])
PY
echo "UDP /proc/net/snmp:"; grep '^Udp:' /proc/net/snmp
grep '^Udp6' /proc/net/snmp6 2>/dev/null | tr '\n' ' '; echo

section "fsync latency, idle, 20 x 4 kB in ${dir}"
python3 - "${dir}" <<'PY' 2>/dev/null || echo "python3 fsync check failed"
import os, sys, time
p = os.path.join(sys.argv[1], ".census-fsync")
fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
lat = []
for i in range(20):
    os.pwrite(fd, os.urandom(4096), 0)
    t = time.monotonic(); os.fsync(fd); lat.append((time.monotonic() - t) * 1000)
os.close(fd); os.unlink(p)
lat.sort()
print("ms: min %.2f median %.2f max %.2f" % (lat[0], lat[len(lat) // 2], lat[-1]))
PY
