#!/usr/bin/env bash
#
# CI PROBE: run a command -- the ctest step -- with the census before it and
# the sampler running alongside it. Exits with the command's status, so the
# job still goes red or green on the tests themselves.
#
# Usage: run-with-probe.sh <command...>
#
# Output lands in ${GITHUB_WORKSPACE}/probe, on the same filesystem as build/,
# which is where the tests write and so where the fsync canary has to be.

here="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
ws="${GITHUB_WORKSPACE:-$(pwd)}"
probe="${ws}/probe"
mkdir -p "${probe}"
chmod 1777 "${probe}"  # the tests run as 'runner' in the containers

bash "${here}/census.sh" "${ws}" 2>&1 | tee "${probe}/census.txt"

python3 "${here}/sampler.py" "${probe}" "${probe}/canary" &
sampler=$!

# Let the tests' helpers put their timing log next to the rest.
export PFC_PROBE_DIR="${probe}/waits"
mkdir -p "${PFC_PROBE_DIR}"
chmod 1777 "${PFC_PROBE_DIR}"

"$@"
rc=$?

kill -TERM "${sampler}" 2>/dev/null
wait "${sampler}" 2>/dev/null

# Scheduler lost-wakeup A/B on this runner. The same reproducer twice: against
# the tree's libXrdUtils, which carries the fix, and with the scheduler from
# before the fix compiled into the executable, whose definitions then override
# the library's. Best effort; it never changes the exit status.
(
	cd "${ws}" || exit 0
	cxx="$(command -v c++ || command -v g++ || command -v clang++)" || exit 0
	# master's XrdScheduler.cc, unmodified, is kept next to this script: deriving
	# it from history breaks as soon as a second commit touches the file.
	cp "${here}/XrdScheduler-before-fix.cc" "${probe}/XrdScheduler-before-fix.cc"
	lib="${ws}/build/lib/libXrdUtils.so"
	# Name the library file itself: with -lXrdUtils a missing build tree
	# silently falls back to a system libXrdUtils, which has no fix.
	[[ -e "${lib}" ]] || { echo "sched_race skipped: no ${lib}"; exit 0; }
	flags=(-O2 -std=c++17 -Isrc -Ibuild/src -Ibuild/include -pthread)
	link=("${lib}" "-Wl,-rpath,${ws}/build/lib")
	"${cxx}" "${flags[@]}" "${here}/sched_race.cc" "${link[@]}" -o "${probe}/sched_race" &&
		"${probe}/sched_race" 90 2 3 780 > "${probe}/sched_race-fixed.txt" 2>&1
	"${cxx}" "${flags[@]}" "${here}/sched_race.cc" "${probe}/XrdScheduler-before-fix.cc" "${link[@]}" \
		-o "${probe}/sched_race_before" &&
		"${probe}/sched_race_before" 90 2 3 780 > "${probe}/sched_race-before-fix.txt" 2>&1
	echo "sched_race loads: $(ldd "${probe}/sched_race" 2>/dev/null | grep -o '/[^ ]*libXrdUtils[^ ]*')"
	rm -f "${probe}/sched_race" "${probe}/sched_race_before"
	echo "sched_race with fix:    $(tail -1 "${probe}/sched_race-fixed.txt" 2>/dev/null)"
	echo "sched_race before fix:  $(tail -1 "${probe}/sched_race-before-fix.txt" 2>/dev/null)"
) 2>&1 | tee "${probe}/sched_race-build.txt"

# The same census at the end, for the counters that moved.
bash "${here}/census.sh" "${ws}" > "${probe}/census-end.txt" 2>&1

exit "${rc}"
