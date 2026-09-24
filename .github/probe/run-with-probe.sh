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

# The same census at the end, for the counters that moved.
bash "${here}/census.sh" "${ws}" > "${probe}/census-end.txt" 2>&1

exit "${rc}"
