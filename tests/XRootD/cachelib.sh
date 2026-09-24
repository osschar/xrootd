#!/usr/bin/env bash
#
# Shared helpers for the XCache (XrdPfc) tests. Sourced by cache*.sh, each of
# which drives one cache configuration against the XRootD::host fixture.
#
# Two independent oracles are available, and they answer different questions:
#
#   * the pfc g-stream file_close record -- b_todisk is exactly how many bytes
#     came over the network on that open, and b_prefetch how much of that was
#     speculative. Both are counted per block as blocks are written out, so
#     they do not move with the client's request size.
#
#   * the .cinfo file, via xrdpfc_print -- the state left on disk: which blocks
#     are present, whether the file is complete, and the access history.
#
# Do not assert on B_hit/B_miss to mean "came from the network". PFC scores a
# block as missed only for the request that created it, so a second request
# landing on the same in-flight block reads as a hit, and with prefetching on a
# cold read of an uncached file reports 100% hit. That is upstream issue #2366.
# b_todisk is the field that states it correctly.

# Defaults. A config that differs must set these before calling cache_setup.
: "${PFC_BLOCKSIZE:=$((128 * 1024))}"

ORIGIN_HOST="root://localhost:5094"

# CI PROBE: how long a wait polls before giving up. 180 s rather than 20 s so
# that a late record can be told apart from one that never arrives.
: "${PFC_WAIT_SECS:=180}"
# A wait slower than this keeps a copy of the server log for later.
: "${PFC_SLOW_SECS:=5}"
# One line per wait, pass or fail, outside the test directory, which teardown
# removes.
: "${PFC_PROBE_DIR:=${BINARY_DIR:-${PWD}}/pfc-probe}"

#-------------------------------------------------------------------------------
# Paths. setup and run are separate processes, so derive rather than export.
#-------------------------------------------------------------------------------

# Local store of the cache: common.cfg points oss.localroot at what test.sh
# exports as REMOTE_DIR, so <lfn> and <lfn>.cinfo land there.
function cache_store() {
	echo "${REMOTE_DIR}"
}

function cinfo_path() {
	echo "$(cache_store)/$1.cinfo"
}

function gstream_log() {
	echo "${PWD}/${NAME}/gstream.json"
}

#-------------------------------------------------------------------------------
# Setup
#-------------------------------------------------------------------------------

# Start the g-stream collector. Call from setup_<name> with PFC_GSTREAM_PORT
# set to the destination port of the xrootd.mongstream line in the config.
function cache_setup() {
	require_commands cmp dd python3 xrdpfc-read xrdpfc_print

	# A test that asserts on b_todisk sets PFC_GSTREAM_PORT and gets a
	# collector; one that only reads the filesystem leaves it unset and does
	# without, so it needs neither a free UDP port nor a working g-stream.
	[[ -n "${PFC_GSTREAM_PORT:-}" ]] || return 0

	rm -f "$(gstream_log)"
	start_gstream_collector
}

# The helper detaches itself and writes its own pid file: a plain background job
# would be killed with the rest of this test's process group as soon as ctest
# reaps the setup step, and one that merely survived would hang the run by
# holding ctest's output pipe open. It returns once the port is bound, so the
# server cannot come up before the collector is listening. test.sh's generic
# teardown kills anything that left a pid file behind.
function start_gstream_collector() {
	assert python3 "${SOURCE_DIR}/utils/gstream_recv.py" \
		"${PFC_GSTREAM_PORT}" "$(gstream_log)" "${PWD}/${NAME}/gstream.pid"
}

# True while the collector started at setup is still running.
function gstream_collector_alive() {
	local pidfile="${PWD}/${NAME}/gstream.pid"
	[[ -s "${pidfile}" ]] && kill -0 "$(cat "${pidfile}")" 2>/dev/null
}

# Call at the top of a test body that asserts on the g-stream.
#
# The collector starts at setup, but ctest runs every setup first and the test
# bodies long afterwards -- in a full run that gap is eighty-odd tests and about
# ten minutes. CI loses one of these idle processes now and then, and since
# nothing restarts it, every g-stream assertion in that one test fails while the
# rest of the job is fine; the victim moved between platforms and tests on each
# run. Starting another one works: a UDP listener that comes back keeps
# receiving. The server does drop one datagram when it returns, the one whose
# send reports the ICMP queued from while the port was dead, but nothing is sent
# between setup and here, so there is none pending.
function ensure_gstream_collector() {
	[[ -n "${PFC_GSTREAM_PORT:-}" ]] || return 0
	gstream_collector_alive && return 0

	echo "g-stream collector is gone, starting another"
	start_gstream_collector
}

#-------------------------------------------------------------------------------
# Test data
#-------------------------------------------------------------------------------

# Size of a file in bytes. GNU stat -c%s and BSD stat -f%z disagree; wc -c is
# in POSIX and needs no branch on uname.
function file_bytes() {
	wc -c < "$1"
}

# make_origin_file <local> <lfn> <n_blocks> -- random file of n_blocks whole
# cache blocks, put on the origin. Whole blocks keep the expected byte counts
# free of rounding.
function make_origin_file() {
	assert dd if=/dev/urandom of="$1" bs="${PFC_BLOCKSIZE}" count="$3" status=none
	assert xrdcp -fs "$1" "${ORIGIN_HOST}//$2"
}

#-------------------------------------------------------------------------------
# The g-stream oracle
#-------------------------------------------------------------------------------

# Integer field $2 of JSON record $1.
function json_int() {
	echo "$1" | sed -n "s/.*\"$2\":\(-\{0,1\}[0-9]\{1,\}\).*/\1/p"
}

#-------------------------------------------------------------------------------
# CI PROBE: timed polling
#-------------------------------------------------------------------------------

function probe_now() {
	date +%s.%N
}

# probe_record <what> <t0> <outcome> [extra...] -- append one line to the
# probe log: wall time, test, outcome, seconds waited, what, extra.
function probe_record() {
	local what="$1" t0="$2" outcome="$3" t1 dt
	shift 3
	t1="$(probe_now)"
	dt="$(awk -v a="${t0}" -v b="${t1}" 'BEGIN { printf "%.2f", b - a }')"
	mkdir -p "${PFC_PROBE_DIR}"
	echo "${t1} ${NAME} ${outcome} ${dt} ${what} $*" >> "${PFC_PROBE_DIR}/waits.log"
	# Keep the evidence of a slow or failed wait: teardown deletes it.
	if [[ "${outcome}" != ok ]] ||
	   awk -v d="${dt}" -v s="${PFC_SLOW_SECS}" 'BEGIN { exit !(d > s) }'; then
		local tag
		tag="${NAME}-$(date +%H%M%S)-$$"
		cp "${PWD}/${NAME}/xrootd.log" "${PFC_PROBE_DIR}/${tag}-xrootd.log" 2>/dev/null || true
		cp "$(gstream_log)" "${PFC_PROBE_DIR}/${tag}-gstream.json" 2>/dev/null || true
		cp "$(gstream_log).recv" "${PFC_PROBE_DIR}/${tag}-gstream.recv" 2>/dev/null || true
	fi
}

# probe_poll <what> <command...> -- run command every 0.2 s until it succeeds
# or PFC_WAIT_SECS have passed. Records how long that took either way, and
# returns non-zero on timeout.
function probe_poll() {
	local what="$1" t0 deadline
	shift
	t0="$(probe_now)"
	deadline=$(( $(date +%s) + PFC_WAIT_SECS ))
	while true; do
		if "$@"; then
			probe_record "${what}" "${t0}" ok
			return 0
		fi
		if (( $(date +%s) >= deadline )); then
			probe_record "${what}" "${t0}" TIMEOUT
			return 1
		fi
		sleep 0.2
	done
}

# The file_close record for lfn $1 at access_cnt $2, if it has arrived.
function gstream_close_record() {
	grep '"event":"file_close"' "$(gstream_log)" 2>/dev/null |
	grep "\"lfn\":\"/$1\"" | grep "\"access_cnt\":$2," | tail -1
}

function have_gstream_close() {
	[[ -n "$(gstream_close_record "$1" "$2")" ]]
}

function have_any_gstream_close() {
	grep '"event":"file_close"' "$(gstream_log)" 2>/dev/null | grep -q "\"lfn\":\"/$1\""
}

# The file_close record for lfn $1 at access_cnt $2. The record is only emitted
# when the cache closes the file, which happens after the client has gone, and
# the g-stream is flushed on a timer on top of that -- so wait, do not race.
function wait_for_gstream_close() {
	if probe_poll "gstream_close /$1 acc=$2" have_gstream_close "$1" "$2"; then
		gstream_close_record "$1" "$2"
		return 0
	fi
	# Say whether the log is empty or merely missing this record: the first
	# means the collector never received anything, the second that the cache
	# did not close the file when expected. They need different fixes.
	error "timed out after ${PFC_WAIT_SECS} s waiting for g-stream file_close of /$1" \
	      "at access_cnt $2; $(gstream_diagnosis)"
}

# The blacklist test's negative: no file_close record for lfn $1 at all. The
# record, if one were coming, would be flushed within a couple of seconds of the
# close; wait longer than that before concluding it is absent.
function assert_no_gstream_close() {
	sleep 5
	if have_any_gstream_close "$1"; then
		error "unexpected g-stream file_close record for /$1:" \
		      "$(grep "\"lfn\":\"/$1\"" "$(gstream_log)")"
	fi
}

# Why might a record be missing? A dead collector and a cache that never closed
# the file need different fixes, and the bare timeout cannot tell them apart.
function gstream_diagnosis() {
	local n_all alive="no"
	n_all="$(grep -c '"event":"file_close"' "$(gstream_log)" 2>/dev/null || echo 0)"
	gstream_collector_alive && alive="yes"
	echo "collector alive: ${alive}, log holds ${n_all} file_close records"
}

# Total b_todisk over every file_close record for lfn $1 so far -- the bytes
# the origin has served for it since the cache started.
#
# Cumulative rather than per-record on purpose. Records are emitted when the
# cache closes a file and are then flushed on a timer, so which record a given
# read lands in, and when it appears in the log, both move around; a running
# total does not. Waits for the first record, then lets stragglers land.
function sum_gstream_todisk() {
	local rec
	if ! probe_poll "gstream_any_close /$1" have_any_gstream_close "$1"; then
		error "timed out after ${PFC_WAIT_SECS} s waiting for any g-stream file_close of /$1;" \
		      "$(gstream_diagnosis)"
	fi
	sleep 3

	local total=0
	while read -r rec; do
		[[ -n "${rec}" ]] || continue
		total=$((total + $(json_int "${rec}" b_todisk)))
	done < <(grep '"event":"file_close"' "$(gstream_log)" 2>/dev/null |
	         grep "\"lfn\":\"/$1\"" || true)

	echo "${total}"
}

# assert_fetched <lfn> <access_cnt> <expected b_todisk> <message>
function assert_fetched() {
	local rec
	rec="$(wait_for_gstream_close "$1" "$2")"
	assert_eq "$3" "$(json_int "${rec}" b_todisk)" "$4"
}

#-------------------------------------------------------------------------------
# The cinfo oracle
#-------------------------------------------------------------------------------

# "<n_blocks> <n_downloaded> <state>"
function cinfo_blocks() {
	xrdpfc_print -u B "$1" |
	sed -n 's/^file_size .*n_blocks \([0-9]*\), n_downloaded \([0-9]*\), state \([a-z]*\).*/\1 \2 \3/p'
}

function cinfo_n_acc() {
	xrdpfc_print -u B "$1" | sed -n 's/^Access records (N_acc_total=\([0-9]*\)).*/\1/p'
}

# "<B_hit> <B_miss> <B_bypass>" of access record $2, in bytes.
function cinfo_access() {
	xrdpfc_print -u B "$1" | awk -v rec="$2" '$1 == rec { print $(NF-2), $(NF-1), $NF; exit }'
}

# The block presence map as a string of x and . , one char per block.
function cinfo_block_map() {
	xrdpfc_print -u B -v "$1" |
	sed -n '/^printing /,/^Access records/p' | sed -n 's/^ *[0-9]\{1,\} \([x.]\{1,\}\)$/\1/p' | tr -d '\n'
}

# Dump what a cinfo actually holds. Called when a wait gives up: a bare
# "timed out" says nothing about whether the value was short, exact or already
# past the one waited for, and that is the first thing you need to know.
function report_cinfo_state() {
	if [[ -f "$1" ]]; then
		echo "  cinfo $1: n_acc=$(cinfo_n_acc "$1") blocks=$(cinfo_blocks "$1")"
	else
		echo "  cinfo $1: does not exist"
	fi
}

# Wait for the cinfo to report exactly $2 access records.
#
# Use this only where the count is genuinely determined. An access record is
# appended when a File attaches, and the cinfo is written and synced repeatedly
# over the File's life -- not once at close -- so the count is live, and an open
# that is refused after the File was constructed still leaves a record behind.
# Where that can happen, wait on the final state instead: see
# wait_for_cinfo_complete().
function has_access_record() {
	[[ -f "$1" ]] && [[ "$(cinfo_n_acc "$1")" == "$2" ]]
}

function wait_for_access_record() {
	probe_poll "access_record $(basename "$1") n=$2" has_access_record "$1" "$2" && return 0
	echo "timed out after ${PFC_WAIT_SECS} s waiting for access record $2 of $1"
	report_cinfo_state "$1"
	error "timed out after ${PFC_WAIT_SECS} s waiting for access record $2 of $1"
}

# Wait for every block of the file to be on disk.
#
# Completeness is monotonic -- a file becomes complete once and stays complete
# -- so unlike an access count there is nothing to overshoot and no baseline to
# sample. Prefer this wherever the assertion that follows is about the file
# being fully cached.
function is_cinfo_complete() {
	local nblk ndone state
	[[ -f "$1" ]] || return 1
	read -r nblk ndone state <<< "$(cinfo_blocks "$1")"
	[[ "${state}" == complete ]] && [[ "${nblk}" == "${ndone}" ]]
}

function wait_for_cinfo_complete() {
	probe_poll "cinfo_complete $(basename "$1")" is_cinfo_complete "$1" && return 0
	echo "timed out after ${PFC_WAIT_SECS} s waiting for $1 to be complete"
	report_cinfo_state "$1"
	error "timed out after ${PFC_WAIT_SECS} s waiting for $1 to be complete"
}
