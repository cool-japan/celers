#!/usr/bin/env bash
#
# One entry point for CeleRS's env-gated, live-service integration tests.
#
# Brings up the docker-compose services each gated suite needs, waits for
# REAL health through the host-forwarded port (not just "the port is open" --
# see "Docker Desktop port-forward wedge" below), exports the eight
# CELERS_TEST_*-family variables documented in tests/integration/README.md,
# runs each crate's gated suite the way that README's "Per-service triage"
# section does, and prints a PASS/FAIL/SKIP summary.
#
#   scripts/test-integration.sh                 # bring up everything, run everything
#   scripts/test-integration.sh --only redis     # just the Redis-gated suites
#   scripts/test-integration.sh --only mysql     # celers-broker-sql + celers-backend-db
#   scripts/test-integration.sh --full           # also python-compat + a docker build smoke
#   scripts/test-integration.sh --keep-up        # don't stop what this run started
#   scripts/test-integration.sh --down-v --only postgres   # fresh postgres volume first
#
# Exit status: 0 if every attempted suite passed, 1 if any failed, 2 for a
# usage or environment problem (bad flag, docker not found, ...).
#
# This script only ever touches services it starts itself -- if a service is
# already answering when a run begins (this repo's docker-compose stack is
# routinely left running across sessions, and other agents/developers may be
# using it right now), it is left running afterwards regardless of
# --keep-up. --down-v is scoped narrowly to the one named service's own
# volume (see reset_service_volume below for why `docker compose down -v`
# itself is not used here).
#
# Prerequisites this script will NOT install for you: docker, docker compose
# (the `docker compose` plugin or the standalone `docker-compose`), and a
# cargo toolchain. redis-cli and curl are used when present for a couple of
# the readiness probes but are not required -- see tcp_probe.
#
# shellcheck disable=SC2329 # Several functions below (tcp_probe, the per-
# service probe_* functions, run_backend_db_postgres_half,
# run_backend_db_mysql_half, run_celers_facade, teardown) are called
# only indirectly -- passed by name to ensure_service/wait_for_health/
# run_suite, which invoke them via "$probe"/"$@", or registered with `trap`
# -- which shellcheck's static call-graph analysis does not follow.

set -euo pipefail

# ---------------------------------------------------------------------------
# Repo root (works from any cwd) and shared constants
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

REDIS_URL="redis://127.0.0.1:6379"
POSTGRES_URL="postgres://celers:celers_password@127.0.0.1:5432/celers"
AMQP_URL="amqp://celers:celers_password@127.0.0.1:5672/%2f"
MYSQL_URL_MAIN="mysql://celers:celers_password@127.0.0.1:3306/celers_test"
MYSQL_URL_BACKEND_DB="mysql://celers:celers_password@127.0.0.1:3306/celers_backend_db_test"
# A third, disposable MySQL database for celers-broker-sql's
# `tests_hardening::migration_upgrade` suite, which proves the
# `celers_task_results` -> `celers_broker_results` rename. That suite drops and
# recreates both result tables, so it must not share a schema with anything
# else; the test itself refuses to run if this names the same database as
# CELERS_TEST_MYSQL_URL.
MYSQL_URL_UPGRADE="mysql://celers:celers_password@127.0.0.1:3306/celers_upgrade_test"
SQS_URL="http://127.0.0.1:4566"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log() { printf '%s\n' "$*" >&2; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
err() { printf 'ERROR: %s\n' "$*" >&2; }

# ---------------------------------------------------------------------------
# Usage / flags
# ---------------------------------------------------------------------------

usage() {
  cat <<'EOF'
Usage: scripts/test-integration.sh [options]

Options:
  --only <service>   Scope to one service: redis|postgres|mysql|rabbitmq|localstack (alias: sqs)
  --keep-up          Do not stop services this run started (default: stop them)
  --down-v           Recreate the involved service(s)' own volume before starting
                      (fixes schema drift; see reset_service_volume for exactly
                      which volume this touches)
  --full             Also run tests/python-compat/run.sh and a docker build smoke test
  -h, --help         Show this help
EOF
}

ONLY=""
KEEP_UP=0
DOWN_V=0
FULL=0

while [ $# -gt 0 ]; do
  case "$1" in
    --only)
      [ $# -ge 2 ] || { err "--only needs a value"; usage >&2; exit 2; }
      ONLY="$2"
      shift 2
      ;;
    --only=*)
      ONLY="${1#--only=}"
      shift
      ;;
    --keep-up)
      KEEP_UP=1
      shift
      ;;
    --down-v)
      DOWN_V=1
      shift
      ;;
    --full)
      FULL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "unknown argument: $1"
      usage >&2
      exit 2
      ;;
  esac
done

case "$ONLY" in
  ""|redis|postgres|mysql|rabbitmq|localstack|sqs) ;;
  *)
    err "unknown --only value '${ONLY}' (expected redis|postgres|mysql|rabbitmq|localstack)"
    exit 2
    ;;
esac
if [ "$ONLY" = "sqs" ]; then
  ONLY="localstack"
fi

if ! command -v docker >/dev/null 2>&1; then
  err "docker was not found on PATH"
  exit 2
fi

# ---------------------------------------------------------------------------
# docker compose dispatch (plugin form preferred, standalone binary as fallback)
# ---------------------------------------------------------------------------

if docker compose version >/dev/null 2>&1; then
  compose_cmd() { docker compose "$@"; }
elif command -v docker-compose >/dev/null 2>&1; then
  compose_cmd() { docker-compose "$@"; }
else
  err "neither 'docker compose' nor 'docker-compose' is available"
  exit 2
fi

compose_up() {
  local svc="$1" profile="$2"
  if [ -n "$profile" ]; then
    compose_cmd --profile "$profile" up -d "$svc"
  else
    compose_cmd up -d "$svc"
  fi
}

compose_restart() {
  local svc="$1" profile="$2"
  if [ -n "$profile" ]; then
    compose_cmd --profile "$profile" restart "$svc"
  else
    compose_cmd restart "$svc"
  fi
}

compose_stop() {
  local svc="$1" profile="$2"
  if [ -n "$profile" ]; then
    compose_cmd --profile "$profile" stop "$svc"
  else
    compose_cmd stop "$svc"
  fi
}

compose_rm() {
  local svc="$1" profile="$2"
  if [ -n "$profile" ]; then
    compose_cmd --profile "$profile" rm -f "$svc"
  else
    compose_cmd rm -f "$svc"
  fi
}

compose_service_running() {
  local svc="$1" profile="$2" ids
  if [ -n "$profile" ]; then
    ids="$(compose_cmd --profile "$profile" ps -q "$svc" 2>/dev/null)"
  else
    ids="$(compose_cmd ps -q "$svc" 2>/dev/null)"
  fi
  [ -n "$ids" ]
}

compose_project_name() {
  compose_cmd config 2>/dev/null | sed -n 's/^name: *//p' | head -1
}

# volume_key_for_service SERVICE
# The docker-compose.yml top-level volume key backing SERVICE's data, or
# empty if it declares none (localstack: no persistent volume is declared,
# so a fresh container already is a fresh reset).
volume_key_for_service() {
  case "$1" in
    redis) printf '%s' "redis-data" ;;
    postgres) printf '%s' "postgres-data" ;;
    mysql) printf '%s' "mysql-data" ;;
    rabbitmq) printf '%s' "rabbitmq-data" ;;
    *) printf '%s' "" ;;
  esac
}

# reset_service_volume SERVICE PROFILE
#
# Stops and removes SERVICE's container, then removes exactly its own named
# volume -- resolved through Docker's own `com.docker.compose.*` labels,
# never a guessed `<project>_<volume>` string.
#
# `docker compose down -v <service>` is deliberately NOT used here: verified
# empirically (a throwaway two-service compose project, one `down -v` naming
# only one service) that `-v` ignores the service-name scope and attempts to
# remove *every* volume declared in the compose file, regardless of which
# service was named -- a volume backing a container that happens to be
# running at that moment fails to remove ("still in use") and is silently
# skipped, but an idle one (e.g. grafana-data or prometheus-data when those
# containers aren't up) would be deleted right along with the one volume
# actually being asked for. That is not an acceptable risk in a checkout
# shared with other running agents.
reset_service_volume() {
  local svc="$1" profile="$2" key project vol

  compose_stop "$svc" "$profile" 2>/dev/null || true
  compose_rm "$svc" "$profile" 2>/dev/null || true

  key="$(volume_key_for_service "$svc")"
  if [ -z "$key" ]; then
    return 0
  fi

  project="$(compose_project_name)"
  if [ -z "$project" ]; then
    warn "could not resolve the compose project name -- skipping volume removal for ${svc}"
    return 0
  fi

  vol="$(docker volume ls \
    --filter "label=com.docker.compose.project=${project}" \
    --filter "label=com.docker.compose.volume=${key}" \
    --format '{{.Name}}' 2>/dev/null | head -1)"
  if [ -z "$vol" ]; then
    log "  ${svc}: no existing volume to remove"
    return 0
  fi
  log "  removing volume ${vol} for a fresh ${svc}"
  # `|| true` plus the explicit `return 0`: this function runs as a bare
  # statement (some call sites via `&&`) before any suite has attempted to
  # run, so a failure here must never trip `set -e` and exit the script
  # without printing the PASS/FAIL/SKIP summary -- warn instead and let
  # ensure_service's own health check surface a volume that is genuinely
  # still in use (its declare/migration would then simply behave as if
  # --down-v had not been passed).
  if ! docker volume rm "$vol" >/dev/null 2>&1; then
    warn "could not remove volume ${vol} (possibly still in use) -- continuing without a fresh ${svc} volume"
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Readiness probes -- real client round trips through the host-forwarded
# port, not `nc -z` (which the documented Docker Desktop wedge passes even
# while wedged) and not `docker exec ... <healthcheck>` (which only proves
# the container's own loopback works, not that the host's port forward
# does).
# ---------------------------------------------------------------------------

# tcp_probe HOST PORT [PAYLOAD]
#
# Connects to HOST:PORT, optionally writes PAYLOAD (a printf %b string --
# e.g. protocol bytes via \xNN escapes), then waits up to 3s for at least one
# byte of reply. Returns success only if the server actually answered, which
# is what distinguishes a healthy service from the wedge (TCP connect
# succeeds either way; only a real service answers the follow-up read).
# Used as the dependency-free fallback for services whose real client binary
# (redis-cli, curl) is not on PATH.
#
# shellcheck disable=SC2034 # `reply`'s success/failure is the signal we
# want; its content never matters (we do not speak any of these protocols
# past the first byte), so it is intentionally read and not otherwise used.
tcp_probe() {
  local host="$1" port="$2" payload="${3:-}" reply="" rc=0
  { exec 9<>"/dev/tcp/${host}/${port}"; } 2>/dev/null || return 1
  if [ -n "$payload" ]; then
    if ! printf '%b' "$payload" >&9 2>/dev/null; then
      { exec 9<&- 9>&-; } 2>/dev/null
      return 1
    fi
  fi
  IFS= read -r -t 3 -u 9 -n 1 reply 2>/dev/null || rc=1
  { exec 9<&- 9>&-; } 2>/dev/null
  return "$rc"
}

probe_redis() {
  if command -v redis-cli >/dev/null 2>&1; then
    [ "$(redis-cli -h 127.0.0.1 -p 6379 -t 3 ping 2>/dev/null)" = "PONG" ]
    return
  fi
  # RESP inline command; a healthy server replies "+PONG\r\n".
  tcp_probe 127.0.0.1 6379 'PING\r\n'
}

probe_postgres() {
  # The 8-byte SSLRequest packet (length=8, code=80877103): the standard
  # zero-auth Postgres liveness ping -- any real server answers with a
  # single 'S' or 'N' byte immediately, whether or not SSL is enabled.
  tcp_probe 127.0.0.1 5432 '\x00\x00\x00\x08\x04\xd2\x16\x2f'
}

probe_mysql() {
  # MySQL sends its handshake-initiation packet unprompted right after
  # accept(); no payload needs to be written first.
  tcp_probe 127.0.0.1 3306
}

probe_rabbitmq() {
  if curl -fsS --connect-timeout 3 --max-time 5 \
       -u "celers:celers_password" http://127.0.0.1:15672/api/overview >/dev/null 2>&1; then
    return 0
  fi
  # Fallback: the raw AMQP 0-9-1 protocol header -- "AMQP" 0 0 9 1. A live
  # broker answers with a connection.start method frame (or, for a version
  # mismatch, its own protocol header); either way, a byte comes back.
  tcp_probe 127.0.0.1 5672 '\x41\x4d\x51\x50\x00\x00\x09\x01'
}

probe_localstack() {
  curl -fsS --connect-timeout 3 --max-time 5 http://127.0.0.1:4566/_localstack/health >/dev/null 2>&1
}

# wait_for_health PROBE_FN MAX_ATTEMPTS INTERVAL_SECONDS
wait_for_health() {
  local probe="$1" max="$2" interval="$3" attempt=1
  while [ "$attempt" -le "$max" ]; do
    if "$probe"; then
      return 0
    fi
    sleep "$interval"
    attempt=$((attempt + 1))
  done
  return 1
}

# ---------------------------------------------------------------------------
# Bring-up: probe first, start only if needed, retry once via restart if it
# looks wedged, track whether *this run* started it.
# ---------------------------------------------------------------------------

LAST_ENSURE_STARTED_IT=0

# ensure_service DISPLAY_NAME COMPOSE_SERVICE PROFILE PROBE_FN
ensure_service() {
  local display="$1" svc="$2" profile="$3" probe="$4"
  LAST_ENSURE_STARTED_IT=0

  if "$probe"; then
    log "  ${display}: already responding on the host-forwarded port -- leaving it alone"
    return 0
  fi

  log "  ${display}: not responding yet -- starting via docker compose"
  compose_up "$svc" "$profile"
  LAST_ENSURE_STARTED_IT=1

  if wait_for_health "$probe" 30 2; then
    log "  ${display}: healthy"
    return 0
  fi

  warn "${display}: did not respond within ~60s -- matches the documented Docker Desktop port-forward wedge (nc -z would still report the port open); restarting the container once and retrying"
  compose_restart "$svc" "$profile"
  if wait_for_health "$probe" 30 2; then
    log "  ${display}: healthy after restart"
    return 0
  fi

  err "${display}: still not responding after a restart"
  return 1
}

# ---------------------------------------------------------------------------
# Suite execution + PASS/FAIL/SKIP tracking
# ---------------------------------------------------------------------------

RESULT_NAMES=()
RESULT_STATUSES=()
OVERALL_FAILED=0

record() {
  RESULT_NAMES+=("$1")
  RESULT_STATUSES+=("$2")
}

# run_suite LABEL CMD...
# Streams CMD's output live (so a hang is visible as a hang, not silence)
# while also capturing it to grep for a SKIP marker afterwards.
#
# This script's real defence against a suite silently skipping is upstream
# of here: it never runs a suite whose service did not pass its protocol
# probe (see record_unavailable below), so by the time run_suite is called
# every gated test it invokes already has a real URL and will not hit its
# own early-return path. The SKIP grep below is therefore a no-op for every
# nextest-driven suite in this script -- nextest discards a *passing*
# test's captured stdout/stderr entirely (that is what
# tests/integration/README.md's own "How to tell a real run from a skip"
# section is about), so a gated test's own "SKIPPED: ..." line would never
# reach this function's captured output even if one did fire. It exists for
# tests/python-compat/run.sh, whose own skip message goes to the *script's*
# stderr directly, unfiltered by nextest.
run_suite() {
  local label="$1"
  shift
  local out rc=0
  out="$(mktemp "${TMPDIR:-/tmp}/celers-integration.XXXXXX")"
  log ""
  log "---- ${label} ----"
  log "    $*"
  "$@" 2>&1 | tee "$out" >&2 || rc=$?
  local status="PASS"
  if [ "$rc" -ne 0 ]; then
    status="FAIL"
    OVERALL_FAILED=1
  elif grep -qiE '^SKIPPED:|is not set' "$out"; then
    status="SKIP"
  fi
  rm -f "$out"
  record "$label" "$status"
}

# record_unavailable LABEL REASON
# For a suite whose service never became healthy: fail loudly and instantly
# rather than let cargo hang or spew connection-refused noise for minutes.
record_unavailable() {
  local label="$1" reason="$2"
  warn "skipping ${label}: ${reason}"
  record "$label" "FAIL"
  OVERALL_FAILED=1
}

print_summary() {
  echo
  echo "================================================================"
  echo " CeleRS integration test summary"
  echo "================================================================"
  local i
  for i in "${!RESULT_NAMES[@]}"; do
    printf '  %-6s %s\n' "${RESULT_STATUSES[$i]}" "${RESULT_NAMES[$i]}"
  done
  echo "================================================================"
  if [ "${#RESULT_NAMES[@]}" -eq 0 ]; then
    echo "  (nothing ran)"
  fi
}

# ---------------------------------------------------------------------------
# Teardown: only ever stops what this invocation itself started.
# ---------------------------------------------------------------------------

STARTED_REDIS=0
STARTED_POSTGRES=0
STARTED_MYSQL=0
STARTED_RABBITMQ=0
STARTED_LOCALSTACK=0

teardown() {
  if [ "$KEEP_UP" -eq 1 ]; then
    log "--keep-up: leaving every service this run touched as it is"
    return 0
  fi
  if [ "$STARTED_REDIS" -eq 1 ]; then log "stopping redis (started by this run)"; compose_stop "redis" "" || true; fi
  if [ "$STARTED_POSTGRES" -eq 1 ]; then log "stopping postgres (started by this run)"; compose_stop "postgres" "" || true; fi
  if [ "$STARTED_MYSQL" -eq 1 ]; then log "stopping mysql (started by this run)"; compose_stop "mysql" "test" || true; fi
  if [ "$STARTED_RABBITMQ" -eq 1 ]; then log "stopping rabbitmq (started by this run)"; compose_stop "rabbitmq" "" || true; fi
  if [ "$STARTED_LOCALSTACK" -eq 1 ]; then log "stopping localstack (started by this run)"; compose_stop "localstack" "test" || true; fi
}
trap teardown EXIT

# ---------------------------------------------------------------------------
# Scope: which services to bring up (NEED_*) and which crates' own suites to
# run (RUN_*). These differ for `--only mysql`: celers-backend-db's mysql-leg
# triage command (tests/integration/README.md's "Per-service triage" MySQL
# block) sets both MYSQL_URL and DATABASE_URL together, so postgres has to be
# up too even though celers-broker-postgres's own suite has no business
# running under `--only mysql`.
# ---------------------------------------------------------------------------

NEED_REDIS=0; NEED_POSTGRES=0; NEED_MYSQL=0; NEED_RABBITMQ=0; NEED_LOCALSTACK=0
RUN_REDIS=0; RUN_POSTGRES=0; RUN_MYSQL=0; RUN_RABBITMQ=0; RUN_LOCALSTACK=0

if [ -z "$ONLY" ]; then
  NEED_REDIS=1; NEED_POSTGRES=1; NEED_MYSQL=1; NEED_RABBITMQ=1; NEED_LOCALSTACK=1
  RUN_REDIS=1; RUN_POSTGRES=1; RUN_MYSQL=1; RUN_RABBITMQ=1; RUN_LOCALSTACK=1
else
  case "$ONLY" in
    redis) NEED_REDIS=1; RUN_REDIS=1 ;;
    postgres) NEED_POSTGRES=1; RUN_POSTGRES=1 ;;
    mysql) NEED_MYSQL=1; NEED_POSTGRES=1; RUN_MYSQL=1 ;;
    rabbitmq) NEED_RABBITMQ=1; RUN_RABBITMQ=1 ;;
    localstack) NEED_LOCALSTACK=1; RUN_LOCALSTACK=1 ;;
  esac
fi

log "CeleRS integration test runner"
log "repo root: ${REPO_ROOT}"
if [ -n "$ONLY" ]; then
  log "scope: --only ${ONLY}"
else
  log "scope: everything"
fi

# ---------------------------------------------------------------------------
# --down-v: reset exactly the volume(s) this run is about to use, before
# bringing anything up. Redis is special-cased: if something is already
# answering on 6379 without a compose-managed redis container running, it is
# a native install (or something else entirely) that this script must not
# touch.
# ---------------------------------------------------------------------------

if [ "$DOWN_V" -eq 1 ]; then
  log ""
  log "==> --down-v: resetting volumes for this run's services"
  # Every `reset_service_volume` call below keeps a trailing `|| true`: under
  # `set -e`, a function is only exempt from aborting the script on its own
  # *internal* failures when it is not the final command of its enclosing
  # list (an `if`'s condition is unconditionally exempt; `||`'s left-hand
  # side is exempt because `true` is final; a bare statement or `&&`'s
  # right-hand side is not) -- verified empirically, since this is one of
  # bash's least intuitive corners. `reset_service_volume` already reports
  # its own failures via `warn`/`log`, so there is nothing more to do with
  # the exit code here; the point is purely to keep it from ever being the
  # final command of the list that calls it.
  if [ "$NEED_REDIS" -eq 1 ]; then
    if compose_service_running "redis" ""; then
      reset_service_volume "redis" "" || true
    else
      log "  redis: no compose-managed container running (a native Redis is likely answering) -- nothing to reset"
    fi
  fi
  [ "$NEED_POSTGRES" -eq 1 ] && { reset_service_volume "postgres" "" || true; }
  [ "$NEED_MYSQL" -eq 1 ] && { reset_service_volume "mysql" "test" || true; }
  [ "$NEED_RABBITMQ" -eq 1 ] && { reset_service_volume "rabbitmq" "" || true; }
  [ "$NEED_LOCALSTACK" -eq 1 ] && { reset_service_volume "localstack" "test" || true; }
fi

# ---------------------------------------------------------------------------
# Bring services up and export the CELERS_TEST_* variables for whichever of
# them become healthy.
# ---------------------------------------------------------------------------

REDIS_READY=0; POSTGRES_READY=0; MYSQL_READY=0; RABBITMQ_READY=0; LOCALSTACK_READY=0

log ""
log "==> bringing up services"

if [ "$NEED_REDIS" -eq 1 ]; then
  if ensure_service "redis" "redis" "" probe_redis; then
    REDIS_READY=1
    export CELERS_TEST_REDIS_URL="$REDIS_URL"
  fi
  STARTED_REDIS=$LAST_ENSURE_STARTED_IT
fi

if [ "$NEED_POSTGRES" -eq 1 ]; then
  if ensure_service "postgres" "postgres" "" probe_postgres; then
    POSTGRES_READY=1
    export CELERS_TEST_POSTGRES_URL="$POSTGRES_URL"
    export DATABASE_URL="$POSTGRES_URL"
  fi
  STARTED_POSTGRES=$LAST_ENSURE_STARTED_IT
fi

if [ "$NEED_MYSQL" -eq 1 ]; then
  if ensure_service "mysql" "mysql" "test" probe_mysql; then
    MYSQL_READY=1
    export CELERS_TEST_MYSQL_URL="$MYSQL_URL_MAIN"
    export MYSQL_URL="$MYSQL_URL_MAIN"
    # The migration-upgrade suite's disposable database. The stock mysql:8.0
    # image grants MYSQL_USER rights on MYSQL_DATABASE only -- `celers` has no
    # CREATE DATABASE privilege -- so it is provisioned here with the root
    # credentials, the same way celers_backend_db_test is further down. Only
    # exported when the creation actually succeeded: an unset variable makes
    # the suite print its SKIPPED line, which is honest, whereas exporting a
    # URL for a database that does not exist would fail the suite for a
    # provisioning reason.
    log "  ensuring celers_upgrade_test exists (disposable schema for the migration-upgrade suite)"
    if docker exec celers-mysql mysql -uroot -pcelers_root_password \
        -e "CREATE DATABASE IF NOT EXISTS celers_upgrade_test; GRANT ALL PRIVILEGES ON celers_upgrade_test.* TO 'celers'@'%'; FLUSH PRIVILEGES;"; then
      export CELERS_TEST_MYSQL_UPGRADE_URL="$MYSQL_URL_UPGRADE"
    else
      warn "could not create celers_upgrade_test; the migration-upgrade suite will skip"
    fi
  fi
  STARTED_MYSQL=$LAST_ENSURE_STARTED_IT
fi

if [ "$NEED_RABBITMQ" -eq 1 ]; then
  if ensure_service "rabbitmq" "rabbitmq" "" probe_rabbitmq; then
    RABBITMQ_READY=1
    export CELERS_TEST_AMQP_URL="$AMQP_URL"
  fi
  STARTED_RABBITMQ=$LAST_ENSURE_STARTED_IT
fi

if [ "$NEED_LOCALSTACK" -eq 1 ]; then
  if ensure_service "localstack" "localstack" "test" probe_localstack; then
    LOCALSTACK_READY=1
    export CELERS_TEST_SQS_URL="$SQS_URL"
    export AWS_ACCESS_KEY_ID="test"
    export AWS_SECRET_ACCESS_KEY="test"
    export AWS_REGION="us-east-1"
  fi
  STARTED_LOCALSTACK=$LAST_ENSURE_STARTED_IT
fi

# ---------------------------------------------------------------------------
# Run each crate's gated suite, per tests/integration/README.md's
# "Per-service triage" section.
# ---------------------------------------------------------------------------

log ""
log "==> running gated suites"

if [ "$RUN_REDIS" -eq 1 ]; then
  if [ "$REDIS_READY" -eq 1 ]; then
    run_suite "redis (broker-redis, backend-redis, worker, cli)" \
      cargo nextest run -p celers-broker-redis -p celers-backend-redis -p celers-worker -p celers-cli \
      --all-features --run-ignored all
  else
    record_unavailable "redis (broker-redis, backend-redis, worker, cli)" "redis never became healthy"
  fi
fi

if [ "$RUN_POSTGRES" -eq 1 ]; then
  if [ "$POSTGRES_READY" -eq 1 ]; then
    run_suite "celers-broker-postgres" \
      cargo nextest run -p celers-broker-postgres --all-features --run-ignored all

    # celers-backend-db's postgres half must run with both MySQL variables
    # unset, even on an unscoped run where the mysql leg below has them
    # exported for its own use: the crate's test binary covers both backends
    # together and its mysql-gated tests accept *either* CELERS_TEST_MYSQL_URL
    # (preferred) or the bare MYSQL_URL (fallback) -- see
    # celers-backend-db/src/lib.rs's `test_env::resolve` -- so leaving just
    # one of the two set is not enough (confirmed live: unsetting only
    # MYSQL_URL still left CELERS_TEST_MYSQL_URL exported, and this
    # invocation ran its mysql-specific tests anyway). An inherited value
    # for either would make this invocation *also* migrate and exercise
    # those tests against whatever database it names -- by default the very
    # same `celers_test` celers-broker-sql owns. That is no longer a schema
    # collision (TODO.md Known gaps #16 is fixed: the broker's result table
    # is `celers_broker_results` now, and the shared-database topology is
    # verified 151/151), but it is still wrong for this step: the postgres
    # half must exercise postgres, and silently also migrating and
    # round-tripping through a MySQL server makes a postgres failure and a
    # mysql failure indistinguishable in this suite's result line.
    run_backend_db_postgres_half() (
      unset MYSQL_URL CELERS_TEST_MYSQL_URL
      cargo nextest run -p celers-backend-db --all-features --run-ignored all
    )
    run_suite "celers-backend-db (postgres half)" run_backend_db_postgres_half
  else
    record_unavailable "celers-broker-postgres" "postgres never became healthy"
    record_unavailable "celers-backend-db (postgres half)" "postgres never became healthy"
  fi
fi

if [ "$RUN_MYSQL" -eq 1 ]; then
  if [ "$MYSQL_READY" -eq 1 ]; then
    run_suite "celers-broker-sql (parallel)" \
      cargo nextest run -p celers-broker-sql --all-features --run-ignored all
    run_suite "celers-broker-sql (serial, --test-threads=1)" \
      cargo nextest run -p celers-broker-sql --all-features --run-ignored all --test-threads 1

    if [ "$POSTGRES_READY" -eq 1 ]; then
      # celers-backend-db's mysql leg gets its own database. Since Known
      # gaps #16 was fixed it *can* share celers-broker-sql's (verified
      # 151/151 against exactly that), but the two suites churn each other's
      # rows while running in parallel, so keeping them apart is what makes a
      # failure here mean something. The same test binary also covers the
      # postgres half, hence DATABASE_URL alongside it.
      log "  ensuring celers_backend_db_test exists (separate database from celers-broker-sql's)"
      docker exec celers-mysql mysql -uroot -pcelers_root_password \
        -e "CREATE DATABASE IF NOT EXISTS celers_backend_db_test; GRANT ALL PRIVILEGES ON celers_backend_db_test.* TO 'celers'@'%'; FLUSH PRIVILEGES;"

      run_backend_db_mysql_half() (
        # shellcheck disable=SC2030,SC2031 # Deliberately local to this
        # subshell -- and independent of run_celers_facade's own identical-
        # looking override further down: each resets MYSQL_URL fresh from
        # $MYSQL_URL_BACKEND_DB, neither reads the other's value, so there is
        # nothing for either to "lose".
        export MYSQL_URL="$MYSQL_URL_BACKEND_DB"
        cargo nextest run -p celers-backend-db --all-features --run-ignored all
      )
      run_suite "celers-backend-db (mysql half, separate db)" run_backend_db_mysql_half
    else
      record_unavailable "celers-backend-db (mysql half, separate db)" "needs postgres too (DATABASE_URL) and postgres never became healthy"
    fi
  else
    record_unavailable "celers-broker-sql (parallel)" "mysql never became healthy"
    record_unavailable "celers-broker-sql (serial, --test-threads=1)" "mysql never became healthy"
    record_unavailable "celers-backend-db (mysql half, separate db)" "mysql never became healthy"
  fi
fi

if [ "$RUN_RABBITMQ" -eq 1 ]; then
  if [ "$RABBITMQ_READY" -eq 1 ]; then
    run_suite "celers-broker-amqp" \
      cargo nextest run -p celers-broker-amqp --all-features --run-ignored all
  else
    record_unavailable "celers-broker-amqp" "rabbitmq never became healthy"
  fi
fi

if [ "$RUN_LOCALSTACK" -eq 1 ]; then
  if [ "$LOCALSTACK_READY" -eq 1 ]; then
    run_suite "celers-broker-sqs (localstack)" \
      cargo test -p celers-broker-sqs --all-features --test localstack -- --ignored --test-threads=1
  else
    record_unavailable "celers-broker-sqs (localstack)" "localstack never became healthy"
  fi
fi

# The facade needs every service at once (see tests/integration/README.md's
# table row for `celers`), so it only makes sense for the unscoped, full run.
if [ -z "$ONLY" ]; then
  if [ "$REDIS_READY" -eq 1 ] && [ "$POSTGRES_READY" -eq 1 ] && [ "$MYSQL_READY" -eq 1 ] \
     && [ "$RABBITMQ_READY" -eq 1 ] && [ "$LOCALSTACK_READY" -eq 1 ]; then
    # `celers`'s own `backend_db_integration::test_mysql_backend_integration`
    # reads the bare `MYSQL_URL` and migrates `celers_task_results` into
    # whatever database it names -- its own doc comment already warns to
    # "point it at a database of its own, not at the one
    # CELERS_TEST_MYSQL_URL names", i.e. exactly the global `MYSQL_URL` this
    # script exports in the RUN_MYSQL block above (celers-broker-sql's own
    # database). Overriding it here to the same celers_backend_db_test
    # database `run_backend_db_mysql_half` already uses (created in that same
    # block, which always runs first whenever this facade step is reachable)
    # keeps this facade run from churning celers-broker-sql's rows --
    # `test_mysql_broker_integration`'s own `CELERS_TEST_MYSQL_URL` is left
    # untouched and still correctly points at celers-broker-sql's database,
    # since it exercises that same broker code path.
    run_celers_facade() (
      # shellcheck disable=SC2030,SC2031 # See run_backend_db_mysql_half's
      # identical override above -- the two subshells are independent, both
      # resetting MYSQL_URL fresh from $MYSQL_URL_BACKEND_DB.
      export MYSQL_URL="$MYSQL_URL_BACKEND_DB"
      cargo nextest run -p celers --all-features --run-ignored all
    )
    run_suite "celers (facade, all backends)" run_celers_facade
  else
    record_unavailable "celers (facade, all backends)" "not every service became healthy"
  fi
fi

# ---------------------------------------------------------------------------
# --full: python-compat interop + a docker build smoke test
# ---------------------------------------------------------------------------

if [ "$FULL" -eq 1 ]; then
  log ""
  log "==> --full: python-compat interop and a docker build smoke test"
  run_suite "python-compat interop" bash "${REPO_ROOT}/tests/python-compat/run.sh"
  run_suite "docker build smoke" docker build -f "${REPO_ROOT}/Dockerfile" -t celers-integration-smoke:local "${REPO_ROOT}"
fi

print_summary

if [ "$OVERALL_FAILED" -eq 1 ]; then
  exit 1
fi
exit 0
