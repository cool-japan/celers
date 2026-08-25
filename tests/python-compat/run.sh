#!/usr/bin/env bash
#
# Set up and run the CeleRS <-> Python Celery interoperability suite.
#
#   ./run.sh              # set up the venv, build the bridge, run the suite
#   ./run.sh capture      # re-record the verbatim wire fixtures
#   ./run.sh shell        # print the environment for running pytest by hand
#
# Everything is idempotent: an existing venv is reused, and an up-to-date
# bridge binary is not rebuilt.
#
# Prerequisites the script will NOT install for you:
#   * a reachable Redis, named by CELERS_TEST_REDIS_URL
#   * a python3 that can create a venv
#   * a cargo toolchain
#
# Without a Redis the suite skips visibly rather than passing quietly.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"

# Keep the virtualenv out of the source tree: it is build output, not source.
VENV="${CELERS_COMPAT_VENV:-${TMPDIR:-/tmp}/celers-python-compat-venv}"
PYTHON_BIN="${VENV}/bin/python"

# Pinned so a fixture recorded against one Celery is never silently compared
# against another. Bump this together with a `./run.sh capture`.
CELERY_SPEC="${CELERS_COMPAT_CELERY_SPEC:-celery[redis]==5.6.3}"
PYTEST_SPEC="${CELERS_COMPAT_PYTEST_SPEC:-pytest}"

log() { printf '  %s\n' "$*" >&2; }

require_redis() {
  if [[ -z "${CELERS_TEST_REDIS_URL:-}" ]]; then
    echo "SKIPPED: tests/python-compat -- no Redis configured (set CELERS_TEST_REDIS_URL to run)" >&2
    exit 0
  fi
}

ensure_venv() {
  if [[ ! -x "${PYTHON_BIN}" ]]; then
    log "creating venv at ${VENV}"
    "${CELERS_COMPAT_PYTHON3:-python3}" -m venv "${VENV}"
    "${PYTHON_BIN}" -m pip install --quiet --upgrade pip
  fi
  # `pip install` on an already-satisfied requirement is a no-op, which is what
  # makes re-running this cheap.
  if ! "${PYTHON_BIN}" -c 'import celery, redis, pytest' >/dev/null 2>&1; then
    log "installing ${CELERY_SPEC} and ${PYTEST_SPEC}"
    "${PYTHON_BIN}" -m pip install --quiet "${CELERY_SPEC}" "${PYTEST_SPEC}"
  fi
  log "$("${PYTHON_BIN}" -c 'import celery, kombu; print(f"celery {celery.__version__}, kombu {kombu.__version__}")')"
}

ensure_bridge() {
  log "building the celers-protocol celery_bridge example"
  ( cd "${REPO_ROOT}" && CARGO_INCREMENTAL=0 cargo build --quiet -p celers-protocol --example celery_bridge )
  BRIDGE="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}/debug/examples/celery_bridge"
  if [[ ! -x "${BRIDGE}" ]]; then
    echo "error: cargo reported success but ${BRIDGE} is missing" >&2
    exit 1
  fi
}

export_env() {
  export CELERS_PYTHON="${PYTHON_BIN}"
  export CELERS_BRIDGE="${BRIDGE}"
  export PYTHONPATH="${HERE}${PYTHONPATH:+:${PYTHONPATH}}"
  # Celery reads `celeryconfig` from the working directory.
  export PYTHONDONTWRITEBYTECODE=1
}

main() {
  local command="${1:-test}"
  require_redis
  ensure_venv
  ensure_bridge
  export_env

  case "${command}" in
    test)
      cd "${HERE}"
      shift || true
      exec "${PYTHON_BIN}" -m pytest -q "$@"
      ;;
    capture)
      cd "${HERE}"
      exec "${PYTHON_BIN}" capture_fixtures.py
      ;;
    shell)
      echo "export CELERS_PYTHON=${CELERS_PYTHON}"
      echo "export CELERS_BRIDGE=${CELERS_BRIDGE}"
      echo "export PYTHONPATH=${PYTHONPATH}"
      echo "# then: cd ${HERE} && \${CELERS_PYTHON} -m pytest -q"
      ;;
    *)
      echo "usage: run.sh [test|capture|shell]" >&2
      exit 2
      ;;
  esac
}

main "$@"
