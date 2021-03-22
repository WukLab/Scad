#!/usr/bin/env bash
# set -x
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

FAAS_HOME="$(readlink -f "${SCRIPT_DIR}/../faas-profiler")"

EXEC="${1}"
ARGS="${@:2}"

"${FAAS_HOME}/${EXEC}" ${ARGS}
