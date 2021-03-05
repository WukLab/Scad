#!/usr/bin/env bash


FAAS_HOME="$(find "${HOME}" -name faas-profiler -type d | head -n 1 )"
INVOKER="./WorkloadInvoker"
ANALYZER="./WorkloadAnalyzer"

EXEC="${1}"
ARGS="${@:2}"


pushd "${FAAS_HOME}"
"./${EXEC}" ${ARGS}
popd
