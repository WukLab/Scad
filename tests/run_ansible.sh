#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ANSIBLE_DIR="${ANSIBLE_DIR:-${SCRIPT_DIR}/../openwhisk/ansible}"

LOCAL_ENV="environments/local"
DIST_ENV="environments/distributed"

ENV="${LOCAL_ENV}"
set -x

ARGS="${@:1}"

if [[ "$1" == "d" ]]; then
    ENV="${DIST_ENV}"
    ARGS="${@:2}"
fi

pushd "${ANSIBLE_DIR}"
ansible-playbook -i "${ENV}" ${ARGS}
popd
