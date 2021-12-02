#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export ANSIBLE_DIR="${SCRIPT_DIR}"/../openwhisk-base/ansible

${SCRIPT_DIR}/run_ansible.sh ${@}
