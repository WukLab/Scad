#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ANSIBLE_DIR="${SCRIPT_DIR}"/../openwhisk-base/ansible

pushd "${ANSIBLE_DIR}"
ansible-playbook -i environments/local ${@}
popd
