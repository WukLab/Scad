#!/usr/bin/env bash

# Exit if any command errors
set -e

#Userful URL to view API docs:
#https://petstore.swagger.io/?url=https://raw.githubusercontent.com/openwhisk/openwhisk/master/core/controller/src/main/resources/apiv1swagger.json#/

SYSTEM_AUTH=789c46b1-71f6-4ed5-8c54-816aa4f8c502:abczO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP
GUEST_AUTH=89c46b1-71f6-4ed5-8c54-816aa4f8c502:abczO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP

# List all actions
#curl -u ${SYSTEM_AUTH} -X GET http://172.17.0.1:3233/api/v1/namespaces/whisk.system/actions

#HOST=http://172.17.0.1:3233
HOST=https://localhost

JSON_FILE="application-example.json"
ACTION_NAME=test-action
if [[ "${1}" == "d" ]]; then
    echo "DELETING"
    curl -k -u ${SYSTEM_AUTH} -X DELETE -H "Content-Type: application/json" --data @${JSON_FILE} ${HOST}/api/v1/namespaces/whisk.system/actions/test-action
elif [[ "${1}" == "p" ]]; then
    # Put an action
    echo "UPLOADING"
    curl -k -u ${SYSTEM_AUTH} -X PUT -H "Content-Type: application/json" --data @${JSON_FILE} ${HOST}/api/v1/namespaces/whisk.system/actions/test-action?overwrite=true
elif [[ "${1}" == "g" ]]; then
    # echo "Getting activation for ${2}"
    curl -k -u ${SYSTEM_AUTH} -X GET -H "Content-Type: application/json" --data @${JSON_FILE} ${HOST}/api/v1/namespaces/whisk.system/activations/${2}/result
    echo ""
    curl -k -u ${SYSTEM_AUTH} -X GET -H "Content-Type: application/json" --data @${JSON_FILE} ${HOST}/api/v1/namespaces/whisk.system/activations/${2}/logs
elif [[ "${1}" == "a" ]]; then
    curl -k -u ${SYSTEM_AUTH} -X GET -H "Content-Type: application/json" --data @${JSON_FILE} ${HOST}/api/v1/namespaces/whisk.system/activations | jq '.[] | select(.name | contains("action_tests_name"))'
else
    echo "ACTIVATING"
    curl -k -u ${SYSTEM_AUTH} -X POST ${HOST}/api/v1/namespaces/whisk.system/actions/test-action
fi
    echo "" # newline
