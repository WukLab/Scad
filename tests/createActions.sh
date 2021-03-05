#!/usr/bin/env bash

export WSK="wsk -i"

${WSK} action create action1 action.js
${WSK} action create action2 action.js
${WSK} action create mySequence --sequence /whisk.system/action1,/whisk.system/action2
