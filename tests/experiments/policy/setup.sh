#!/usr/bin/env bash


put_action() {
  ../../../wskish/wskish put \
    --file "../../../runtime/test/src/policy_test/${1}.json" \
    --app "${2}"
}

main() {
  put_action "merge1" "m1"
  put_action "merge3" "m3"
  put_action "merge4" "m4"
  put_action "split3" "s3"
  put_action "split4" "s4"
}

main $@
