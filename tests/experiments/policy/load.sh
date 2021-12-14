#!/usr/bin/env bash


main() {
  while [[ true ]]; do
  ../../../wskish/wskish post -n --app "${1}" --profile &
  sleep "${2}"
  done
}

main "$@"
