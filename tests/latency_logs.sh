#!/usr/bin/env bash


controller="$(docker logs controller0)"
racksched="$(docker logs racksched0)"
invoker="$(docker logs invoker0)"

all_logs="${controller}${racksched}${invoker}"
tids="$(echo "${controller}" | grep "||latency" | grep "#tid_" | awk '{print $4}' | sort | uniq)"
echo "please pick a transaction"
echo "${tids}" | less
read tid
echo "${all_logs}" | grep "${tid}" | grep "||" | sort | less -S
