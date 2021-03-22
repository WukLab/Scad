#!/usr/bin/env python3

import os
import sys

import argparse
import subprocess
import shlex
import re

import numpy as np
import matplotlib.pyplot as plt
import pandas


CONTROLLER_NAME = 'controller0'
RACKSCHED_NAME = 'racksched0'
INVOKER_NAME = 'invoker0'
LOG_TYPES = [CONTROLLER_NAME, RACKSCHED_NAME, INVOKER_NAME]

FN_TO_RACKSCHED = 'posting to racksched'
FN_AT_RACKSCHED = 'about to schedule: Some'
FN_TO_INVOKER = 'scheduled: Some('
FN_AT_INVOKER = 'handling activation message'
FN_FINISH_INVOCATION = 'storing activation response'
FN_STORE_INVOCATION = 'stored final activation response'

FILTER_PRETTY = {
    FN_TO_RACKSCHED: 'CDF of latency (ms) for top-level scheduling',
    FN_AT_RACKSCHED: 'CDF of latency (ms) for top-level --> rack comms (Kafka)',
    FN_TO_INVOKER: 'CDF of latency (ms) for rack scheduler',
    FN_AT_INVOKER: 'CDF of latency (ms) for rack --> invoker comms (Kafka)',
    FN_FINISH_INVOCATION: 'CDF of latency (ms) for container invocation'
}

FILTERS = {
    FN_TO_RACKSCHED: CONTROLLER_NAME,
    FN_AT_RACKSCHED: RACKSCHED_NAME,
    FN_TO_INVOKER: RACKSCHED_NAME,
    FN_AT_INVOKER: INVOKER_NAME,
    FN_FINISH_INVOCATION: INVOKER_NAME
    # FN_STORE_INVOCATION: INVOKER_NAME
}

filterset = set(FILTERS)

LAT_STR = '||latency'

HEALTH_ACTION = 'invokerHealthTestActionracksched0'

LAT_REGEX = r'\|\|latency: (\d*)$'
TID_REGEX = r'\[(#tid_[A-z0-9].*?)\]'

def get_logs(container_name):
    return subprocess.check_output(shlex.split("docker logs {}".format(container_name))).decode('utf-8')

def reject_outliers(data, m=1):
    return data[abs(data - np.median(data)) < m * np.std(data)]

def get_all_logs():
    logs = {}
    for container in LOG_TYPES:
        lg =  get_logs(container).split('\n')
        filtered = filter(lambda x: LAT_STR in x and HEALTH_ACTION not in x, lg)
        logs[container] = list(filtered)
    return logs

def do_individual_latencies():
    logs = get_all_logs()
    def regexmatch(x):
        match = re.findall(TID_REGEX, x)
        res = match
        if len(res) < 2:
            # print(x)
            return None
        return res[1]

    ctrl_logs = filter(lambda x: FN_TO_RACKSCHED in x and HEALTH_ACTION not in x, logs[CONTROLLER_NAME])
    tids = list(map(lambda x: regexmatch(x), ctrl_logs))
    # print(tids)
    df = pandas.DataFrame({'tid': tids})
    tid_logs = {}
    for transaction in pandas.unique(df['tid']):
        tid_logs[transaction] = []
        for container in LOG_TYPES:
            [tid_logs[transaction].append(x) for x in filter(lambda y: transaction in y, logs[container])]

    filt_diffs = {}
    for filt in FILTERS:
        filt_diffs[filt] = []

    for transid in tid_logs:
        log = tid_logs[transid]
        filttime = {}
        for filt in FILTERS:
            added = False
            for line in log:
                if filt not in line:
                    continue
                filttime[filt] = get_latency_from_line(filt, line)
                added = True
            if not added:
                print("EIROejnksdlfkjsdf didn't add for filter: {}".format(filt))
                break
        # filter names now mapped to a latency..let's calculate the differences
        filt_diffs[FN_TO_RACKSCHED].append(filttime[FN_TO_RACKSCHED])
        def add_time(curr_filt, prev_filt):
            diff = filttime[curr_filt] - filttime[prev_filt]
            if diff >= 0:
                filt_diffs[curr_filt].append(diff)

        if len(filttime) < 4:
            continue
        add_time(FN_AT_RACKSCHED, FN_TO_RACKSCHED)
        add_time(FN_TO_INVOKER, FN_AT_RACKSCHED)
        add_time(FN_AT_INVOKER, FN_TO_INVOKER)
        add_time(FN_FINISH_INVOCATION, FN_AT_INVOKER)

        # [print(x) for x in tid_logs[transid]]
        # break

    fig, axes = plt.subplots(len(filt_diffs), 1)
    # fig.tight_layout()
    fig.subplots_adjust(hspace=0.5)
    fig.set_size_inches(10.5, 16.5)
    # fig.savefig('test2png.png', dpi=100)
    filter_keys = list(FILTERS.keys())
    for i in range(len(axes)):
        dt = filter_keys[i]
        arr = np.array(filt_diffs[dt])
        if len(arr) <= 0:
            continue
        print('{} values for {} (max: {})'.format(len(arr), dt, arr.max()))
        bins = np.arange(1, arr.max())
        ax = axes[i]
        ax.hist(arr, bins=bins, cumulative=True, density=True, histtype='step')
        ax.set_title(FILTER_PRETTY[dt])
        ax.set_xticks(np.arange(0, 200, 5))
        ax.set_xbound(0, 200)
        ax.grid()
    plt.show()


def get_latency_from_line(filt, text):
    if filt not in text:
        return None
    lat = int(re.search(LAT_REGEX, text).groups()[0])
    return lat



def do_cumulative_latency():
    logs = get_all_logs()
    datas = {}
    for filt in FILTERS:
        datas[filt] = []
        for line in logs[FILTERS[filt]]:
            lat = get_latency_from_line(filt, line)
            if lat is not None:
                datas[filt].append(lat)
                # break
    fig, axes = plt.subplots(len(datas), 1)
    # fig.tight_layout()
    fig.subplots_adjust(hspace=0.5)
    fig.set_size_inches(10.5, 16.5)
    # fig.savefig('test2png.png', dpi=100)
    filter_keys = list(FILTERS.keys())
    for i in range(len(axes)):
        dt = filter_keys[i]
        arr = np.array(datas[dt])
        if len(arr) <= 0:
            continue
        print('{} values for {} (max: {})'.format(len(arr), dt, arr.max()))
        bins = np.arange(1, arr.max())
        ax = axes[i]
        ax.hist(arr, bins=bins, cumulative=True, density=True, histtype='step')
        ax.set_title(FILTER_PRETTY[dt])
        ax.set_xticks(np.arange(0, 200, 5))
        ax.set_xbound(0, 200)
        ax.grid()
    plt.show()

def main():
    do_individual_latencies()
    do_cumulative_latency()

if __name__ == "__main__":
    main()
