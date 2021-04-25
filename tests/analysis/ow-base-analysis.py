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
INVOKER_NAME = 'invoker0'
LOG_TYPES = [CONTROLLER_NAME, INVOKER_NAME]

FN_CONTROLLER_START = 'controller_loadbalancer_start'
FN_CONTROLLER_FINISH =   'controller_loadbalancer_finish'
FN_INVOKER_START =     'invoker_activation_start'
FN_INVOKER_FINISH = 'invoker_activationRun_finish'
FN_CONTROLLER_ACT_FINISH = 'controller_blockingActivation_finish'

FILTER_PRETTY = {
    FN_CONTROLLER_START: 'CDF of latency (ms) for invocation setup',
    FN_CONTROLLER_FINISH: 'CDF of latency (ms) for top-level scheduling',
    FN_INVOKER_START: 'CDF of latency (ms) for LB --> Invoker comms (Kafka)',
    FN_INVOKER_FINISH: 'CDF of latency (ms) for container execution',
    FN_CONTROLLER_ACT_FINISH: 'CDF of latency (ms) for invoker --> LB comms (kafka)'
}

FILTERS = {
    FN_CONTROLLER_START: CONTROLLER_NAME,
    FN_CONTROLLER_FINISH: CONTROLLER_NAME,
    FN_INVOKER_START: INVOKER_NAME,
    FN_INVOKER_FINISH: INVOKER_NAME,
    FN_CONTROLLER_ACT_FINISH: CONTROLLER_NAME
}

filterset = set(FILTERS)

MARKER_STR = '[marker:'

HEALTH_ACTION = 'invokerHealthTestActionracksched0'

MARKER_REGEX = r'\[marker:.*?\]'
TID_REGEX = r'\[(#tid_[A-z0-9].*?)\]'

def get_logs(container_name):
    with open('/var/tmp/wsklogs/{cntr}/{cntr}_logs.log'.format(cntr=container_name)) as f:
        return f.read()

def reject_outliers(data, m=3):
    return data[abs(data - np.median(data)) < m * np.std(data)]

def get_all_logs():
    logs = {}
    for container in LOG_TYPES:
        lg =  get_logs(container).split('\n')
        filtered = filter(lambda x: MARKER_STR in x and HEALTH_ACTION not in x, lg)
        logs[container] = list(filtered)
    return logs

def do_individual_latencies():
    '''This function attempts to calculate all of the individual latencies at
    each DAG execution step.

    Steps:

    - get all logs from each container
    - filter all logs with only those containing a log marker (existence of marker token)
    - extract a unique list of transaction IDs
    - for each txid, map it to the set of logs corresponding to the txid
    -
    '''
    logs = get_all_logs()
    def regexmatch(x):
        match = re.findall(TID_REGEX, x)
        res = match
        # if len(res) < 2:
        #     print(x)
        #     return None
        return res[0]

    ctrl_logs = filter(lambda x: FN_CONTROLLER_START in x and HEALTH_ACTION not in x, logs[CONTROLLER_NAME])
    tids = list(map(lambda x: regexmatch(x), ctrl_logs))

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
                time = get_latency_from_line(filt, line)
                filttime[filt] = time
                added = True
            if not added:
                print("################### \n[{}] didn't add for filter: {}\n{}".format(transid, filt, '\n'.join(log)))
                print("###################")
                break
        print(filttime)
        # filter names now mapped to a latency..let's calculate the differences
        filt_diffs[FN_CONTROLLER_START].append(filttime[FN_CONTROLLER_START])
        def add_time(prev_filt, curr_filt):
            try:
                diff = filttime[curr_filt] - filttime[prev_filt]
                if diff >= 0:
                    filt_diffs[curr_filt].append(diff)
            except KeyError as e:
                # don't always have all of the filters for each execution. Just skip one isn't found
                # print("keyError for [{}] [{}|{}] on {}".format(transid, curr_filt, prev_filt, filttime))
                # raise e
                pass

        add_time(FN_CONTROLLER_START, FN_CONTROLLER_FINISH)
        add_time(FN_CONTROLLER_FINISH, FN_INVOKER_START)
        add_time(FN_INVOKER_START, FN_INVOKER_FINISH)
        add_time(FN_INVOKER_FINISH, FN_CONTROLLER_ACT_FINISH)

        # [print(x) for x in tid_logs[transid]]
        # break

    fig, axes = plt.subplots(len(filt_diffs), 1)
    # fig.tight_layout()
    fig.subplots_adjust(hspace=0.7)
    fig.set_size_inches(14, 20.5)
    filter_keys = list(FILTERS.keys())
    for i in range(len(axes)):
        dt = filter_keys[i]
        arr = np.array(filt_diffs[dt])
        if len(arr) == 0:
            print('0 len for {}'.format(dt))
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
    try:
        match = re.search(MARKER_REGEX, text)
        if not match:
            raise Exception("Failed to find match on {} for marker".format(text))
        match = match.group(0).split(':')[2]
        match = match if not match.endswith(']') else match[:-1]
        lat = int(match)
    except Exception as e:
        print("err: {} ||| {}".format(filt, text))
        raise e

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
    fig.subplots_adjust(hspace=0.8)
    fig.set_size_inches(16, 24)
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
