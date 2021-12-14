#!/usr/bin/env python3

import argparse
import requests

from modules.policy import splitter as sp
from modules.policy import merger
from modules.policy import element
from element import ElementStats

MB = 1024 * 1024

URL = 'http://wuklab-01.ucsd.edu:8080'

def pull_data(function, element):
    resp = requests.get("{}/element/{}/{}".format(URL, function, element))
    return resp.json()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", help="clears the data", action='store_true', default=False)
    parser.add_argument("--print", help="prints collected elements", action='store_true', default=False)
    parser.add_argument("-f", '--functions', help="functions to constrain", default='merge3,merge4')
    # parser.add_argument("element", help="the element to poll profiling data from")
    args = parser.parse_args()
    if args.clear:
        requests.get('{}/clear'.format(URL))
        return

    apps = args.functions.split(',')
    rawd = map(lambda x: ElementStats(pull_data(x, 'compute1')), apps)
    vals = map(lambda x: [x.cpu(), x.memory(), x.networkTo('mem1')], rawd)
    def toElem(x):
        return element.Element(
        types=['c', 'm'],
        coreUsageMetric=[x[0], 0],
        memUsageMetric=[0, x[1]],
        communicationMetric=[
            [0, x[2]],
            [x[2], 0]
        ]
    )
    elems = list(map(toElem, vals))
    if args.print:
        [print(x) for x in elems]
        return


    splitter = sp.Splitter()
    result = splitter.SuggestSplit(
        splitCandidates=list(elems),
        minDesiredServerPoolReduction={
            'cores': 1.0,
            'mem': 1234,
        },
        leftCPUPoolCapacity={
            'cores': 96,
            'mem': 983040,
        },
        leftMemoryPoolCapacity={
            'cores': 96,
            'mem': 983040
        },
        verbose=False
    )
    print(result)



if __name__ == "__main__":
    main()
