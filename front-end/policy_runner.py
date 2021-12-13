#!/usr/bin/env python3

import argparse
import requests

from modules.policy import splitter as sp
from modules.policy import merger
from modules.policy import element
from element import ElementStats

MB = 1024 * 1024

def pull_data(function, element):
    resp = requests.get("http://wuklab-01.ucsd.edu:8080/element/{}/{}".format(function, element))
    return resp.json()

def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("function", help="the function to poll profiling data from")
    # parser.add_argument("element", help="the element to poll profiling data from")
    args = parser.parse_args()
    apps = ['merge3', 'merge4']
    rawd = map(lambda x: ElementStats(pull_data(x, 'compute1')), apps)
    vals = map(lambda x: [x.cpu(), x.memory(), x.networkTo('mem1')], rawd)
    def toElem(x):
        return element.Element(
        types=['c', 'm'],
        coreUsageMetric=[x[0], 0],
        memUsageMetric=[0, x[1]],
        communicationMetric=[
            [x[2], 0],
            [0, x[2]]
        ]
    )
    elems = map(toElem, vals)
    [print(x) for x in elems]

    splitter = sp.Splitter()
    result = splitter.SuggestSplit(
        splitCandidates=list(elems),
        minDesiredServerPoolReduction={
            'cores': 1.0,
            'mem': 536870912,
        },
        leftCPUPoolCapacity={
            'cores': 88.0,
            'mem': 95104 * MB,
        },
        leftMemoryPoolCapacity={
            'cores': 12.0,
            'mem': 95104 * MB
        },
        verbose=True
    )
    print(result)



if __name__ == "__main__":
    main()
