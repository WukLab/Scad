from abc import ABC
from importlib import import_module
import logging
from pathlib import Path

from ir import SplitTree
from code import *
from stage import *

from flows import getFlows, initFlows

def printHelp():
    flows = getFlows()
    print('avaliable flows and modules:')
    for flow, modules in flows.items():
        for name, stage in modules.items():
            print(f":{flow}:{name} - {stage.desc}")

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description='Launch Frontend Compiler')
    parser.add_argument('--modules', type=str, nargs='+',
                        help='select modules to include')
    parser.add_argument('--flows', type=str, nargs='+',
                        help='select flows to include')
    parser.add_argument('-i', '--input', type=str, required=True,
                        help='run from initial idenfication')
    parser.add_argument('-o', '--output', type=str,
                        help='output directory')
    # other optional
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    # print help anyway
    printHelp()

    if args.verbose:
        logFormatter = logging.Formatter(fmt=' %(name)s :: %(levelname)-8s :: %(message)s')
        logging.basicConfig(level = logging.DEBUG)

    env = vars(args)
    flows = initFlows(env)
    # filter the flows
    if args.flows:
        flows = {k:flows[k] for k in flows if k in args.flows }
    # filter the operations
    if args.modules:
        def filterFlow(d):
            return { k:v for k,v in d.items if k in args.modules }
        flows = {k:filterFlow(v) for k,v in flows}

    # load the target
    dags = [DAGUnloaded(args.input)]

    # execute the stages
    exec = StageExecutor(dags=dags)
    for flow, modules in flows.items():
        stages = list(modules.values())
        if flow == 'identify':
            exec.stageProd(stages)
        else:
            exec.stageSeq(stages)
    



        

