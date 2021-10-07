from abc import ABC
from importlib import import_module

import logging

from pathlib import Path


def loadModule(prefix, name, args = None):
    mod = import_module(f'.{name.lower()}', prefix)
    constructor = getattr(mod, name)
    # if no args is provided, return the constructor it self
    if args is None:
        return constructor
    else:
        return constructor(*args)

# stage function: SplitTree -> SplitTree
stages = ['preprocess', 'split', 'optimize']

# load moduels for a single cata
def loadModules(predix, name, args):
    pass
    

def loadRootFromSource(sourceFile, name = None):
    if name is None:
        name = Path(sourceFile).stem
    # TODO: dispatch Code object
    return SplitTree.root(name, PythonCode())

def codeDump(outputPath, physicals):
    for p in physicals:
        ext = p.code.ext
        filename = os.path.join(outputPath, f'{p.name}.o.{ext}')
        logging.info(f'Writing object {p} to file {filename}')
        with open(filename, w) as f:
            f.write(str(p.code))

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description='Launch Frontend Compiler')
    parser.add_argument('--m-pre', type=str, nargs='+',
                        help='select modules for preprocess')
    parser.add_argument('--m-id', type=str, nargs='+',
                        help='select modules for logical identification')
    parser.add_argument('--m-opt', type=str, nargs='+',
                        help='select modules for dag optimization')

    parser.add_argument('-i', '--input', type=str, required=True,
                        help='run from initial idenfication')
    # optional read from json
    group = parser.add_argument_group('optimization')
    group.add_argument('-o', '--optimize', type=str, nargs='?',
                        help='start from optimization split file')
    group.add_argument('-s', '--stats', type=str, nargs='?',
                        help='start from optimization file')
    parser.add_argument('objectfile', type=str, required=True,
                        help='object file')

    # other optional
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level = logging.INFO)

    # load modules
    modules = loadModules()

    # if not optimization process
    trees = None
    if not args.optimize:
        trees = [loadRootFromSource(args.input)]
        # pre process
        trees = stageSeq(trees, modules['pre'])
        # id process
        trees = stageProd(trees, modules['id'])
    else
        trees = resume(args.inptu)

    if trees is None:
        raise RuntimeError('Cannot Load Source')

    # optimize
    trees = stageSeq(trees, modules['opt'])

    # allow multiple output
    # TODO: move IO to dump
    for t in trees:
        t.generate()
        t.checkpoint()
        

