from abc import ABC
from inliner import inline

import logging

def loadModule(prefix, name, args = None):
    mod = import_module(f'.{name.lower()}', prefix)
    constructor = getattr(mod, name)
    if args is None:
        return constructor
    else:
        return constructor(*args)


PREPROCESSORS = [inline]
IDENTIFIERS = {
    "initial": [],
    "load": []
}
MAX_IDENFICATION_LOOP = 4
generators = {
    'compute': {
        'production': [ MetaCodeGen(), MainCodeGen(), LoadStoreCodeGen() ],
        'profile': [ MetaCodeGen(), MainCodeGen(), ProfileCodeGen() ]
    },
    'memory': {
        'default': [ MetaCodeGen() ],
    }
}

optimizer = None

def preprocess(source):
    s = source
    for p in preprocessors:
        s = p(s)
    return s

def mergeLogical(source):
    return source

def identify(source, matchEngines, logicals = []):
    for engine in matchEngines:
        l = engine.match(source)
        logicals.identify(l)
    return logicals

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
    parser.add_argument('-s', '--source-code', type=str, required=True,
                        help='launch full flow on source code')
    # module selection
    parser.add_argument('--mpreprocess', type=str, nargs='+',
                        help='select modules for preprocess')
    parser.add_argument('--mcodegen', type=str, nargs='+',
                        help='select modules for codegen')
    parser.add_argument('--midentification', type=str, nargs='+',
                        help='select modules for logical identification')
    parser.add_argument('--moptimization', type=str, nargs='+',
                        help='select modules for dag optimization')

    parser.add_argument('-i', '--initial', type=bool, action='store_true',
                        help='run from initial idenfication')
    parser.add_argument('objectfile', type=str,
                        help='object file')

    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level = logging.INFO)
    
    processed = preprocess(source)

    # identification loop
    logicals = []
    identifiers = IDENTIFIERS['initial'] if args.inital else IDENTIFIERS['initial'] 
    loop_count = 0
    while True:
        
        logicals = identify(source, identifiers)

        success, physicals = optimizer.optimize(logicals)
        if success:
            # dispatch codegen
            for physical in physicals:
                generators = [generatorMap[r] for r in generatorMap if re.match(r, physical.metas)]
                codeGen(physical, generators)
            # write the output files
            codeDump(args.output, physicals)
            break

        loop_count += 1
        if loop_count > MAX_IDENFICATION_LOOP:
            exit(1)

