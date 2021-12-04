from jinja2 import Template
from element import Element
from ir import DAG, DAGNode
from stage import IdentityStage, RequiresLoaded
from codeoperator import LoadedPythonCode

from modules.policy.merger import Merger
from modules.policy.splitter import Splitter

TEMPLATE_FILE = "resource/templates/wrapper.py"

def renderCode(launchGroups, transports):
    template = Template(filename = TEMPLATE_FILE)
    numMains = sum([len(g) for g in launchGroups])
    lines = template.render(
        numMains = numMains,
        launchGroups = launchGroups,
        transports = transports)
    # return a codeOp
    return LoadedPythonCode(lines.splitlines(True))

# optimize a DAG
def corunningGroupPartition(dag: DAG):
    groups = []
    for node in dag.nodes:
        processed = False
        for g in groups:
            if node in g:
                processed = True
                break
        if processed:
            continue
        g = dag.nextCorunningGroup(node)
        groups.append(g)
    print('partition to groups: ', groups)
    return groups

def nodesToMergerInput(nodes):
    pass

def merginSuggesstionsToNodeList(nodes, suggestion):
    pass

def renameNodes(dag, nodes):
    # we can do random
    pass
def renameMainAndTransports(nodes, name):
    # return main, transports
    pass

def nodeToOptimizerElement(node):
    element =  Element()
    return element

def optimizeMerge(dag: DAG):
    e1 = Element(types=['c','m'],
        coreUsageMetric=[1, 0.2], memUsageMetric= [52, 245],
        communicationMetric=[[0, 120], [120,0]])
    e2 = Element(types=['c','m'],
        coreUsageMetric=[0.8, 0.5], memUsageMetric= [158, 652],
        communicationMetric=[[0, 840], [840,0]])
    e3 = Element(types=['c','m'],
        coreUsageMetric=[1, 1], memUsageMetric= [26, 39],
        communicationMetric=[[0, 30], [30,0]])

    merger = Merger()

    # after merging
    for g in corunningGroupPartition(dag):
        elements = list(map(nodeToOptimizerElement, g))

        mergingDecisions = merger.SuggestMerge(mergeCandidates=nodes,
                                               maxServerPoolAllowanceForMerging={'cores': 0, 'mem':0},
                                               serverSize={'cores': 40, 'mem':10240},
                                               verbose=False)

        nodes = merginSuggesstionsToNodeList(g, ret)

        if len(nodes) == 0:
            continue

        # do code gen
        mains = []
        transports = []
        name = renameNodes(dag, nodes)
        for n in nodes:
            main, trans = renameMainAndTransports(nodes, name)
            mains.append(main)
            transports.append(trans)

        launchGroups = DAG('mergedNode', nodes).getLaunchGroups()
        codeOp = renderCode(launchGroups, transports)

        # generate meta data
        meta = {}
        # TODO: validate meta
        newNode = DAGNode(meta, codeOp)
        # TODO: validate nodes
        dag.mergeFrom(nodes, newNode)

# wrap as a stage
class MergeSplitOptimizer(IdentityStage, RequiresLoaded):
    desc = "Merge and Split using the default memory pool policy"
    def pipe(self, dag: DAG):
        ret = super().pipe(dag)
        optimize(dag)
        return ret
