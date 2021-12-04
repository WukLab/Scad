# import of modules
from modules.folderDAGLoader import FolderDAGLoader, FolderDAGDumper, FolderDAGGenerator
from modules.statLoader import SimpleDBStatLoader
from modules.corunningMergeOptimizer import MergeSplitOptimizer

def getFlows():
    flowsDict = {
        'preprocess': {
            'loadFromFolder': FolderDAGLoader,
            'stat': SimpleDBStatLoader
        },
        'identification': {

        },
        'optimization': {
            'policy': MergeSplitOptimizer
        },
        'generate': {
            'dump': FolderDAGDumper,
            'generate': FolderDAGGenerator
        }
    }
    return flowsDict

def initFlows(env):
    flows = getFlows()
    return {
        flow: { name: init(env) for name,init in m.items() }
        for flow, m in flows.items()
    }