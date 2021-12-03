from itertools import chain
import json
from element import ElementStats
from code import *
import os
from glob import glob

def filterAdd(list, toRemove, toAdd):
    return [e for e in list if e not in toRemove] + [toAdd]
def subConcat(list, toRemove, toAdd):
    list = list - toRemove
    return list + toAdd

class DAG:
    def __init__(self, name, nodes = []):
        self.name = name
        self.nodes = nodes
    # get the start node of the DAG
    def getStarts(self):
        starts = []
        for n in self.nodes:
            if len(n.parents) == 0:
                for cn in n.corunning:
                    if len(cn.parents) != 0:
                        continue
                starts.append(n)
        return starts

    def getEnds(self):
        ends = []
        for n in self.nodes:
            if len(n.dependents) == 0 and n.hasDependentData():
                for cn in n.corunning:
                    if len(cn.parents) != 0:
                        continue
                ends.append(n)
        return ends

    def nextCorunningGroup(self, node):
        cg = [node]
        index = 0
        while True:
            for n in cg[index].corunning:
                if n not in cg:
                    cg.append(n)
            index += 1
            if index > len(cg):
                break
        # return a group containing node
        return cg

    def getRelationshipsForGroup(self, nodes):
        parents = []
        dependents = []
        for n in nodes:
            for p in n.parents:
                if p not in nodes and p not in parents:
                    parents.append(p)
            for d in n.dependents:
                if d not in nodes and d not in dependents:
                    dependents.append(p)
        return parents, dependents

    def mergeFrom(self, nodes, newNode):
        parents, dependents = self.getRelationshipsForGroup(nodes)
        newNode.parents = parents
        newNode.dependents = dependents
        for p in parents:
            p.dependets = filterAdd(p.dependents, nodes, newNode)
        for d in dependents:
            d.parents = filterAdd(d.parents, nodes, newNode)
        # TODO: corunning?
        self.nodes = [n for n in self.nodes if n not in nodes] + [newNode]

    def splitTo(self, node:'DAGNode', newNodesDAG:'DAG'):
        starts, ends = newNodesDAG.getStarts(), newNodesDAG.getEnds()
        # parents 
        for p in node.parents:
            p.parents = subConcat(p.parents, node, starts)
        # dependents
        for p in node.dependents:
            p.dependents = subConcat(p.dependents, node, ends)
        # TODO: corunning?
        self.nodes = subConcat(self.nodes, node, newNodesDAG.nodes)
    # TODO: traverse dependent groups

    def __repr__(self) -> str:
        return f"({self.name} -> {','.join(map(str, self.nodes))})"

class DAGUnloaded(DAG):
    def __init__(self, path):
        super().__init__(None)
        self.path = path

# MetaData: json serializable
# merge and split object
class DAGNode:
    def __init__(self, metaDict, codeOp):
        self.compileMeta = {}
        # relationship
        self.corunning = []
        self.parents = []
        self.dependents = []
        # content
        self.codeOp = codeOp
        # proprity
        self.resources = {}
        self.type = None
        self.name = None
        # stat
        self.stat = None

        self.loadMetaDict(metaDict)

    def __repr__(self) -> str:
        return self.name

    def loadMetaDict(self, metaDict):
        if metaDict is None:
            raise RuntimeError('Cannot init node without meta dict')
        self.name = metaDict['name']
        self.type = metaDict['type']
        self.parents = metaDict.get('parents', [])
        self.dependents = metaDict.get('dependents', [])
        self.corunning = metaDict.get('corunning', [])
        self.resources = metaDict.get('limits', {})
    def dumpMetaDict(self):
        meta = {}
        meta['name'] = self.name
        meta['type'] = self.type
        meta['parents'] = self.parents
        meta['dependents'] = self.dependents
        meta['corunning'] = self.corunning
        meta['resources'] = self.resources
        return meta

    # Can be splited!
    def generateMeta(self):
        meta = {
            "parents": [n.name for n in self.parents],
            "dependets": [n.name for n in self.dependents],
            "corunning": [n.name for n in self.corunning],
            "type": self.type,
            "compileMeta": { k:v.toJson() for k,v in self.meta.items() }
        }
        return meta

    def hasDependentData(self):
        return self.type == 'compute'

# immutable obejct
class SplitTree:
    @staticmethod
    def root(name, code):
        element = Element(name, code)
        return SplitTree(element = element)

    def __init__(self, parent = None, children = [], element = None):
        self.parent = parent
        self.root = not parent
        self.leaf = not children
        self.children = children
        self.stats = None

        # for leaf, content
        self.el = element

        # info about who create the node
        self.splitter = None
        self.splitmeta = None

        # pointers for root

    def update(self, rec = True):
        self.stats = ElementStats.empty()
        for c in self.children:
            self.stats += c.stats
        if rec and self.parent is not None:
            self.parent.update()

    def toJSON(self):
        pass

    def split(self, splitter):
        pass

    def leaves(self):
        if self.leaf:
            return [self]
        else:
            return chain.from_iterable(
                map(lambda x: x.leaves(), self.children))

    # dump files to a folder
    def generate(self, destDir, name):
        if not self.root:
            raise RuntimeError('try to dump a non-root tree')
        targetDir = os.path.join(destDir, name)
        # only generate leaf nodes
        for leaf in self.leaves():
            leaf.el.generate(targetDir)





