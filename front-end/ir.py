from itertools import chain
import json
from element import ElementStats

# merge and split object
class DAGNode:
    def __init__(self, meta):
        self.meta = meta
    def 

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
        


