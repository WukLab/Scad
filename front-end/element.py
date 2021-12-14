from abc import *
from typing import overload

class StatOperator(ABC):
    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def reduce(self, tag, value):
        pass

class StatScalarOperator(StatOperator):
    def __init__(self):
        self.value = 0
    def get(self):
        return self.value

class StatCollectOperator(StatOperator):
    def __init__(self):
        self.values = {}
    def get(self):
        return self.values
    def reduce(self, tag, value):
        self.values[tag] = value

class StatMax(StatScalarOperator):
    def reduce(self, tag, value):
        if value > self.value:
            self.value = value

class StatAvg(StatCollectOperator):
    def get(self):
        return sum([v for _, v in self.values.items()]) / len(self.values)

class ElementStats:
    def __init__(self, data: dict):
        self.data = data

    def isValid(self):
        return len(self.data) > 0

    def dataStat(self, selector, aidOp, pointOp):
        aData = aidOp()
        for aid, aidData in self.data.items():
            pData = pointOp()
            for point, pointData in aidData.items():
                pData.reduce(point, selector(pointData))
            aData.reduce(aid, pData.get())
        return aData.get()

    def memory(self):
        selector = lambda x: x['memory']
        return self.dataStat(selector, aidOp=StatMax, pointOp=StatAvg)
    def cpu(self):
        selector = lambda x: x['cpu']
        return self.dataStat(selector, aidOp=StatMax, pointOp=StatAvg)

    def networkTo(self, name):
        selector = lambda x: sum(x.get('network',{}).get(name, [0]))
        return self.dataStat(selector, aidOp=StatMax, pointOp=StatAvg)

class ElementContext:
    def __init__(self):
        self.parents = []
        self.dependents = []

# elements. all elements must be associated with code object
class LogicalElement:
    def __init__(self, context, stat, sourceRange, loc):
        self.context = context
        self.stat = stat

class PhysicalElement:
    def __init__(self, logicals,
                 resources = None, name = None, source = None):
        self.logicals = logicals
        self.resources = resources
        self.source = logicals[0].source if source is None else source

        self.stat.range = self.validate(logicals)
        self.stat.name = str(hash(self)) if name is None else name

    # TODO: this only work for linear programs
    def validate(self, logicals):
        ls = sorted(logicals, lambda l: l.range[0])
        for s, e in zip(ls, ls[1:]):
            if s.range[1] != e.range[0]:
                raise RuntimeError("Cannot merge virtual elements")
        return ls[0].range[0], ls[-1].range[1]

    def generate(self, destDir):
        meta = self.generateMeta()
        self.code.dumpWithMeta(destDir, self.name, meta)

