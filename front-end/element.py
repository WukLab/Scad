from abc import ABC

class ElementStats:
    def __init__(self, executionTime = 0,
            memory = 0, cpu = 0, storage = 0,
            netRx = 0, netTx = 0):
        self.executionTime = executionTime
        self.memory = memory
        self.cpu = cpu
        self.storage = storage
        self.netRx = netRx
        self.netTx = netTx

    def toJSON(self):
        return json.dumps(self.__dict__)

    @staticmethod
    def empty(self):
        return ElementStats()

    def __iadd__(self, other):
        self.executionTime += other.executionTime
        self.memory += other.memory
        self.cpu += other.cpu
        self.storage += other.storage
        
class ElementContext:
    def __init__(self):
        self.parents = []
        self.dependents = []

# elements. all elements must be associated with code object
class Element(ABC):
    def __init__(self, name, code):
        self.name = name
        self.code = code


class LogicalElement(Element):
    def __init__(self, context, stat, sourceRange, loc):
        self.context = context
        self.stat = stat

class PhysicalElement(Element):
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
    
