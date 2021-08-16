from abc import ABC

class ElementStats:
    def __init__(self):
        self.executionTime = None
        self.memory = None
        self.cpu = None
        self.storage = None
        self.profiling = None
        
class ElementContext:
    def __init__(self):
        self.loc = loc
        self.range = sourceRange
        self.parents = []
        self.dependents = []

class Element(ABC):
    def __init__(self):
        self.data = {}


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

    def generate(self, code):
        self.code = code
    
