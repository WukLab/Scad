from elements import *

class Optimizer:
    def __init__(self, ir, stat = None):
        self.ir = ir
        self.stat = stat

    @abstractmethod
    def optimize(self):
        pass

class IdOptimizer(Optimizer):
    def optimize(self, logicals):
        def assignResource(logical):
            resource = logical.stat
            return PhyiscalElement([logical], resource)
        phyiscals = map(logicals, assignResource)
        return (True, physicals)

