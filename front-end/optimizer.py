from elements import *

class Optimizer:
    pass

class IdOptimizer(Optimizer):
    def optimize(self, logicals):
        def assignResource(logical):
            resource = logical.stat
            return PhyiscalElement([logical], resource)
        phyiscals = map(logicals, assignResource)
        return (True, physicals)

