from spilttree import SplitTree
from abc import ABC

class Stage(ABC):
    # TODO: check valid priority
    def __init__(self, name, priority):
        self.name = name
        self.priority = priority
        
    # pipe: SplitTree -> [SplitTree]
    @abstractmethod
    def pipe(self):
        pass

def stageSeq(inputs, stages):
    trees = inputs[:]
    for s in stages:
        ntrees = []
        for t in trees:
            ntrees += s.pipe(t)
        trees = ntrees
    return ntrees

# prod of all inputs and stages. iterratively
def stageProd(inputs, stages):
    trees = inputs[:]
    res = inputs[:]

    while True:
        ntrees = []
        for t in trees:
            for s in stages:
                ts = s.pipe(t)
                ntrees += ts
        if len(ntrees) == 0:
            break
        trees = ntrees 
        res += ntrees

    return res

