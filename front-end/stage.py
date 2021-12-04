from ir import *
from abc import *
import logging

class Guard:
    def __init__(self):
        self.res = []
    def do(self, cond, msg) -> 'Guard':
        if cond:
            self.res.append(msg)
        return self
    def get(self):
        res = ' '.join(self.res)
        if len(res) == 0:
            return None
        return res

class Stage(ABC):
    @property
    @abstractmethod
    def desc(self):
        pass

    def __init__(self, env):
        self.env = env
        
    # pipe: SplitTree -> [SplitTree]
    def pipe(self, dag: DAG):
        msg = self.validate(dag)
        if msg is not None:
            raise RuntimeError('Stage pre-request check error: ' + msg)
        pass

    def validate(self, dag: DAG):
        return None

# traits
class RequiresLoaded(Stage):
    def validate(self, dag: DAG):
        if dag is None:
            return "Try to Operate on None DAG."
        elif isinstance(dag, DAGUnloaded):
            return "Try to Operate on unloaded DAG."
        return None

class IdentityStage(Stage):
    def pipe(self, dag: DAG):
        super().pipe(dag)
        return [dag]

class StageExecutor:
    def __init__(self, dags = []):
        self.dags = dags

    def stageSeq(self, stages):
        dags = self.dags[:]
        for s in stages:
            print('Running Stage', s, 'on', dags)
            newDags = []
            for t in dags:
                newDags = newDags + s.pipe(t)
            dags = newDags
        self.dags = dags
        return self

    # prod of all inputs and stages. iterratively
    def stageProd(self, stages):
        trees = self.dags[:]
        res = self.dags[:]

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

        self.dags = res
        return self

