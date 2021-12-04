from stage import IdentityStage, RequiresLoaded 
from ir import *

class SimpleDBStatLoader(IdentityStage, RequiresLoaded):
    desc = "Load Stat from Python Database"
    def loadForNode(self, name, node:'DAGNode'):
        pass
    def loadForDAG(self, dag:'DAG'):
        name = dag.name
        for n in dag.nodes:
            self.loadForNode(name,n)
    def pipe(self, dag):
        self.loadForDAG(dag)
        return super().pipe(dag)