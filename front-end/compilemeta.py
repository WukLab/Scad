from abc import *
import json

class CompileMeta(ABC):
    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def toJson(self):
        pass

    @abstractmethod
    def mappend(self, *other):
        pass

class CodeMergeHistory(CompileMeta):
    name = "merge_history"
    def __init__(self, node, dag):
        self.node = node
        self.dag = dag
    def toJson(self):
        res = self.node
        return json.dumps(res)

