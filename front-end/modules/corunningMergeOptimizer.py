from jinja2 import Template

from modules.policy.merger import Merger
from modules.policy.splitter import Splitter
from stage import IdentityStage, RequiresLoaded

def renderCode(inputs):
    template = Template(filename = "resource/templates/wrapper.py")
    template.render(
        mains = [],
        transports = []
    )

# optimize a DAG
def optimize(dag):
    root = dag.getStart()
    q = [root]
    while True:
        pass

# wrap as a stage
class MergeSplitOptimizer(IdentityStage, RequiresLoaded):
    desc = "Merge and Split using the default memory pool policy"
    pass
