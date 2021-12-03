import yaml
from stage import *
from ir import *
from codeoperator import *
import generate

class FolderDAGLoader(Stage):
    desc = "Load DAG from folder"
    def __init__(self, env):
        super().__init__(env)
        path = env['input']
        if path.endswith('/'):
            path = path[:-1]
        self.path = path
        self.targetPath = path
        self.appName = os.path.basename(self.path)

    def validateMeta(self, meta):
        meta['type']
        meta['filetype']
    
    def loadDAG(self):
        objects = glob(os.path.join(self.path, "*.o.*"))
        nodes = [self.loadDAGNode(f) for f in objects]
        return DAG(name = self.appName, nodes = nodes)
            
    def loadDAGNode(self, filename:"str"):
        hasMeta, meta, code = generate.filemeta(filename)
        codeOp = getCodeByType(meta['type'], meta['filetype'], code)
        return DAGNode(meta, codeOp)

    def pipe(self, dag: DAG):
        super().pipe(dag)
        return [self.loadDAG()]

class FolderDAGDumper(IdentityStage):
    desc = "Dump DAG to a given folder"
    def generateMetaData(self, node: DAGNode):
        meta = node.dumpMetaDict()
        yamlLines = yaml.dump(meta, line_break='\n').split('\n')
        metaLines = [l + '\n' for l in node.codeOp.metaLines(yamlLines)]
        return metaLines
    def validate(self, dag: DAG):
        return Guard().do('output' not in self.env, 'Cannot find output file').get()
    def pipe(self, dag: DAG):
        ret = super().pipe(dag)
        for n in dag.nodes:
            # TODO: fix this
            filename = f"{n.name}.o{n.codeOp.fileType}"
            targetFile = os.path.join(self.env['output'], filename)
            with open(targetFile, 'w') as f:
                f.writelines(self.generateMetaData(n))
                f.writelines([''])
                f.writelines(n.codeOp.getCode())
        return ret

class FolderDAGGenerator(IdentityStage):
    desc = "Generate Json file for folder"
    def validate(self, dag: DAG):
        return Guard().do('output' not in self.env, 'Cannot find output path').get()
    def pipe(self, dag):
        ret = super().pipe(dag)
        path = self.env['output']
        filename = f"{self.env['output']}.json"
        generate.generate(path, filename)
        return ret
