import yaml
import re

class CodePos:
    def __init__(self, pos):
        self.pos = pos

    def shift(self, cur, n):
        if self.pos > cur:
            self.pos += n

class Code:
    def __init__(self, ext):
        self.code = []
        self.tags = {}
        self.ext = ext
        self.cp = 0

    def __str__(self):
        return '\n'.join(self.code)
    def __iter__(self):
        return self
    def __next__(self):
        cur = code[cp]
        cp += 1
        return cur

    def pos(self, n):
        if n > len(self.code):
            raise RuntimeError('Index to Longer Position')
        self.cp = n
        return self

    def walk(v, f):
        if isinstance(v, dict):
            for dk, dv in v.items():
                walk(dv, f)
        elif isinstance(lv, list):
            for lv in l:
                walk(lv, f)
        else:
            f(v)
            
            
    def append(self, *lines):
        self.code.concat(lines)
        self.cp += len(lines)
    def insert(self, pos, *lines):
        cur = self.cp
        def changeReference(p):
            if isinstance(p, CodePos):
                p.shift(cur, p)
        # walk for nested datastructures
        # walk(self.tags,

    def tag(self, key, value = None):
        if value is not None:
            self.tags[key] = value
        return self.tags.get(key)
    def tagLine(self, key):
        return self.tag(key, CodePos(len(self.code)))

class YamlCode(Code):
    def __init__(self):
        super().__init__(self, 'yaml')
    def meta(self, lines):
        self.append(lines)

class PythonCode(Code):
    def __init__(self):
        super().__init__(self, 'py')
        self.spaces = 0

    def append(self, *lines):
        self.code.concat([" " * self.spaces + l for l in lines])
    def cr(self, n = 1):
        self.code.concat([" " * self.spaces] * n) 
    def comments(self, lines, lead = '# '):
        self.code.concat([ lead + l for l in lines])
    def meta(self, lines):
        self.comments(lines, '#@ ')
    def indent(self, n = 1):
        self.spaces += n * 4
    def deindent(self, n = 1):
        pass
        # self.space = min(self.spaces -= n * 4, 0)
        

class CodeGenerator:
    def __init__(self, priority, requires = [],
                 requireTags = [], produceTags = []):
        # priority:
        # 0 preprocess
        # 20 meta and tempaltes
        # 40 main code generation
        # 60 match and change of code
        # 80 post generation changes
        self.priority = priority
        self.requires = []
        self.requireTags = []
        self.produceTags = []
    
    def generate(self, code, physical):
        pass

    def invoke(self, code, phyiscal):
        for p in self.requires:
            if p not in code.tag('__passes'):
                raise RuntimeError(f'Generation Pass {name} Requires {p} to be done')
        return self.generate(code, physical)
        

class RuleCodeGen(CodeGenerator):
    def __init__(self, ruleFile):
        super().__init__(60)
        self.ruleFile = ruleFile

    def generate(self, physical):
        pass

class MetaCodeGen(CodeGenerator):
    def __init__(self, ruleFile):
        super().__init__(20)
        self.ruleFile = ruleFile

    def generate(self, code, physical):
        meta = {}
        meta['type'] = physical.type
        if physical.dependents:
            meta['dependents'] = [
                p.name for p in physical.dependents]
        if physical.dependents:
            meta['parents'] = [
                p.name for p in physical.parents]
        if physical.corunning:
            meta['corunning'] = [
                p.name for p in physical.parents]

        meta['limits'] = {}
        if phyiscal.resources.mem:
            meta['limits']['mem'] = phyiscal.resources.mem
        if phyiscal.resources.cpu:
            meta['limits']['cpu'] = phyiscal.resources.cpu
        if phyiscal.resources.storage:
            meta['limits']['storage'] = phyiscal.resources.storage
        if len(meta['limits']) == 0:
            del meta['limits']
        
        code.comments(yaml.dump(meta))

class ArrayTagCodeGen(CodeGenerator):
    RE_OFFSET_INDEXING = re.compile('\d+')
    RE_OFFSET_RANGE = re.compile('\d*:\d*')

    def __init__(self):
        super().__init__(60, requires = 'Main')

    def generate(self, code, phyiscal):
        arrays = code.tag('remote_array')
        if arrays is not None:
            array_uses = { a: [] for a in arrays }
            compiled_regex = {
                a: re.compile(f'{a}[(.*)]') for a in arrays }
            for nu, line in code:
                for r in compiled_regex:
                    for m in compiled_regex[r].searchall(line):
                        index = match.group(1)
                        pos = match.pos
                        if RE_OFFSET_INDEXING.matchall(index):
                            array_uses[r].append((index, 'INDEXING', pos))
                        elif RE_OFFSET_RANGE.matchall(index):
                            array_uses[r].append((index, 'RANGE', pos))

class MainCodeGen(CodeGenerator):
    MAIN_LINE = "def main(action, params):"

    def __init__(self, ruleFile):
        super().__init__(40)
        self.ruleFile = ruleFile

    def generate(self, code, physical):
        code.cr()
        code.tagLine('premain') 
        code.append(MAIN_LINE)
        code.tagLine('main') 
        code.indent()
        code.append(*phyiscal.source)
        code.deindent()

