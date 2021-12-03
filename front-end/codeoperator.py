from abc import *
import re

# classes for operating DAGs

# abstract class of Code Block, which can be black box code generate by external compilers
class ElementContent(ABC):
    def __init__(self, operator):
        self.op = operator
        
    @abstractmethod
    def dump(self):
        pass
    @abstractmethod
    def dumpWithMeta(self):
        pass

class CodeOperator:
    @property
    @abstractmethod
    def fileType(self):
        pass
    @abstractmethod
    def getCode(self):
        pass
    @abstractmethod
    def metaLines(self, lines):
        pass

class LoadedCode(CodeOperator):
    def __init__(self, lines):
        self.lines = lines
        self.focus = None
    def regexDelete(self, pattern):
        self.lines = [l for l in self.lines if re.match(pattern, l) is None]
    def regexReplace(self, pattern, repl):
        self.lines = [re.sub(l,pattern,repl) for l in self.lines]
    # override
    def getCode(self):
        return self.lines

class LoadedPythonCode(LoadedCode):
    fileType = ".py"
    def __init__(self, lines):
        super().__init__(lines)

    def removeImports(self):
        self.regexDelete(r"import.*")
        self.regexDelete(r"from.*import.*")
    def renameFunction(self, oldname, newname):
        self.regexReplace(rf'def ({oldname})\(.*\):', rf'{newname}')

    def metaLines(self, lines):
        return ['#@ ' + l for l in lines]

# python code with 
class MemoryOperator(CodeOperator):
    def __init__(self, codeList):
        self.code = codeList
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
        walk(self.tags)

    def tag(self, key, value = None):
        if value is not None:
            self.tags[key] = value
        return self.tags.get(key)

    def tagLine(self, key):
        return self.tag(key, CodePos(len(self.code)))
    

def getCodeByType(elementType, codeType, code):
    if elementType == 'compute':
        if codeType == '.py':
            return LoadedPythonCode(code)
    return None
