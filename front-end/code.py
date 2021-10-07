from abc import ABC

# abstract class of Code Block, which can be black box code generate by external compilers
class CodeOperator(ABC):
    @abstractmethod
    def getLine(self, n):
        pass


class Code(ABC):
    def __init__(self, operator):
        self.op = operator
        

    @abstractmethod
    def dump(self):
        pass
    @abstractmethod
    def dumpWithMeta(self):
        pass

class PythonCode(Code):
    pass

class FileOperator(CodeOperator):
    def __init__(self, code):
        self.code = code

    def toArray(self):
        return ArrayOperator(self, code):

    def copy(self):
        pass
        

# python code with 
class ArrayOperator(CodeOperator):
    def __init__(self, code):
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
        
    def __getitem__(self, key):
        return self.code[key]
    def __setitem__(self, key, value):
        return self.code[key] = value

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
        walk(self.tags, 

    def tag(self, key, value = None):
        if value is not None:
            self.tags[key] = value
        return self.tags.get(key)

    def tagLine(self, key):
        return self.tag(key, CodePos(len(self.code)))
    
    
