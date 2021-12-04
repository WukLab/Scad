from stage import Stage

class ElementResourceHint:
    def __init__(self):
        pass

class ElementCompiler(Stage):
    def __init__(self):
        pass

    def pipe(self):
        pass

    # split an element, 
    @abstractmethod
    def split(self, element, hint = None):
        pass

    # try to merge a set of elements
    @abstractmethod
    def merge(self, elements):
        pass

    # common help methods:
    @abstractmethod
    def canSplit(self, element):
        pass
    

class ElementCompiler(MatchEngine):
    def __init__(self, regex, matched):
        pass

    def identify(self, source):
        pass
