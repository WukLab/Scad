class ElementResourceHint:
    def __init__(self):
        pass

class ElementCompiler(ABC):
    def __init__(self):
        pass

    # split an element, 
    @abstractmethod
    def split(self, element, hint = None):
        pass

    # try to merge a set of elements
    @abstractmethod
    def merge(self, elements):
        pass

class ElementCompiler(MatchEngine):
    def __init__(self, regex, matched):
        pass

    def identify(self, source):
        pass
