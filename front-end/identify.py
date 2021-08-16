class Identifier(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def identify(self, source):
        pass

class DatabaseIdentifier(MatchEngine):
    def __init__(self, regex, matched):
        pass

    def identify(self, source):
        pass
