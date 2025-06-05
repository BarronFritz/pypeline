from .connector import PypeConnector

class Pipe:
    def __init__(self, extract:PypeConnector, loads:list[PypeConnector]):
        self.extract = extract
        self.loads = loads
    
    def run(self) -> None:
        data = self.extract.read()
        for load in self.loads:
            load.write(data)