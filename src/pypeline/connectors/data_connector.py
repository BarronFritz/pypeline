from ..connector import PypeConnector
from ..data import PypeData
from typing import Any

class DataConnector(PypeConnector):
    def __init__(self, data: PypeData | Any):
        self.data = PypeData(data)

    def check(self) -> bool:
        return self.data.df.shape != (0, 0)

    def read(self) -> PypeData:
        return self.data

    def write(self, data: PypeData):
        self.data = data
