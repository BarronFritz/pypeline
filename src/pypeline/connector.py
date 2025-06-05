from abc import ABC, abstractmethod
from .data import PypeData
from pathlib import Path
import polars as pl
from typing import Any

class PypeConnector(ABC):
    @abstractmethod
    def check(self) -> bool:
        pass
    @abstractmethod
    def read(self) -> PypeData:
        pass
    @abstractmethod
    def write(self, data:PypeData):
        pass

class DataConnector(PypeConnector):
    def __init__(self, data:PypeData|Any):
        self.data = PypeData(data)
    def check(self) -> bool:
        return self.data.df.shape != (0,0)
    def read(self) -> PypeData:
        return self.data
    def write(self, data:PypeData):
        self.data = data

class CSVConnector(PypeConnector):
    def __init__(self, filepath:Path|str, separator:str=","):
        self.path = Path(filepath)
    def check(self) -> bool:
        return self.path.is_file()
    def read(self) -> PypeData:
        return PypeData(pl.read_csv(self.path))
    def write(self, data:PypeData):
        data.df.write_csv(self.path)