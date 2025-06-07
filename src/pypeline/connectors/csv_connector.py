from ..connector import PypeConnector
from ..data import PypeData
from pathlib import Path
from polars import read_csv

class CSVConnector(PypeConnector):
    def __init__(self, filepath: Path | str, separator: str = ","):
        self.path = Path(filepath)

    def check(self) -> bool:
        return self.path.is_file()

    def read(self) -> PypeData:
        return PypeData(read_csv(self.path))

    def write(self, data: PypeData):
        data.df.write_csv(self.path)
