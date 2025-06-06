import polars as pl
from typing import Any


class PypeData:
    def __init__(self, data: Any):
        if isinstance(data, PypeData):
            data = data.df
        self.df = pl.DataFrame(data)
