import polars as pl
from typing import Any

class PypeData:
    def __init__(self, data:Any):
        self.df = pl.DataFrame(data)