from abc import ABC, abstractmethod
from .data import PypeData
from pathlib import Path
import polars as pl
from typing import Any
from sqlalchemy import create_engine, inspect


class PypeConnector(ABC):
    @abstractmethod
    def check(self) -> bool:
        pass

    @abstractmethod
    def read(self) -> PypeData:
        pass

    @abstractmethod
    def write(self, data: PypeData):
        pass


class DataConnector(PypeConnector):
    def __init__(self, data: PypeData | Any):
        self.data = PypeData(data)

    def check(self) -> bool:
        return self.data.df.shape != (0, 0)

    def read(self) -> PypeData:
        return self.data

    def write(self, data: PypeData):
        self.data = data


class CSVConnector(PypeConnector):
    def __init__(self, filepath: Path | str, separator: str = ","):
        self.path = Path(filepath)

    def check(self) -> bool:
        return self.path.is_file()

    def read(self) -> PypeData:
        return PypeData(pl.read_csv(self.path))

    def write(self, data: PypeData):
        data.df.write_csv(self.path)


class SQLConnector(PypeConnector):
    def __init__(
        self,
        con_str: str,
        db: str | None = None,
        schema: str | None = None,
        table: str | None = None,
    ):
        self.db = db
        self.schema = "dbo" if schema is None else schema
        self.table = table
        self.engine = create_engine(con_str, connect_args={"database": self.db})

    def check(self) -> bool:
        return inspect(self.engine, raiseerr=False) is not None  # type: ignore

    def read(self) -> PypeData:
        if self.db is None or self.table is None:
            raise ValueError("read requires 'db' and 'table' be set")

        sql = f"SELECT * FROM {self.schema}.{self.table}"
        with self.engine.connect() as db_conn:
            df = pl.read_database(query=sql, connection=db_conn)
            return PypeData(df)

    def write(self, data: PypeData):
        if self.db is None or self.table is None:
            raise ValueError("write requires 'db' and 'table' be set")

        with self.engine.connect() as db_conn:
            data.df.write_database(
                f"{self.schema}.{self.table}",
                connection=db_conn,
                if_table_exists="replace",
            )
