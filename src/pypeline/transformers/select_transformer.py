"""PypeTransformer class definition."""

import duckdb

from pypeline.data import PypeData
from pypeline.transformer import PypeTransformer


class SelectTransformer(PypeTransformer):
    """Transform PypeData with a SQL Statement."""

    def __init__(self, data: PypeData, select_sql: str) -> None:
        """Create a transformation operation.

        Raises:
            ValueError: When select_sql does not contain 'FROM data'.

        Args:
            data (PypeData): Data to be transformed.
            select_sql (str): Select Statement with "data" as the table name.
            Example: SELECT * FROM data WHERE col_a > 0.9

        """
        data.cache()
        self.data = data

        if "FROM DATA" not in select_sql.upper():
            msg = "SQL Statement must target a table called data."
            raise ValueError(msg)
        self.select = select_sql.replace("data", f"'{self.data.cache_file}'")

    def transform(self) -> PypeData:
        """Create a new PypeData object.

        Returns:
            PypeData: Copy of transformed data.

        """
        return PypeData(duckdb.query(self.select).pl())
