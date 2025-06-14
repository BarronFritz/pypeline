"""PypeTransformer class definition."""

import duckdb

from pypeline.data import PypeData


class PypeTransformer:
    """Abstract interface for transforming PypeData with SQL."""

    def __init__(self, data: PypeData) -> None:
        """Create a transformation operation.

        Args:
            data (PypeData): Data to be transformed.

        """
        data.cache()
        self.data = data

    def transform(self, select_sql: str) -> PypeData:
        """Create a new PypeData object.

        Args:
            select_sql (str): Select Statement with "data" as the table name.
            Example: SELECT * FROM data WHERE col_a > 0.9

        Raises:
            ValueError: When select_sql does not contain 'FROM {data}'.

        Returns:
            PypeData: Copy of transformed data.

        """
        if "FROM DATA" not in select_sql.upper():
            msg = "transform(select_sql) must target a table called 'FROM data'"
            raise ValueError(msg)
        statement = select_sql.replace("data", f"'{self.data.cache_file}'")
        return PypeData(duckdb.query(statement).pl())
