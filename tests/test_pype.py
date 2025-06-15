"""Test Pipe class operations."""

from pathlib import Path
from typing import Any

from sqlalchemy import inspect

from pypeline.connectors.csv_connector import CSVConnector
from pypeline.connectors.data_connector import DataConnector
from pypeline.connectors.sql_connector import SQLConnector
from pypeline.pipe import Pipe


def test_pipe() -> None:
    """Test simple pipe operations."""
    # Setup
    data: dict[str, Any] = {"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}
    dict_extractor = DataConnector(data)

    csv_path = Path("test.csv")
    csv_loader = CSVConnector(csv_path)

    connection_string = "mssql+pyodbc://@testdb?Trusted_Connection=yes"
    database_name = "testdb"
    schema_table = "dbo.test_tbl"
    sql_loader = SQLConnector(
        con_str=connection_string,
        db=database_name,
        table=schema_table,
    )

    # Put operations into a pipe
    pipe1 = Pipe(dict_extractor, [csv_loader, sql_loader])
    # Assert that pipe doesn't throw an error
    assert pipe1.run() is None
    # Assert csv file exists
    assert csv_path.exists()
    assert csv_path.is_file()
    # Extract "PypeData" from csv file
    csv_extractor = CSVConnector(csv_path)
    # Assert csv_extractor confirms data is available
    assert csv_extractor.check()
    # Assert that data from csv matches data written to csv
    data_1 = dict_extractor.read()
    data_2 = csv_extractor.read()
    assert data_1.dataframe.shape == data_2.dataframe.shape
    # Cleanup Test
    csv_path.unlink()
    # Cleanup testing environment
    _, table = schema_table.split(".")
    if inspect(sql_loader.engine).has_table(table):
        sql_loader.execute(f"DROP TABLE {schema_table}")
