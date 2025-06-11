"""Test built-in connector classes can init, check, read, and write."""

from pathlib import Path
from typing import Any

from sqlalchemy import inspect

from pypeline.connectors.csv_connector import CSVConnector
from pypeline.connectors.data_connector import DataConnector
from pypeline.connectors.parquet_connector import ParquetConnector
from pypeline.connectors.sql_connector import SQLConnector
from pypeline.data import PypeData


def test_data_connector() -> None:
    """Test operations for the DataConnector.

    Tests init, check, read, and write.
    """
    # Create test data and setup connector
    test_data: dict[str, Any] = {
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"],
    }
    data = PypeData(test_data)
    extractor = DataConnector(data)

    # Test data exists
    assert extractor.check()  # noqa: S101
    test_data_2 = test_data
    test_data_2["col1"] = [6, 7, 8, 9, 10]

    # Test extractor data is the same as test_data
    assert extractor.read().collect().equals(data.collect())  # noqa: S101

    # Not equal after write
    extractor.write(PypeData(test_data_2))
    assert not extractor.read().collect().equals(data.collect())  # noqa: S101


def test_parquet_connector() -> None:
    """Test operations for the ParquetConnector.

    Tests init, check, read, and write.
    """
    test_data: dict[str, Any] = {
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"],
    }
    data = PypeData(test_data)

    test_file = Path("tests/test_parquet_connector.parquet")
    parquet_connector = ParquetConnector(test_file)

    # Test file doesn't exist
    assert not parquet_connector.check()  # noqa: S101
    assert not test_file.exists()  # noqa: S101

    # Test file exists after write
    parquet_connector.write(data)
    assert parquet_connector.check()  # noqa: S101

    # Test extracted data is the same as loaded
    assert parquet_connector.read().dataframe.equals(data.dataframe)  # noqa: S101

    # Clean-up
    test_file.unlink()


def test_csv_connector() -> None:
    """Test operations for the CSVConnector.

    Tests init, check, read, and write.
    """
    test_data: dict[str, Any] = {
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"],
    }
    data = PypeData(test_data)

    test_file = Path("tests/test_csv_connector.csv")
    csv_connector = CSVConnector(test_file)

    # Test file doesn't exist
    assert not csv_connector.check()  # noqa: S101
    assert not test_file.exists()  # noqa: S101

    # Test file exists after write
    csv_connector.write(data)
    assert csv_connector.check()  # noqa: S101

    # Test extracted data is the same as loaded
    assert csv_connector.read().dataframe.equals(data.dataframe)  # noqa: S101

    # Clean-up
    test_file.unlink()


def test_sql_connector() -> None:
    """Test operations for the SQLConnector.

    Tests init, check, read, write, and execute.
    """
    database_name = "testdb"
    schema, table = ("dbo", "test_tbl")
    database_table = f"{schema}.{table}"

    # Create test data and setup connector
    test_data: dict[str, Any] = {
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"],
    }
    data = PypeData(test_data)

    connection_string = "mssql+pyodbc://@testdb?Trusted_Connection=yes"
    sql_connector = SQLConnector(
        con_str=connection_string,
        db=database_name,
        table=database_table,
    )

    # Cleanup testing environment
    if inspect(sql_connector.engine).has_table(table):
        sql_connector.execute(f"DROP TABLE {database_table}")

    # Test data does not exist before write
    assert not sql_connector.check()  # noqa: S101

    # Test data exists after write
    sql_connector.write(data)
    assert sql_connector.check()  # noqa: S101

    # Test extracted data is the same as loaded
    assert sql_connector.read().collect().equals(data.collect())  # noqa: S101

    # Cleanup testing environment
    if inspect(sql_connector.engine).has_table(table):
        sql_connector.execute(f"DROP TABLE {database_table}")
