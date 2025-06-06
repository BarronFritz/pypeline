from pypeline.pipe import Pipe
from pypeline.data import PypeData
from pypeline.connector import DataConnector, SQLConnector, CSVConnector
from pathlib import Path


def test_pipe():
    # Create test data
    data = PypeData(dict({"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}))
    # Extract "PypeData" from data dict
    dict_extractor = DataConnector(data)
    # Load to CSV file
    test_con_str = "mssql+pyodbc://@testdb"
    test_db = "testdb"
    test_table = "test_tbl"
    loader = SQLConnector(con_str=test_con_str, db=test_db, table=test_table)
    # Put operations into a pipe
    pipe1 = Pipe(dict_extractor, [loader])
    # Extract data back out of database
    csv_path = Path("tests/test.csv")
    csv_loader = CSVConnector(csv_path)
    pipe2 = Pipe(loader, [csv_loader])
    # Run pipes
    pipe1.run()
    pipe2.run()
    # Assert data out = data in
    assert csv_path.exists() and csv_path.is_file()
    data_in = csv_loader.read()
    csv_path.unlink()
    assert data.df.shape == data_in.df.shape
