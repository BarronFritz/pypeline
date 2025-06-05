from pypeline.pipe import Pipe
from pypeline.connector import DataConnector, CSVConnector
from pathlib import Path

def test_pipe():
    # Create test data
    data = dict({
        "col1": [ 1 , 2 , 3 , 4 , 5 ],
        "col2": ['a','b','c','d','e']
    })
    # Extract "PypeData" from data dict
    dict_extractor = DataConnector(data)
    # Load to CSV file
    csv_path = Path('test.csv')
    loader = CSVConnector(csv_path)
    # Put operations into a pipe
    pipe1 = Pipe(dict_extractor, [loader])
    # Assert that pipe doesn't throw an error
    assert pipe1.run() is None
    # Assert csv file exists
    assert csv_path.exists() and csv_path.is_file()
    # Extract "PypeData" from csv file
    csv_extractor = CSVConnector(csv_path)
    # Assert csv_extractor confirms data is available
    assert csv_extractor.check()
    # Assert that data from csv matches data written to csv
    data_1 = dict_extractor.read()
    data_2 = csv_extractor.read()
    assert data_1.df.shape == data_2.df.shape # type: ignore
    # Cleanup Test
    csv_path.unlink()