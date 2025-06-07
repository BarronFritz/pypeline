from pypeline.data import PypeData
from pathlib import Path

# Test DataConnector
from pypeline.connectors.data_connector import DataConnector
from pypeline.connectors.csv_connector import CSVConnector


def test_data_connector():
    # Create test data and setup connector
    test_data = dict({"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]})
    data = PypeData(test_data)
    extractor = DataConnector(data)

    # Test data exists
    assert extractor.check()
    test_data_2 = test_data
    test_data_2["col1"] = [6, 7, 8, 9, 10]
    # Test extractor data is the same as test_data
    assert extractor.read().df.equals(data.df)
    # Not equal after write
    extractor.write(PypeData(test_data_2))
    assert not extractor.read().df.equals(data.df)

def test_csv_connector():
    # Create test data and setup connector
    test_data = dict({"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]})
    data = PypeData(test_data)
    test_file = Path('tests/test_csv_connector.csv')
    csv_connector = CSVConnector(test_file)
    # Test file doesn't exist
    assert not test_file.exists()
    # Test file exists after write
    csv_connector.write(data)
    assert test_file.exists() and test_file.is_file()
    # Test extracted data is the same as loaded
    assert csv_connector.read().df.equals(data.df)
    # Clean-up
    test_file.unlink()

    
    