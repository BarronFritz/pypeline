"""CSV Connector class definition."""

from pathlib import Path

from polars import read_csv

from pypeline.connector import PypeConnector
from pypeline.data import PypeData


class CSVConnector(PypeConnector):
    """Connect to a csv file.

    Args:
        PypeConnector (ABC): Serialize and Deserialize pypeline data.

    """

    def __init__(self, filepath: Path | str, separator: str = ",") -> None:
        """Create a connection to a csv file.

        Args:
            filepath (Path | str): Path to the csv file. Converts to pathlib.
            separator (str, optional): Custom Delimiter. Defaults to ",".

        """
        self.filepath = Path(filepath)
        self.separator = separator

    def check(self) -> bool:
        """Check if the file is availble.

        Returns:
            bool: True if file exists and path is a file.

        """
        return self.filepath.exists() and self.filepath.is_file()

    def read(self) -> PypeData:
        """Extract csv data into PypeDate.

        Returns:
            PypeData: Extracted data in a PypeData object.

        """
        return PypeData(read_csv(self.filepath, separator=self.separator))

    def write(self, data: PypeData) -> None:
        """Load data into csv file.

        Args:
            data (PypeData): PypeData to load to csv file.

        """
        data.collect().write_csv(self.filepath, separator=self.separator)
