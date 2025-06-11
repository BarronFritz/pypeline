"""Parquet connector class definition."""

from pathlib import Path

from polars import read_parquet

from pypeline.connector import PypeConnector
from pypeline.data import PypeData


class ParquetConnector(PypeConnector):
    """Connect to a parquet file.

    Args:
        PypeConnector (ABC): Serialize and Deserialize pypeline data.

    """

    def __init__(self, filepath: Path | str) -> None:
        """Create a connection to a parquet file.

        Args:
            filepath (Path | str): Path to the parquet file. Converts to pathlib.

        """
        self.filepath = Path(filepath)

    def check(self) -> bool:
        """Check if the file is availble.

        Returns:
            bool: True if file exists and path is a file.

        """
        return self.filepath.exists() and self.filepath.is_file()

    def read(self) -> PypeData:
        """Extract parquet data into PypeDate.

        Returns:
            PypeData: Extracted data in a PypeData object.

        """
        return PypeData(read_parquet(self.filepath))

    def write(self, data: PypeData) -> None:
        """Load data into parquet file.

        Args:
            data (PypeData): PypeData to load to parquet file.

        """
        data.collect().write_parquet(self.filepath)
