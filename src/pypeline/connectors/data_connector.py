"""DataConnector class definition."""

from typing import Any

from pypeline.connector import PypeConnector
from pypeline.data import PypeData


class DataConnector(PypeConnector):
    """Connect to a PypeData source."""

    def __init__(self, data: PypeData | Any) -> None:  # noqa: ANN401
        """Create a new or reference to existing PypeData.

        Args:
            data (PypeData | Any): Attempt to convert Any type to PypeData.

        """
        self.data = PypeData(data)

    def check(self) -> bool:
        """Check if PypeData has data.

        Returns:
            bool: True if underlying dataframe is not empty.

        """
        return not self.data.df.is_empty()

    def read(self) -> PypeData:
        """Create new reference to the data.

        Returns:
            PypeData: new reference to the read data.

        """
        return self.data

    def write(self, data: PypeData) -> None:
        """Update internal PypeData object reference.

        Args:
            data (PypeData): New object to reference.

        """
        self.data = data
