"""PypeConnector abstract base class definition."""

from abc import ABC, abstractmethod

from .data import PypeData


class PypeConnector(ABC):
    """Abstract base class for connecting to, reading and writing datasources."""

    @abstractmethod
    def check(self) -> bool:
        """Check if the expected data is ready to be extracted.

        Returns:
            bool: True if data is available.

        """

    @abstractmethod
    def read(self) -> PypeData:
        """Extract data into a PypeData object.

        Returns:
            PypeData: Object containing reference to polars DataFrame.

        """

    @abstractmethod
    def write(self, data: PypeData) -> None:
        """Load data into a PypeData object.

        Args:
            data (PypeData): Object containing reference to polars DataFrame.

        """
