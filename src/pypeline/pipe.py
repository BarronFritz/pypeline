"""PypePipe class definition."""

from pypeline.connector import PypeConnector


class Pipe:
    """Simple extract and load Handler."""

    def __init__(self, extract: PypeConnector, loads: list[PypeConnector]) -> None:
        """Create a new Pipe.

        Args:
            extract (PypeConnector): Connector with check and read options.
            loads (list[PypeConnector]): List of Connector's with extract option.

        """
        self.extract = extract
        self.loads = loads

    def run(self) -> None:
        """Perform internal operations in sequence.

        Extract PypeData from 'extract' PypeConnector into a local reference.
        For each loads PypeConnector, load extracted data to target destination.
        """
        data = self.extract.read()
        for load in self.loads:
            load.write(data)
