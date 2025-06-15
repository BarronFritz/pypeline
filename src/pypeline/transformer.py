"""PypeTransformer class definition."""

from abc import ABC, abstractmethod

from pypeline.data import PypeData


class PypeTransformer(ABC):
    """Abstract interface for transforming PypeData with SQL."""

    @abstractmethod
    def transform(self) -> PypeData:
        """Create a new PypeData object.

        Returns:
            PypeData: Transformed PypeData.

        """
        ...
