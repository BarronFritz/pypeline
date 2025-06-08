"""PypeData class definition."""

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from polars._typing import FrameInitTypes  # type: ignore  # noqa: PGH003


class PypeData:
    """Wrapper for polars DataFrame.

    Which itself is a wrapper around an apache-arrow dataset.
    """

    def __init__(self, data: "PypeData | FrameInitTypes | pl.DataFrame") -> None:  # type: ignore  # noqa: PGH003
        """Create a new reference to passed 'data' or construct from source.

        Args:
            data (Any): PypeData, polars DataFrame, or DataFrame initializer.

        """
        if isinstance(data, PypeData):
            self.df = data.df
        elif isinstance(data, pl.DataFrame):
            self.df = data
        else:
            self.df = pl.DataFrame(data)
