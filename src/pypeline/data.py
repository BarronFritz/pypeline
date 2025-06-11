"""PypeData class definition."""

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from polars import DataFrame, LazyFrame, read_parquet, scan_parquet

if TYPE_CHECKING:
    from polars._typing import FrameInitTypes  # type: ignore  # noqa: PGH003

DEFAULT_CACHE_FOLDER = "cache/"


class PypeData:
    """Wrapper for polars DataFrame.

    Which itself is a wrapper around an apache-arrow dataset.
    """

    def __init__(self, data: "PypeData | FrameInitTypes | DataFrame") -> None:  # type: ignore  # noqa: PGH003
        """Wrap data to be used in the pypeline.

        Args:
            data (Any): PypeData, Polars DataFrame, or DataFrame initializer.
            TODO: Accept Pandas DataFrame

        """
        # Assign a new UUID
        self.cache_id: UUID = uuid4()
        self.cache_file: Path = Path(DEFAULT_CACHE_FOLDER) / f"{self.cache_id}.parquet"
        self.dataframe: DataFrame | None = None

        # PypeData initializer - Copy existing cache_id
        if isinstance(data, PypeData):
            self.cache_id = data.cache_id
            self.cache_file: Path = (
                Path(DEFAULT_CACHE_FOLDER) / f"{self.cache_id}.parquet"
            )
            self.dataframe = data.dataframe

        # Polars DataFrame initializer
        elif isinstance(data, DataFrame):
            self.dataframe = data

        # Polars raw data initializer.
        else:
            self.dataframe = DataFrame(data)

    def is_cached(self) -> bool:
        """Check if the data is cached."""
        if not self.cache_file.exists():
            print("is_cached.exists = False")
            return False
        if not self.cache_file.is_file():
            print("is_cached.is_file = False")
            return False
        return True

    def cache(self) -> None:
        """Cache data to local parquet file.

        Args:
            data (DataFrame): Polars DataFrame

        """
        if self.is_cached():
            return

        if self.dataframe is None:
            msg = f"{self.cache_file} is not cached, and {self.dataframe} is None."
            raise ValueError(msg)

        # Ensure parent folders are created.
        self.cache_file.parent.mkdir(exist_ok=True)
        self.dataframe.write_parquet(file=self.cache_file)

        # Ensure that cache loaded correctly, and clear the dataframe from memory
        if self.is_cached():
            self.dataframe = None

    def clear_cache(self) -> None:
        """Delete local parquet file if exists."""
        if self.cache_file.exists():
            if self.dataframe is None:
                self.dataframe = read_parquet(self.cache_file)
            self.cache_file.unlink()

    def lazy_collect(self) -> LazyFrame:
        """Lazily collect cached data from file.

        Call collect() again to perform extract.

        Raises:
            FileNotFoundError: If unable to find cache file.

        Returns:
            LazyFrame: Polars LazyFrame.

        """
        if not self.cache_file.exists():
            msg = f"File {self.cache_file} not found. Did you call cache() first?"
            raise FileNotFoundError(msg)

        return scan_parquet(self.cache_file)

    def collect(self) -> DataFrame:
        """Get dataframe, from cache if cached.

        Returns:
            DataFrame: Polars DataFrame.

        """
        if self.cache_file.exists():
            return read_parquet(self.cache_file)

        if self.dataframe is None:
            raise ValueError

        return self.dataframe
