"""Test PypeData class methods."""

from typing import Any

import polars as pl
import pytest

from pypeline.data import PypeData


def test_pype_data_constructor() -> None:
    """Test PypeData constructor."""
    data: dict[str, Any] = {"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}
    data_from_dict = PypeData(data)
    data_from_polars_df = pl.DataFrame(data)
    data_from_pype_data = PypeData(data_from_dict)

    assert data_from_dict.dataframe.equals(data_from_polars_df)
    assert data_from_dict.dataframe.equals(data_from_pype_data.dataframe)
    assert data_from_pype_data.dataframe.equals(data_from_polars_df)


def test_pype_data_cache() -> None:
    """Test PypeData 'cache' and 'collect' methods."""
    data: dict[str, Any] = {"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}
    data_from_dict = PypeData(data)

    # Persisted data does not exist
    assert not data_from_dict.is_cached()
    # Persist data to disk. Verify it exists now.
    data_from_dict.cache()
    assert data_from_dict.is_cached()
    # Delete persisted data on disk. Verify it's gone.
    data_from_dict.clear_cache()
    assert not data_from_dict.is_cached()

    # Persist again, then collect a lazy frame
    data_from_dict.cache()
    lazy_frame = data_from_dict.lazy_collect()
    assert isinstance(lazy_frame, pl.LazyFrame)

    data_frame = data_from_dict.collect()
    assert data_frame.equals(lazy_frame.collect())

    # Delete the data and try to collect the lazy_frame
    data_from_dict.clear_cache()
    with pytest.raises(FileNotFoundError):
        lazy_frame.collect()
