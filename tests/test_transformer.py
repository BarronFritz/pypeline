"""Tests for Transformer class."""

from typing import Any

from pypeline.data import PypeData
from pypeline.transformer import PypeTransformer


def test_transformer() -> None:
    """Test Transformer.transform() method."""
    data_dict: dict[str, Any] = {
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"],
    }
    pype_data = PypeData(data_dict)

    transformer = PypeTransformer(pype_data)

    trans_data = transformer.transform("SELECT * FROM data WHERE col1 >= 3")

    assert pype_data.cache_id != trans_data.cache_id
    assert not pype_data.collect().equals(trans_data.collect())

    trans_data.cache()
