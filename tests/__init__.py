"""pytest module."""

from pathlib import Path


def test_tests_dir_exists() -> None:
    """Test that the 'tests' directory exists."""
    path = Path("tests")
    assert path.exists()
    assert path.is_dir()


def test_cache_dir_exists() -> None:
    """Test for the existance of cache directory."""
    path = Path("cache")
    assert path.exists()
    assert path.is_dir()
