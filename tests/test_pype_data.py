from pypeline.data import PypeData
import polars as pl

def test_pype_data_constructor():
    data = dict({"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]})
    pl_df = pl.DataFrame(data)
    pd = PypeData(data)
    assert pd.df.equals(pl_df)