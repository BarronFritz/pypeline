# Pypeline

A wrapper around polars and duckdb for simple and fast ETL.

## Usage

```python
# Configure a source (csv file)
extractor = CSVConnector("file.csv")
# Configure a destination (database table)
loader = SQLConnector(db_conn, db_schema, db_table)
# Setup a pipe (data operation)
pipe = Pipe(extract=extractor, loads=[loader])
# Schedule the pipe to run
pipe.run()
```