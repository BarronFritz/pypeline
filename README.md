# Pypeline

A wrapper around polars and duckdb for simple and fast ETL.


## Minimum Viable Product: Alpha Checklist
+ [x] Simple object wrapper around `polars` dataframe.
+ [x] Simple abstract interface for creating new data connectors.
+ [x] Simple EL example with multiple load destinations. 
- [ ] Pre-Load SQL Transformations with `duckdb`
- [ ] Complex Pipe orchestration with Pipelines
    - In architecture planning phases. See Key objective for more information.


## Key Objectives
1. Support a range of ETL and ELT operations 
2. Simple and Easy to Learn API
3. Performance Critical operations outsource to faster libraries.
4. Arbitrarily complex pipe dependancy logic
    - Simple Extract and Load operations.
    - Simple Extract, Transform, Load operations.
    - Complex Extract Operations
        - Waiting for data to be available before attempting extract.
        - Waiting for availble data from multiple sources.
    - Complex Load Operations
        - Loading to multiple destinations.
    - Complex Transform Operations
        - Transforming data in stages.
        - Using staged data as source for an arbritrary number or transformations or loads.
        - Transforming data before loading to multiple sources
    - Join Data from Multiple Sources
    - Perform any number of transformations, and loads, in arbritrary non-acyclic order 


## Install Requirements
```
uv sync
```

## Run Tests
```
uv run pytest
```

## Library Usage
```python
from pypeline.pipe import Pipe
from pypeline.connectors import CSVConnector, SQLConnector

# Configure a source (csv file)
extractor = CSVConnector("file.csv")

# Configure a destination (database table)
loader = SQLConnector(db_conn, db_schema, db_table)

# Setup a pipe (data operation)
# loads accepts a list of connectors,
#   in order to load to multiple destinations.
pipe = Pipe(extract=extractor, loads=[loader])

# Schedule the pipe to run
pipe.run()
```