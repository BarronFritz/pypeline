"""SQLConnector connects to a sql database using an sqlalchemy connection string."""

from polars import read_database
from sqlalchemy import create_engine, inspect, text

from pypeline.connector import PypeConnector
from pypeline.data import PypeData


class SQLConnector(PypeConnector):
    """SQLConnector connects to a sql database using an sqlalchemy connection string."""

    def __init__(
        self,
        con_str: str,
        db: str,
        table: str,
    ) -> None:
        """Create a PypeConnector that can connect to a variety of SQL sources.

        Args:
            con_str (str): sqlalchemy connection string.
            db (str | None, optional): Name of database to connect to.
            table (str | None, optional): schema qualified table name.

        """
        self.db = db
        self.engine = create_engine(con_str, connect_args={"database": self.db})
        self.table = table
        self.select = text(f"SELECT * FROM {self.table}")  # noqa: S608

    def check(self) -> bool:
        """Test if the data is available to be read().

        Returns:
            bool: True if data is available. False otherwise.

        """
        database = inspect(self.engine)
        table = self.table

        # Check if temp table exists.
        if "#" in table and not database.has_table(table_name=table):
            return False  # Table does not exist!

        # Split schema/table if table has a dot.
        if "." in table:
            schema, table = self.table.split(".")
            if not database.has_table(table_name=table, schema=schema):
                return False  # Schema.Table does not exist
        elif not database.has_table(table_name=table):
            return False  # Table does not exist

        sql = f"SELECT 1 WHERE EXISTS ({self.select.bindparams()})"
        with self.engine.connect() as db_conn:
            data = read_database(
                query=sql,
                connection=db_conn,
            )
            is_empty: bool = data.is_empty()  # type: ignore  # noqa: PGH003
            return not is_empty  # Data doesn't exists

    def read(self) -> PypeData:
        """Execute "SELECT * FROM table".

        Returns:
            PypeData: data returned from select statement

        """
        with self.engine.connect() as db_conn:
            data = read_database(query=self.select, connection=db_conn)
            return PypeData(data)

    def write(self, data: PypeData) -> None:
        """Truncate and load data to schema_table.

        Args:
            data (PypeData): _description_

        """
        with self.engine.connect() as db_conn:
            data.df.write_database(
                table_name=self.table,
                connection=db_conn,
                if_table_exists="replace",
            )
            db_conn.commit()

    def execute(self, query: str) -> None:
        """Execute arbitrary SQL statements.

        Args:
            query (str): Executable SQL Statement.

        """
        with self.engine.connect() as db_conn:
            db_conn.execute(statement=text(query))
            db_conn.commit()
