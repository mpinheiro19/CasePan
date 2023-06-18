import sqlite3
import pandas as pd


class SQLiteSingleton:
    """
    Singleton class for managing an SQLite database connection and data retrieval into pandas DataFrame.
    """

    _instance = None

    def __new__(cls, db_file: str):
        """
        Creates a new instance of the SQLiteSingleton class or returns an existing instance if available.

        Args:
            db_file: Path to the SQLite database file.

        Returns:
            An instance of SQLiteSingleton class.

        """
        if not cls._instance:
            cls._instance = super(SQLiteSingleton, cls).__new__(cls)
            cls._instance.connection = sqlite3.connect(db_file)
        return cls._instance


    def read_dataframe(self, table_name: str) -> pd.DataFrame:
        """
        Reads data from the specified table into a pandas DataFrame.

        Args:
            table_name: Name of the table to read data from.

        Returns:
            A pandas DataFrame containing the queried data.

        """
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.connection)
        return df