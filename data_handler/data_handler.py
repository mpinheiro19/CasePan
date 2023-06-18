from db.db import SQLiteSingleton


class DataHandler:
    def __init__(self, db: SQLiteSingleton):
        """
        Initializes the DataHandler with a SQLite database connection for SQLite singleton class.

        Args:
            db: SQLiteSingleton database connector.

        """
        self.db = db

    def execute_query(self, query: str) -> None:
        """
        Executes a query to modify tables in the SQLite database, commit and close DB connection.

        Args:
            query: The SQL query to execute.

        """
        cursor = self.db.connection.cursor()
        cursor.execute(query)
        self.db.connection.commit()
        cursor.close()

    def replicate_table(self, table_name: str, new_table_name: str) -> None:
        """
        Creates a 'replica' from scr table for data handling integrity
        puroposes.

        Args:
            table_name: Name of the table to be replicated
            new_table_name: Name of the new table
        """
        query = f"CREATE TABLE IF NOT EXISTS {new_table_name} as SELECT * FROM {table_name}"
        self.execute_query(query)

    def fill_nan(self, table_name: str, column: str, value: any) -> None:
        """
        Fills missing values (NaN) in the specified column of a table with the given value.

        Args:
            table_name: The name of the table.
            column: The name of the column containing missing values.
            value: The value to fill the missing values with.
        """
        query = f"UPDATE {table_name} SET {column} = {value} WHERE {column} IS 'nan'"
        self.execute_query(query)
    
    def create_new_table_schema(self, table_name : str) -> None:
        """
        Cria-se uma nova tabela com as chaves únicas do output do book de variaveis

        Args:
            table_name: Nome da nova tabelatabela.
        """
        query = f"""CREATE TABLE IF NOT EXISTS {table_name}(
        chave_cpf varchar NOT NULL,
        data_consulta varchar NOT NULL
        )"""
        self.execute_query(query)

    def creating_bookscr_table(self, table_name : str, origin_table : str):
        """
        Inserindo valores no book do scr

        Args:
            origin_table : Tabela de origem do dado
        """
        query = f"""
            insert into {table_name} (chave_cpf, data_consulta)
                select chave_cpf, data_consulta_dado_bacen
                from {origin_table}
                group by chave_cpf, data_consulta_dado_bacen
        """
        self.execute_query(query)

    