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
    
    def create_new_table_schema(self, table_name : str, base_origem) -> None:
        """
        Cria-se uma nova tabela com as chaves únicas do output do book de variaveis

        Args:
            table_name: Nome da nova tabela.
        """
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} as
        select distinct chave_cpf, data_consulta_dado_bacen, codigo_modalidade_operacao
        from {base_origem}
        """
        self.execute_query(query)

    # def creating_bookscr_table(self, table_name : str, origin_table : str):
    #     """
    #     Inserindo valores no book do scr

    #     Args:
    #         origin_table : Tabela de origem do dado
    #     """
    #     query = f"""
    #         insert into {table_name} (chave_cpf, data_consulta, codigo_modalidade_operacao)
    #             select chave_cpf, data_consulta_dado_bacen, codigo_modalidade_operacao
    #             from {origin_table}
    #             group by chave_cpf, data_consulta_dado_bacen,codigo_modalidade_operacao
    #     """
    #     self.execute_query(query)

    def popula_dados_operacao(self, cod_operacao, nome_operacao, base_origem):
        """
        Método responsável por escrever em disco tabelas intermediárias de modalidades especificas
        É possível criar o book sem escrever em disco, porém esse processo possivelmente é mais rápido,
        possibilita testes unitários mais eficazes e habilita análises mais pontuais.

        Args:
            cod_operacao : codigo oriundo do bacen que é obtido via arquivo yaml externo
            nome_operacao : nome da operacao no dicionário de-para no arquivo yaml
            base_origem : base do scr na qual estamos oberservando a fonte original do dado
        """
        query = f"""
        create table if not exists {nome_operacao} as
        with operacao as (
                select 
                    chave_cpf, 
                    data_consulta_dado_bacen,
                    codigo_modalidade_operacao,
                    SUM(coalesce(valor_credito_vencer_ate_30_dia,0)) as {nome_operacao}_valor_credito_vencer_ate_30_dia, 
                    SUM(coalesce(valor_credito_vencer_31_60_dia,0)) as {nome_operacao}_valor_credito_vencer_31_60_dia, 
                    SUM(coalesce(valor_credito_vencer_61_90_dia,0)) as {nome_operacao}_valor_credito_vencer_61_90_dia, 
                    SUM(coalesce(valor_credito_vencer_acima_90_dia,0)) as {nome_operacao}_valor_credito_vencer_acima_90_dia, 
                    SUM(coalesce(valor_credito_vencido_15_30_dia,0)) as {nome_operacao}_valor_credito_vencido_15_30_dia, 
                    SUM(coalesce(valor_credito_vencido_31_60_dia,0)) as {nome_operacao}_valor_credito_vencido_31_60_dia, 
                    SUM(coalesce(valor_credito_vencido_61_90_dia,0))  as {nome_operacao}_valor_credito_vencido_61_90_dia, 
                    SUM(coalesce(valor_credito_vencido_acima_90_dia,0)) as {nome_operacao}_valor_credito_vencido_acima_90_dia
                from {base_origem}
                group by chave_cpf, data_consulta_dado_bacen 
                having codigo_modalidade_operacao in {cod_operacao}
            )
            select 
                a.chave_cpf,
                a.data_consulta_dado_bacen,
                b.{nome_operacao}_valor_credito_vencer_ate_30_dia,
                b.{nome_operacao}_valor_credito_vencer_31_60_dia,
                b.{nome_operacao}_valor_credito_vencer_61_90_dia,
                b.{nome_operacao}_valor_credito_vencer_acima_90_dia,
                b.{nome_operacao}_valor_credito_vencido_15_30_dia,
                b.{nome_operacao}_valor_credito_vencido_31_60_dia,
                b.{nome_operacao}_valor_credito_vencido_61_90_dia,
                b.{nome_operacao}_valor_credito_vencido_acima_90_dia
            from bacen_replica a
            join operacao as b
            on a.chave_cpf = b.chave_cpf 
                and a.data_consulta_dado_bacen = b.data_consulta_dado_bacen
                and b.codigo_modalidade_operacao = a.codigo_modalidade_operacao;
        """
        self.execute_query(query)