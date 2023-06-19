from db.db import SQLiteSingleton
from data_handler.data_handler import DataHandler
import yaml

# Open and read the YAML file
with open('global_params.yaml', 'r') as file:
    params = yaml.safe_load(file)


dbfile = params['db_file']['path']
db = SQLiteSingleton(dbfile)


if __name__ == '__main__':

    # instancia a classe de data handler que herda a conexão com db
    dth = DataHandler(db)

    # Replica a tabela original para melhor track de modificações
    # (passo opcional, feito apenas para manter histórico de modificações em
    # ambientes distintos!)
    orig_table = params['db_file']['original_table']
    replica = params['db_file']['replica_table']

    dth.replicate_table(orig_table, replica)

    # implementa o método de preencher os valores nan encontrados na exploratória com
    # o 'wildcard value' de 0 seguindo o schema encontrado na tabela scr
    # Note que não foi possível encontrar um padrão mais adequado, portanto vou completar com 0
    dth.fill_nan(
        table_name=replica,
        column=params['cols_to_normalize'],
        value='0'
    )
    
    # Cria-se a base da tabela para o book de variáveis
    # essa tabela contém registros únicos para popular com os dados
    dth.cria_book_scr_table_keys(replica)

    # Criando novas tabela para dar origem ao book de variáveis.
    # Essas tabelas contém apenas chave_cpf e data_consulta e o consolidado do segmento
    # Esse processo é interessante pois permite maior flexibilidade de testes unitários 
    # e habilita reprocessamento de segmentos isolados caso necessidade
    for nome_operacao in params['cod_modalidade']['operacoes']:
        cd_op = params['cod_modalidade']['operacoes'][nome_operacao]

        dth.popula_dados_operacao(
            cod_operacao=cd_op,
            nome_operacao=nome_operacao,
            base_origem=replica
        )

    # Gerando o procedimento para gerar uma query capaz de unir todas as tabelas produzidas até entãoo
    # query_book_final é a query que será populada proceduralmente
    # o primeiro loop aninhado é para gerar as colunas da cláusula select
    # o segundo loop for simples é pra gerar os joins
    # To do: Repensar a query focando performance
    query_book_final = f"""SELECT 
    book_scr.chave_cpf
    ,book_scr.data_consulta_dado_bacen
    """
    for nome_operacao in params['cod_modalidade']['operacoes']:
        for var in params['num_cols']:
             join_select = f""",COALESCE({nome_operacao}_{var},0) as {nome_operacao}_{var}
             """
             query_book_final = f"{query_book_final} {join_select}"
    query_book_final = f"{query_book_final} from book_scr"

    for nome_operacao in params['cod_modalidade']['operacoes']:
        query_book_final =  dth._generate_query_procedure(nome_operacao, query_book_final)
    
    # A query final pode ser lida via print statement abaixo
    # print(query_book_final)

    # Cria uma nova tabela com os dados agregados via join
    dth.escreve_tabela(query_book_final,'book_scr_final')

    # Criação de variáveis extras
    # gerando de maneira procedural a query
    query = """select 
        book.* 
        """
    for nome_operacao in params['cod_modalidade']['operacoes']:

            join_query = f""",{nome_operacao}_valor_credito_vencer_ate_30_dia
                + {nome_operacao}_valor_credito_vencer_31_60_dia 
            as {nome_operacao}_valor_credito_vencer_ate_60_dia
            ,{nome_operacao}_valor_credito_vencer_ate_30_dia
                + {nome_operacao}_valor_credito_vencer_31_60_dia 
                + {nome_operacao}_valor_credito_vencer_61_90_dia 
            as {nome_operacao}_valor_credito_vencer_ate_90_dia
            ,{nome_operacao}_valor_credito_vencer_ate_30_dia
                + {nome_operacao}_valor_credito_vencer_31_60_dia 
                + {nome_operacao}_valor_credito_vencer_61_90_dia
                + {nome_operacao}_valor_credito_vencer_acima_90_dia 
            as {nome_operacao}_valor_credito_vencer_total_dia
            ,{nome_operacao}_valor_credito_vencido_15_30_dia
                + {nome_operacao}_valor_credito_vencido_31_60_dia 
            as {nome_operacao}_valor_credito_vencido_ate_60_dia
            ,{nome_operacao}_valor_credito_vencido_15_30_dia
                + {nome_operacao}_valor_credito_vencido_31_60_dia 
                + {nome_operacao}_valor_credito_vencido_61_90_dia 
            as {nome_operacao}_valor_credito_vencido_ate_90_dia
            ,{nome_operacao}_valor_credito_vencido_15_30_dia
                + {nome_operacao}_valor_credito_vencido_31_60_dia 
                + {nome_operacao}_valor_credito_vencido_61_90_dia
                + {nome_operacao}_valor_credito_vencido_acima_90_dia  
            as {nome_operacao}_valor_credito_vencido_total_dia        
            """

            query = f"{query} {join_query}"
    
    query_extra_vars = f"{query} FROM BOOK_SCR_FINAL as book"
    
    # print(query)
    dth.escreve_tabela(query_extra_vars, 'feat_store_scr')