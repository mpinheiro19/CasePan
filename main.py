from db.db import SQLiteSingleton
from data_handler.data_handler import DataHandler
import yaml

# Open and read the YAML file
with open('global_params.yaml', 'r') as file:
    params = yaml.safe_load(file)


dbfile = params['db_file']['path']
db = SQLiteSingleton(dbfile)


## To Do:

# ***OK*** 1- Normalização das features NaN 
# ***OK*** 2- Criação da classe DataHandler
# 3- Feature Eng
    # 3.0 - Normalizar a tabela (chave unica é cpf,mes_consulta,cod_modalidade)
    # 3.1.1 - Criação do saldo por modadalidade (colunarizacao da feat)
    # 3.1.2 - Criacao do saldo em janelas (requisito)
    # 3.2 - Possui Saldo Vencido
    # 3.3 - Saldo total a vencer
    # 3.4 - Saldo total vencido
    # 3.5 - Perc Saldo vencido acima 90d
    # 4.6 - Perc saldo a vencer acima de 90d
# 4- PosProcess
#     4.1 - Garantir que cada linha é um CPF/dataconsulta
#     4.2 - método para exportar
# 5- Recomendações extras de next steps

if __name__ == '__main__':

    # instancia a classe de data handler que herda a conexão com db
    dth = DataHandler(db)

    # Replica a tabela original para melhor track de modificações
    # (set opcional, feito apenas para manter histórico de modificações em
    # ambientes distintos!)
    orig_table = params['db_file']['original_table']
    replica = params['db_file']['replica_table']

    dth.replicate_table(orig_table, replica)

    # implementa o método de preencher os valores nan encontrados na exploratória com
    # o 'wildcard value' de 0 em str seguindo o schema encontrado na tabela scr
    # Note que não foi possível encontrar um padrão mais adequado, portanto vou completar com 0
    dth.fill_nan(
        replica,
        'valor_credito_vencido_15_30_dia',
        '0'
    )
    
    # Nesse bloco simplesmente armazenamos uma lista com o nome das colunas numericas a serem realiados calculos
    df_col = []
    prefix = params['num_cols']['prefix']
    for i in params['num_cols']['sufix']:
        df_col.append(prefix + i)