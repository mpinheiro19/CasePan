from db.db import SQLiteSingleton
dbfile = "db/base_bacen.sqlite"


db = SQLiteSingleton(dbfile)
df = db.read_dataframe("scr")
print(df.head())


## To Do:

# 1- Normalização das features NaN
# 2- Criação da classe DataHandler
# 3- Feature Eng
#     3.1 - 
# 4- PosProcess
#     4.1 - Garantir que cada linha é um CPF/dataconsulta
#     4.2 - método para exportar
# 5- Recomendações extras de next steps