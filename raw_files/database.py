import pandas as pd
import sqlite3

def connect():
    conn = sqlite3.connect('base_bacen.sqlite')
    return conn

def query(conn, sql_query):
    try:
        df = pd.read_sql_query(sql_query, con=conn)
    except TypeError:
        df = None

    return df


conn = connect()
print(conn)

# Exemplo de query
#query(conn, 'select * from scr limit 10')

#df = pd.read_sql_query("SELECT * FROM scr LIMIT 10", conn)