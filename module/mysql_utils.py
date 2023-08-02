import os
import pymysql
import pandas as pd

db_info = {
  'host' : 'ENV_FILE',
  'port' : 3306,
  'user' : 'ENV_FILE',
  'passwd' : 'ENV_FILE',
  'schema' : 'ENV_FILE'
}

def get_db_conn():
    return pymysql.connect(host=db_info['host'],
                           user=db_info['user'],
                           port=db_info['port'],
                           passwd=db_info['passwd'],
                           db=db_info['schema'],
                           charset="utf8")

def write_db( db_conn, sql ):
    cursor = db_conn.cursor()
    rows = cursor.execute(sql)
    db_conn.commit()
    return rows


def execute_db( db_conn, sql ):
    df = pd.read_sql(sql, db_conn)
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df

    return pd.DataFrame()