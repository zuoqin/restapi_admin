#!/usr/bin/python
import psycopg2
# note that we have to import the Psycopg2 extras library!
import psycopg2.extras
import pandas as pd
from datetime import date, datetime, timedelta
conn_string = "host='37.18.75.197' port=5432 dbname='omnia' user='postgres' password='Qwerty123'"

conn = psycopg2.connect(conn_string)
cur = conn.cursor()


sql = 'select id from accounts_customuser'

df = pd.read_sql(sql, con=conn)
for index, row in df.iterrows():
    dt = datetime.now().replace(hour=0, minute=0,second=0,microsecond=0)
    sql = 'SET datestyle = "DMY";\n'
    sql += 'select count(user_id) as cnt from user_actions where time > \'' + dt.strftime('%d.%m.%Y %H:%M:%S') +\
           '\' and user_id=' + str(row['id'])
    df_cnt = pd.read_sql(sql, con=conn)
    sql = 'update accounts_customuser set requests_per_day =' + str(df_cnt['cnt'][0]) + ' where id = ' +\
        str(row['id'])
    cur.execute(sql)

    dt = dt + timedelta(days=-7)
    sql = 'select count(user_id) as cnt from user_actions where time > \'' + dt.strftime('%d.%m.%Y %H:%M:%S') +\
           '\' and user_id=' + str(row['id'])
    df_cnt = pd.read_sql(sql, con=conn)
    sql = 'update accounts_customuser set requests_per_week =' + str(df_cnt['cnt'][0]) + ' where id = ' +\
        str(row['id'])
    cur.execute(sql)

    dt = dt + timedelta(days=-30)
    sql = 'select count(user_id) as cnt from user_actions where time > \'' + dt.strftime('%d.%m.%Y %H:%M:%S') +\
           '\' and user_id=' + str(row['id'])
    df_cnt = pd.read_sql(sql, con=conn)
    sql = 'update accounts_customuser set requests_per_month =' + str(df_cnt['cnt'][0]) + ' where id = ' +\
        str(row['id'])
    cur.execute(sql)

    sql = 'select count(user_id) as cnt from user_actions where' +\
           ' user_id=' + str(row['id'])
    df_cnt = pd.read_sql(sql, con=conn)
    sql = 'update accounts_customuser set requests_total =' + str(df_cnt['cnt'][0]) + ' where id = ' +\
        str(row['id'])
    cur.execute(sql)
conn.commit()
cur.close()