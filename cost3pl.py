import psycopg2
from configparser import ConfigParser
import pandas as pd
import gspread
import json
import psycopg2.extras
import psycopg2.errorcodes
import schedule
import time

#connect db
conn = psycopg2.connect(
    host="35.247.185.45",
    database="ezjob",
    user="lamtnb",
    password="Abc123")
cur = conn.cursor()

#connect ggsheet
gc=gspread.service_account(filename='cost3pl.json')
sh=gc.open_by_key('1ujDL1fEQhRs0LACWdBpcSG4CvBqgeBSS5euSM7HQwSc')
worksheet=sh.sheet1
df = pd.DataFrame(worksheet.get_all_records())
# df = worksheet.get_all_values()

#update from df to postgre
def fcn(df,table,cur):
    if len(df) > 0:
        df_columns = dict(df)
        
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

        #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table,columns,values)
        cur.execute("truncate " + table + ";")  # avoiding uploading duplicate data!
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
        print('success')
    conn.commit()
fcn(df,'cost3pl',cur)
schedule.every().day.at("10:30").do(fcn,df,'cost3pl',cur)
while True:
    schedule.run_pending()
    time.sleep(1)   




