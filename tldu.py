import gspread
import json
import pandas as pd
import numpy as np
import schedule
import time
import psycopg2
import datetime
import schedule
import time
gc=gspread.service_account(filename='test123.json')
sh=gc.open_by_key('12MtpVgHWGSI1WJb-EgfEeAnDW9bi5YNliZIh5wnvFgE')
worksheet=sh.sheet1
def myfunction():

    conn = psycopg2.connect(
        host="35.247.185.45",
        database="ezjob",
        user="lamtnb",
        password="Abc123")
    cur = conn.cursor()
    cur.execute("SELECT * from tldu ")
    rows = cur.fetchall()
    df=pd.DataFrame(rows, columns=['Ngày Làm ', 'Khu Vực ', 'Phòng Ban','Mã Kho','Chi Nhánh','Công Việc','Phân loại FL','Phân Loại Ca',
        'Ca Làm','Yêu Cầu','Ezjob','Thực tế','Đối Tác','Off','TLDU Dự Kiến','TLDU Thực Tế','TLDU Organic'])
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    print("sucess")
myfunction()
schedule.every().day.at("10:30").do(myfunction)
while True:
    schedule.run_pending()
    time.sleep(1) 


