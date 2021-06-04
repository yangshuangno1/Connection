import gspread
import json
import pandas as pd
import numpy as np
import schedule
import time
gc=gspread.service_account(filename='chiphiez.json')
sh=gc.open_by_key('1pJssovmuxPit3XIqyr9VNZdJdyyfRTKfikrMiD37d9s')
worksheet=sh.sheet1
#res=worksheet.get_all_values()
#res=worksheet.get_all_records(A:T)
#res=worksheet.col_values(1)
#res=worksheet.get('A:T')
#user=["Susan",25,"Sydney"]
#worksheet.insert_row(user)
#worksheet.update_cell(6,2,28)
#worksheet.delete_row(1)
worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())

print(dataframe)