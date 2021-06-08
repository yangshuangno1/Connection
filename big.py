import pandas as pd
from pandas.io import gbq
import requests
import schedule
import time

def du_kien9():
	res = requests.post('http://35.198.227.133:3000/api/card/49/query/json', 
	              headers = {'Content-Type': 'application/json',
	                        'X-Metabase-Session': "54cbf9e7-de3e-4fb7-9eb5-4ab02368eb57"
	                        }
	            )
	r=res.json()
	m=pd.DataFrame(r)
	m.to_gbq(destination_table='ezjob_test.du_kien9',
		project_id='just-landing-314007',
		if_exists='replace')
	print("Success")

def new_user12():
	res = requests.post('http://35.198.227.133:3000/api/card/12/query/json', 
              		headers = {'Content-Type': 'application/json',
                        	'X-Metabase-Session': "54cbf9e7-de3e-4fb7-9eb5-4ab02368eb57"
                        	}
            	)
	r=res.json()
	m=pd.DataFrame(r)
	m.to_gbq(destination_table='ezjob_test.new_user12',
		project_id='just-landing-314007',
		if_exists='replace')
	print("Success")

def phanbo_user15():
	res = requests.post('http://35.198.227.133:3000/api/card/15/query/json', 
              		headers = {'Content-Type': 'application/json',
                        	'X-Metabase-Session': "54cbf9e7-de3e-4fb7-9eb5-4ab02368eb57"
                        	}
            	)
	r=res.json()
	m=pd.DataFrame(r)
	m.to_gbq(destination_table='ezjob_test.phanbo_user15',
		project_id='just-landing-314007',
		if_exists='replace')
	print("Success")

du_kien9()
new_user12()
phanbo_user15()

schedule.every().day.at("10:30").do(du_kien9)
while True:
    schedule.run_pending()
    time.sleep(1)

schedule.every().day.at("10:30").do(new_user12)
while True:
    schedule.run_pending()
    time.sleep(1) 
    
schedule.every().day.at("10:30").do(phanbo_user15)
while True:
    schedule.run_pending()
    time.sleep(1) 
