import pandas as pd
import requests
from openpyxl.workbook import Workbook

res = requests.get('http://35.198.227.133:3000/api/table', 
              headers = {'Content-Type': 'application/json',
                        'X-Metabase-Session': "b477afad-7fcd-470a-91e2-9a858082d231"
                        }
            )

r=res.json()
print(r)
