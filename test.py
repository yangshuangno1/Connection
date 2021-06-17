from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
from google.cloud import bigquery
import pandas as pd
import pytz
import requests

def du_kien9():
    credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\pc\\Desktop\\project\\corded-sunbeam-296904-d825b50b19e5.json')

    project_id = 'corded-sunbeam-296904'
    client = bigquery.Client(credentials= credentials,project=project_id)
    res = requests.post('http://35.198.227.133:3000/api/card/49/query/json', 
                      headers = {'Content-Type': 'application/json',
                                'X-Metabase-Session': "54cbf9e7-de3e-4fb7-9eb5-4ab02368eb57"
                                }
                    )
    r=res.json()
    df=pd.DataFrame(r)
    table_id = "corded-sunbeam-296904.ezjob.du_kien9"
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("ngaylam", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("chinhanh", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("quanhuyen", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("tinhthanh", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("calam", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("slcantuyen", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("sldaduyet", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("tientamtinh", bigquery.enums.SqlTypeNames.INTEGER)
        ],

        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id,job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.
    print("success")

def new_user12():
    credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\pc\\Desktop\\project\\corded-sunbeam-296904-d825b50b19e5.json')

    project_id = 'corded-sunbeam-296904'
    client = bigquery.Client(credentials= credentials,project=project_id)
    res = requests.post('http://35.198.227.133:3000/api/card/12/query/json', 
                      headers = {'Content-Type': 'application/json',
                                'X-Metabase-Session': "54cbf9e7-de3e-4fb7-9eb5-4ab02368eb57"
                                }
                    )
    r=res.json()
    df=pd.DataFrame(r)
    table_id = "corded-sunbeam-296904.ezjob.new_user12"
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("timecoviec", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("ngaylam", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("thang", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hoten", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("tuan", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("yesno", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("khuvuc", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ngaythamgia", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("sdt", bigquery.enums.SqlTypeNames.STRING)
        ],

        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id,job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.
    print("success")

def phanbo_user15():
    credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\pc\\Desktop\\project\\corded-sunbeam-296904-d825b50b19e5.json')

    project_id = 'corded-sunbeam-296904'
    client = bigquery.Client(credentials= credentials,project=project_id)
    res = requests.post('http://35.198.227.133:3000/api/card/15/query/json', 
                      headers = {'Content-Type': 'application/json',
                                'X-Metabase-Session': "54cbf9e7-de3e-4fb7-9eb5-4ab02368eb57"
                                }
                    )
    r=res.json()
    df=pd.DataFrame(r)
    table_id = "corded-sunbeam-296904.ezjob.phanbo_user15"
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("tuoi", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("ngaylam", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("thang", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("namsinh", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hoten", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("gioitinh", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("tuan", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ca", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("khuvuc", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("dotuoi", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("sdt", bigquery.enums.SqlTypeNames.STRING)
        ],

        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id,job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.
    print("success")

def datahcm():
    credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\pc\\Desktop\\project\\corded-sunbeam-296904-d825b50b19e5.json')

    project_id = 'corded-sunbeam-296904'
    client = bigquery.Client(credentials= credentials,project=project_id)
    gc=gspread.service_account(filename='corded-sunbeam-296904-84fe10fbe65b.json')
    sh=gc.open_by_key('1I6MWhYLk6ZVFXr-r69XatIu3Vt0dUxh3I3Bvs4Yw-80')
    worksheet=sh.sheet1    
    df=pd.DataFrame(worksheet.get_all_records())
    table_id = "corded-sunbeam-296904.ezjob.datahcm"
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("ngaylam", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("khuvuc", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("phongban", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("kho", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("chinhanh", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("congviec", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("phanloaifl", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("phanloaica", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("giovao", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("giora", bigquery.enums.SqlTypeNames.STRING,mode='NULLABLE'),
            bigquery.SchemaField("yeucau", bigquery.enums.SqlTypeNames.FLOAT,mode='NULLABLE'),
            bigquery.SchemaField("ezjob", bigquery.enums.SqlTypeNames.FLOAT,mode='NULLABLE'),
            bigquery.SchemaField("thucte", bigquery.enums.SqlTypeNames.FLOAT,mode='NULLABLE'),
            bigquery.SchemaField("doitac", bigquery.enums.SqlTypeNames.FLOAT,mode='NULLABLE'),
        ],

        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id,job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.


