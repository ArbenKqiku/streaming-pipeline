import io
import json
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import bigquery # pip install google-cloud-bigquery and pyarrow as a dependency
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    '/home/arbenkqiku/streaming-pipeline/mage-zoomcamp/streaming-pipeline-418713-7f7d915b1fc7.json', scopes=['https://www.googleapis.com/auth/cloud-platform'],
)

df = pd.read_csv("company_house_core_clean.csv")

client = bigquery.Client(project=credentials.project_id, credentials=credentials)

job_config = bigquery.LoadJobConfig()

table_name = 'company_house_core'

table_id = '{0}.{1}.{2}'.format(credentials.project_id, "company_house", table_name)
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

# Upload new set incrementally:
# ! This method requires pyarrow to be installed:
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)