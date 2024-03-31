import io
import json
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import bigquery # pip install google-cloud-bigquery and pyarrow as a dependency
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    '/home/arbenkqiku/streaming-pipeline/streaming-pipeline-418713-7f7d915b1fc7.json', scopes=['https://www.googleapis.com/auth/cloud-platform'],
)

client = bigquery.Client(project=credentials.project_id, credentials=credentials)

job_config = bigquery.LoadJobConfig()

json_example = {'company_name': 'CONSULTANCY, PROJECT AND INTERIM MANAGEMENT SERVICES LTD', 'company_number': '13255037', 'company_status': 'active', 'date_of_creation': '2021-03-09', 'postal_code': 'PE6 0RP', 'published_at': '2024-03-23T18:37:03'}
df = pd.DataFrame([json_example])

df['date_of_creation'] = pd.to_datetime(df['date_of_creation'])
df['published_at'] = pd.to_datetime(df['published_at'])

table_name = 'company_house_stream'

table_id = '{0}.{1}.{2}'.format(credentials.project_id, "company_house", table_name)
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

# Upload new set incrementally:
# ! This method requires pyarrow to be installed:
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)