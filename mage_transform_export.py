from typing import Dict, List
import pandas as pd
import pyarrow as pa
from google.cloud import bigquery
from google.oauth2 import service_account

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(messages: List[Dict], *args, **kwargs):

    # define container for incoming messages
    incoming_messages = []

    counter = 1
    for msg in messages:
        # print(counter)
        counter += 1
        # print(msg)

        # append each message
        incoming_messages.append(msg)
    
    # turn into a pandas data frame
    df = pd.DataFrame(incoming_messages)

    # convert string columns to date or date time
    df['date_of_creation'] = pd.to_datetime(df['date_of_creation'])
    df['published_at'] = pd.to_datetime(df['published_at'])

    # define credentials
    credentials = service_account.Credentials.from_service_account_file(
        '/home/src/kafka-streaming-418018-22f81afd78ce.json', scopes=['https://www.googleapis.com/auth/cloud-platform'],
    )

    # define client
    client = bigquery.Client(project=credentials.project_id, credentials=credentials)

    # define job
    job_config = bigquery.LoadJobConfig()

    # define table name and big query details
    table_name = 'company_house_stream'
    table_id = '{0}.{1}.{2}'.format(credentials.project_id, "company_house", table_name)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    print("BLOCK 3.1")

    print(df)

    # Upload new set incrementally:
    # ! This method requires pyarrow to be installed:
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    print(df.head())

    return df