from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
import os


from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class CurrencyScoopHook(BaseHook):

    def __init__(self, currency_conn_id: str):
        super().__init__()
        self.conn_id = currency_conn_id

    def get_rate(self, date, base_currency: str, currency: str):
        url = 'https://api.currencyscoop.com/v1/historical'
        params = {
            'base': base_currency.upper(),
            'symbols': currency.upper(),
            'api_key': self._get_api_key(),
            'date': str(date),
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()['response']['rates'][currency]

    def _get_api_key(self):
        conn = self.get_connection(self.conn_id)
        if not conn.password:
            raise AirflowException('Missing API key (password) in connection settings')
        return conn.password



class CurrencyScoopOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            base_currency: str,
            currency: str,
            conn_id: str = 'cur_scoop_conn_id',
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.base_currency = base_currency
        self.currency = currency

    def execute(self, context: Any):
        api = CurrencyScoopHook(self.conn_id)
        return api.get_rate(context['execution_date'].date(), self.base_currency, self.currency)


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
csv_file = 'file.csv'



def create_file_from_df():
    d= {}
    base_currency = '{{ params.base_currency }}'
    currency = '{{ params.currency }}'
    date = '{{ execution_date.strftime("%Y-%m-%d") }}'
    rate = {{ ti.xcom_pull(task_ids="get_rate") }}

    d['base'] = base_currency
    d['currency'] = currency
    d['date'] = date
    d['rate'] = rate
    df = pd.DataFrame(d)
    return df.to_csv('file.csv')



def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)


with DAG(
        dag_id='dag',
        start_date=datetime(2021, 3, 1),
        schedule_interval='@daily',
) as dag:

    get_rate = CurrencyScoopOperator(
        task_id='get_rate',
        base_currency='USD',
        currency='BYN',
        conn_id='cur_scoop_conn_id',
        dag=dag,
        do_xcom_push=True,
    )

    create_file_from_df = PythonOperator(
        task_id="create_file_from_df",
        python_callable=create_file_from_df,
        op_kwargs={
            "response": f"{path_to_local_home}/{csv_file}",
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{csv_file}",
            "local_file": f"{path_to_local_home}/{csv_file}",
        }
    )

    get_rate >> create_file_from_df >> local_to_gcs_task
