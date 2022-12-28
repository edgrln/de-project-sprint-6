from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag

import boto3
import logging
import pendulum
import contextlib
import hashlib
import json
import vertica_python
import pandas as pd
from typing import Dict, List, Optional


AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

logger = logging.getlogger("airflow.task")


def fetch_s3_file(bucket: str, key: str) -> str:
    sessions = boto3.session.Session()
    s3_client = session.client(
        service_name = 's3',
        endpoint_url = 'https://storage.yandexcloud.net',
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len (df)
    vertica_conn = vertica_python.connect(
        host='localhost',
        port=5433,
        user='dbadmin',
    )
    columns = ', '.join(columns)
    copy_expr = f """
    COPY {schema}.{table} ({columns}) FROM STDIM DELIMITER ',' ENCLOSED BY ''''
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            logger.info(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmpchunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            logger.info("loaded")
            start += chunk_size + 1
    
    vertica_conn.close()


bash_command_tmpl = """
head {{ files }}
"""

@dag(schedule_interval='0 0 * * *', start_date=pendulum.parse('2022-09-17'),catchup=False)
def sprint6_project_dag_get_data():


    # Шаг 1. Загрузить данные из S3   
    buket_files = ('group_log.csv')
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs = {'bucket': 'sprint6', 'key': key},
        ) for key in bucket_files
    ]

    print_10_lines_of_each = BashOperator(
        task_id = 'print_10_lines_of_each',
        bash_command = bash_command_tmpl,
        params = {'files': " ".join(f'/data/{f}' for f in bucket_files)}
    )
    # Шаг 3. Загрузить данные в Vertica
    load_group_log = PythonOperator(
        task_id = 'load_group_log',
        python_callable = load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',
            'schema': 'staging',
            'table': 'group_log',
            'columns': ['group', 'user_id', 'user_id_from', 'event', 'event_dt']

        }
    )

    fetch_tasks >> print_10_lines_of_each >> load_group_log

sprint6_project_dag_get_data = sprint6_project_dag_get_data()