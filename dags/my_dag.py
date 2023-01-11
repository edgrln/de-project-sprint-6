from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.models.variable import Variable


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
import logging



AWS_ACCESS_KEY_ID =  Variable.get("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY") 


task_logger = logging.getLogger(__name__)


def fetch_s3_file(bucket: str, key: str) -> str:    
    session = boto3.session.Session()
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


bash_command_tmpl = """
wc -l {{ params.files }}
"""

bucket_files = ('group_log.csv',)


@dag(schedule_interval='0 0 * * *', start_date=pendulum.parse('2022-09-17'),catchup=False)
def sprint6_project_dag_get_data():


    # Шаг 1. Загрузить данные из S3   
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs = {'bucket': 'sprint6', 'key': key},
        ) for key in bucket_files
    ]

    print_count_lines_of_each = BashOperator(
        task_id = 'print_10_lines_of_each',
        bash_command = bash_command_tmpl,
        params = {'files': " ".join(f'/data/{f}' for f in bucket_files)}
    )

    # Шаг 3. Загрузить данные в Vertica

    load_group_log = VerticaOperator(
        task_id='load_groups',
        vertica_conn_id='vertica_default',
        sql='''TRUNCATE TABLE LAKSHINEDYANDEXRU__STAGING.group_log;
        COPY LAKSHINEDYANDEXRU__STAGING.group_log(group_id, user_id, user_id_from, event, event_dt) FROM LOCAL '/data/group_log.csv' DELIMITER ',';''')

    fetch_tasks >> print_count_lines_of_each >> load_group_log

sprint6_project_dag_get_data = sprint6_project_dag_get_data()