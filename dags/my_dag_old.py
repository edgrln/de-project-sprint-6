from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
from airflow.operators.dummy_operator import DummyOperator
#from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.providers.vertica.operators.vertica import VerticaOperator


import boto3

def fetch_s3_file(bucket: str, key: str):
    # сюда поместить код из скрипта для скачивания файла

    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла
bash_command_tmpl = """
echo {{ params.files }}; head -10 {{ params.files }}
"""

@dag(schedule_interval='0 0 * * *', start_date=pendulum.parse('2022-09-17'),catchup=False)
def sprint6_dag_get_data():
    bucket_files = ['groups.csv','users.csv','dialogs.csv']
    
    task1 = PythonOperator(
        task_id=f'fetch_groups.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups.csv'},
    )

    task2 = PythonOperator(
        task_id=f'fetch_users.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'users.csv'},
    )
    
    task3 = PythonOperator(
        task_id=f'fetch_dialogs.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs.csv'},
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
    #   params={'files': [f'/data/{f}' for f in bucket_files]}
        params={'files': ' '.join([f"/data/{f}" for f in bucket_files])}
    )

    start = DummyOperator(task_id="start", trigger_rule="all_success",)
    end = DummyOperator(task_id="end", trigger_rule="all_success",)

    load_users = VerticaOperator(
        task_id='load_users',
        vertica_conn_id='vertica_default',
        sql='''COPY LAKSHINEDYANDEXRU__STAGING.users (id,
                                        chat_name, 
                                        registration_dt, 
                                        country, 
                                        age )
                FROM LOCAL '/data/users.csv'
                DELIMITER ','
                ENCLOSED BY '"';
                --REJECTED DATA AS TABLE LAKSHINEDYANDEXRU__STAGING.users_rej;'''
                )

    load_dialogs = VerticaOperator(
        task_id='load_dialogs',
        vertica_conn_id='vertica_default',
        sql='''COPY LAKSHINEDYANDEXRU__STAGING.dialogs (message_id, 
                                                        message_ts, 
                                                        message_from, 
                                                        message_to, 
                                                        message, 
                                                        message_group)
                FROM LOCAL '/data/dialogs.csv'
                DELIMITER ','
                ENCLOSED BY '"';
                --REJECTED DATA AS TABLE LAKSHINEDYANDEXRU__STAGING.dialogs_rej;'''
                )

    load_groups = VerticaOperator(
    task_id='load_groups',
    vertica_conn_id='vertica_default',
    sql='''COPY LAKSHINEDYANDEXRU__STAGING.groups (id, 
            admin_id, 
            group_name, 
            registration_dt, 
            is_private )
            FROM LOCAL '/data/groups.csv'
            DELIMITER ','
            ENCLOSED BY '"';
            --REJECTED DATA AS TABLE LAKSHINEDYANDEXRU__STAGING.groups_rej;'''
            )



    [task1, task2, task3] >> print_10_lines_of_each >> start >> [load_users, load_dialogs, load_groups] >> end

sprint6_dag_get_data = sprint6_dag_get_data()


