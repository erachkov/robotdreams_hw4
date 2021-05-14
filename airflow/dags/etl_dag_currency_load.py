import os
from datetime import datetime
from airflow import DAG
import airflow.hooks.postgres_hook
import logging
from tempfile import NamedTemporaryFile
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from operators.currency_http import CurrencyHttpOperator

# Change these to your identifiers, if needed.
POSTGRES_CONN_ID = "postgres_robot_dreams"
HTTP_ROBOT_DREAMS_API = "http_robot_dreams_data_api"
BASE_DIR = '/usr/local/airflow/data'


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(dag_id='LOAD_TO_FILE',
    description='LOAD_TO_FILE',
    schedule_interval=None,
    start_date=datetime(2021, 2, 22),
    default_args=default_args
)

get_out_of_stock = CurrencyHttpOperator(
    task_id="get_out_of_stock",
    http_conn_id=HTTP_ROBOT_DREAMS_API,
    endpoint="/out_of_stock",
    endpoint_auth='/auth',
    xcom_push=True,
    path=f'{BASE_DIR}/data',
    dag=dag
)


def copy_table_to_file(**kwargs):
    _table_name = kwargs['table_name']
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    dir_csv_file = os.path.join(BASE_DIR, 'database')
    os.makedirs(dir_csv_file, exist_ok=True)
    csv_file =  os.path.join(dir_csv_file, f'{_table_name}.csv')
    with open(csv_file, mode='w') as file:
        logging.info("Exporting table to csv file '%s'", file.name)
        pg_hook.copy_expert(f"COPY (SELECT * FROM {_table_name})  TO STDOUT WITH HEADER CSV", filename=csv_file)



table_lists = ['actor', 'address', 'category', 'city', 'country', 'customer', 'film', 'film_actor', 'film_category',
               'inventory', 'language', 'payment', 'rental', 'staff', 'store']

for table_name in table_lists:
    copy_to_gcs_task = PythonOperator(
        task_id=f"copy_table_{table_name}_to_file",
        python_callable=copy_table_to_file,
        op_kwargs={"table_name": table_name},
        dag=dag,
    )
    get_out_of_stock >> copy_to_gcs_task

get_out_of_stock
