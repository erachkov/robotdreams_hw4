from datetime import datetime
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator

from operators.currency_http import CurrencyHttpOperator

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    'currency_complexhttp_dag',
    description='currency complex http dag',
    schedule_interval=None,
    start_date=datetime(2021, 2, 22),
    default_args=default_args
)


get_out_of_stock = CurrencyHttpOperator(
        task_id="get_out_of_stock",
        http_conn_id="http_robot_dreams_data_api",
        endpoint="/out_of_stock",
        endpoint_auth='/auth',
        xcom_push=True,
        path='/usr/local/airflow/data/currencies.json',
        dag=dag
    )

get_out_of_stock
