from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,1,1),
    'catchup': False,
}

def show_bucket_objects(ti):
    gcs_files_list = ti.xcom_pull(task_ids='task_list_objects')
    [print(file_name) for file_name in gcs_files_list]


with DAG('dag_list_bucket',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    
    task_list_objects = GCSListObjectsOperator(
        task_id = 'task_list_objects',
        bucket = 'pl-postgres-bq-raw',
        prefix = 'formula1/',
        gcp_conn_id = 'gcs_conn',
    )

    task_print_objects = PythonOperator(
        task_id = 'task_print_objects',
        python_callable=show_bucket_objects,
    )

    task_list_objects >> task_print_objects
