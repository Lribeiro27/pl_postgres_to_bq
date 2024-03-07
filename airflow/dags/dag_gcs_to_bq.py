from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG('dag_gcs_to_bq',
         schedule_interval='@daily',
         default_args=default_args) as dag:
    
    task_get_files_in_bucket = GCSListObjectsOperator(
        task_id='task_get_files_in_bucket',
        bucket='pl-postgres-bq-raw',
        prefix='formula1/',
        gcp_conn_id='gcs_conn',
    )

    files = ['circuits',
             'constructor_results',
             'constructor_standings',
             'constructors',
             'driver_standings',
             'drivers',
             'lap_times',
             'pit_stops',
             'qualifying',
             'races',
             'results',
             'seasons',
             'sprint_results',
             'status']
    
    transfer_tasks = [] 

    for file in files:
        transfer_gcs_to_bigquery = GCSToBigQueryOperator(
            task_id=f'transfer_{file}_to_bigquery',
            bucket='pl-postgres-bq-raw',
            source_objects=[f'{file}.csv'],  
            destination_project_dataset_table=f'dbt-specialization.bronze.{file}',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            gcp_conn_id='gcs_conn',
            field_delimiter=',',
            skip_leading_rows=1,
            max_bad_records=10,
            allow_quoted_newlines=True,
        )
        transfer_tasks.append(transfer_gcs_to_bigquery)  
        
    task_get_files_in_bucket >> transfer_tasks
