from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator, PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG('postgres_to_gcs_etl',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    
    schema_name = 'formula1'

    def get_all_tables_in_schema():
        postgres_hook = PostgresHook(postgres_conn_id = 'postgres_con')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        query = f""" SELECT table_name,
                    FROM information_schema.tables
                    WHERE table_schema = {schema_name}"""
        cursor.execute(query)
        tables = cursor.fetchall()
        tables_list = [table[0] for table in tables]
        cursor.close()
        conn.close()
        return tables_list

    export_postgres_to_gcs = PostgresToGCSOperator(
        task_id='export_postgres_to_gcs',
        sql='SELECT * FROM formula1.drivers',  # Your SQL query here
        bucket='pl-postgres-bq-raw',  # Your GCS bucket here
        filename='formula1/drivers.csv',  # The destination file path in GCS
        postgres_conn_id='postgres_conn',  # Your Postgres connection ID
        export_format='csv',  # The format for the exported data
        gcp_conn_id = 'gcs_conn',
    )

    list_tables = PythonOperator(
        task_id = 'list_tables',
        python_callable=get_all_tables_in_schema,
        dag=dag,
    )

    list_tables > export_postgres_to_gcs
