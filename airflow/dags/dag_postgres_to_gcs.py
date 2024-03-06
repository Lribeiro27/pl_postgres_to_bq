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
    tables = ['circuits', 'status', 'lap_times', 'sprint_results', 'drivers', 'races', 'constructors', 'constructor_standings', 'qualifying', 'driver_standings', 'constructor_results', 'pit_stops', 'seasons', 'results']
    
    def get_all_tables_in_schema():
        postgres_hook = PostgresHook(postgres_conn_id = 'postgres_conn')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        query = f""" SELECT table_name
                     FROM information_schema.tables
                     WHERE table_schema = '{schema_name}'"""
        cursor.execute(query)
        tables = cursor.fetchall()
        #tables_list = [table[0] for table in tables]
        #print(tables_list)
        cursor.close()
        conn.close()
        #return tables_list
    
    list_tables = PythonOperator(
        task_id = 'list_tables',
        python_callable=get_all_tables_in_schema,
        dag=dag,
    )
    
    for table in tables:
        export_postgres_to_gcs = PostgresToGCSOperator(
            task_id= f'export_table_{table}__to_gcs',
            sql=f'SELECT * FROM {schema_name}.{table}',  # Your SQL query here
            bucket='pl-postgres-bq-raw',  # Your GCS bucket here
            filename=f'{schema_name}/{table}.csv',  # The destination file path in GCS
            postgres_conn_id='postgres_conn',  
            export_format='csv',  
            gcp_conn_id = 'gcs_conn',
        )


    #list_tables >> export_postgres_to_gcs
