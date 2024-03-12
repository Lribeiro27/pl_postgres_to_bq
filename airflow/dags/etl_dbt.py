
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import os

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import (
    DbtDag,
    ProfileConfig,
    ProjectConfig,
    ExecutionConfig
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
    "catchup": False,
}

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt" 
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH)) 

profile_config = ProfileConfig(
    profile_name="data_enrichment",
    target_name="dev",
    profiles_yml_filepath=DBT_ROOT_PATH / "profiles.yml"
)

dag_dbt_cosmos = DbtDag(
    project_config=ProjectConfig( 
         DBT_ROOT_PATH / "data_enrichment", 
     ), 
    profile_config=profile_config,
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },

    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2024, 2, 23),
    catchup=False,
    dag_id="dag_dbt_cosmos",
    default_args={"retries": 2},
)
