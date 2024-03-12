from pathlib import Path
import os   


DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt" 
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH)) 

print(DEFAULT_DBT_ROOT_PATH)
print(DBT_ROOT_PATH)