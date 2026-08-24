from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 's25321830', 
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='sprint_13_project_dolgovyazov_sergei',
    default_args=default_args,
    description='Обновление витрин Data Lake',
    schedule_interval='@daily',
    catchup=False,
    tags=['data_lake', 'user_mart', 'zones_mart', 'friends_mart']
) as dag:

    base_path = '/user/s25321830'
    scripts_path = f'{base_path}/scripts'
    
    task_user_mart = BashOperator(
        task_id='user_mart',
        bash_command=f'python {scripts_path}/user_mart.py',
    )

    task_zones_mart = BashOperator(
        task_id='zones_mart',
        bash_command=f'python {scripts_path}/zones_mart.py',
    )

    task_friends_mart = BashOperator(
        task_id='friends_mart',
        bash_command=f'python {scripts_path}/friends_mart.py',
    )

    task_user_mart >> task_zones_mart >> task_friends_mart