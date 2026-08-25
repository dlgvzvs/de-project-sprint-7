from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 's25321830', 
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}

dag = DAG(
    dag_id='sprint_13_project_dolgovyazov_sergei',
    default_args=default_args,
    description='Обновление витрин Data Lake',
    schedule_interval='@daily',
    catchup=False
    )

task_users_mart = SparkSubmitOperator (
    task_id = 'task_users_mart',
    dag=dag,
    application = '/user/s25321830/scripts/users_mart.py',
    conn_id = 'yarn_spark',
    conf={"spark.driver.maxResultSize": "2g"},
    executor_cores = 2,
    executor_memory = '2g'
)

task_zones_mart = SparkSubmitOperator (
    task_id = 'task_zones_mart',
    dag=dag,
    application = '/user/s25321830/scripts/zones_mart.py',
    conn_id = 'yarn_spark',
    conf={"spark.driver.maxResultSize": "2g"},
    executor_cores = 2,
    executor_memory = '2g'
)

task_friends_mart = SparkSubmitOperator (
    task_id = 'task_friends_mart',
    dag=dag,
    application = '/user/s25321830/scripts/friends_mart.py',
    conn_id = 'yarn_spark',
    conf={"spark.driver.maxResultSize": "2g"},
    executor_cores = 2,
    executor_memory = '2g'
)

[task_users_mart, task_zones_mart] >> task_friends_mart