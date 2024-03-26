from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Estos son los valores predeterminados que se aplicarán a tus tareas
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# Define tu DAG
dag = DAG(
    'dag_proyecto_final',
    default_args=default_args,
    description='A simple ETL pipeline DAG',
    schedule_interval='*/5 * * * *',  # Ejecuta una vez al día
    catchup=False
)

# Define tu única tarea ETL utilizando BashOperator
etl_task = BashOperator(
    task_id='run_etl',
    bash_command='python /opt/airflow/plugins/etl_script.py',
    dag=dag,
)

