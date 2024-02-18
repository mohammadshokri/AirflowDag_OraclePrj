from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.utils.dates import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'oracle_set_daily_dop',
    default_args=default_args,
    description='DAG to insert data into Oracle table every 5 minutes',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
)

sql_command = """
INSERT INTO galaxy_aux.tb_max_dop (max_dop, min_dop, dt)
SELECT MAX(dop_sequence),Min(dop_sequence), substr(t_date,1,10) FROM galaxy_ai.event_prim
where 
"""

insert_task = OracleOperator(
    task_id='insert_into_oracle',
    oracle_conn_id= 'oracle_dw',
    sql=sql_command,
    dag=dag,
)

insert_task

