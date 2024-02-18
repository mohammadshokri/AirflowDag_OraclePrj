from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator,OracleStoredProcedureOperator
from airflow.utils.dates import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'prc_test',
    default_args=default_args,
    description='DAG to run Oracle stored procedure galaxy_ai.prc_etl_service every night at 18:00',
    schedule='0 18 * * *',  # Run every night at 18:00
)

iexecute_procedure_task = OracleStoredProcedureOperator(
    task_id='prc_test_task',
    oracle_conn_id='oracle_dw',
    procedure="galaxy_fin.prc_etl_service",
    dag=dag,
)

execute_procedure_task