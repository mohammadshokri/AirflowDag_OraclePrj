from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator, OracleStoredProcedureOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'etl_oracle_service',
    default_args=default_args,
    description='DAG to run Oracle stored procedure galaxy_ai.prc_etl_service every day at 2:00 AM',
    schedule_interval='0 2 * * *',
    catchup=False,
)

execute_procedure_task1 = OracleStoredProcedureOperator(
    task_id='execute_prc_etl_service',
    oracle_conn_id='oracle_dw',
    procedure="galaxy_fin.prc_etl_service",
    dag=dag,
)
execute_procedure_task2 = OracleStoredProcedureOperator(
    task_id='execute_prc_etl_service_agg',
    oracle_conn_id='oracle_dw',
    procedure="galaxy_fin.prc_etl_service_agg",
    dag=dag,
)

send_email_task = EmailOperator(
    task_id='send_email_notification',
    to='faghihabdollahi.r@tiddev.com',
    cc='shokri.m@tiddev.com,panahi.s@tiddev.com',
    subject='Notification: Oracle ETL Service DAG Completed (service_call)',
    html_content='The Oracle ETL Service DAG has completed successfully!',
    dag=dag,
    conn_id='email_tid',
)

execute_procedure_task1 >> execute_procedure_task2 >> send_email_task
