from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dba_oracle',
    default_args=default_args,
    description='DAG to run Oracle stored procedures',
    schedule='0 5 * * *',  # Run daily at 4:00 AM
    catchup=False,
)

def send_email_on_failure(context):
    try:
        logging.info("Running send_email_on_failure callback")
        task_instance = context['task_instance']
        logging.info(f"Task  instance: {task_instance}")

        subject = f"Task Gathering Stats Failure Alert - {task_instance.task_id}"
        content = f"<h2>Task {task_instance.task_id} failed.</h2>\n\nError: {context['exception']}"
        email_operator = EmailOperator(
            task_id=f'send_email_{task_instance.task_id}_failure',
            to='faghihabdollahi.r@tiddev.com',
            cc='shokri.m@tiddev.com',
            subject=subject,
            html_content=content,
            dag=context['dag'],
            conn_id='email_tid',
            mime_subtype='mixed'
        )
        email_operator.execute(context=context)
        logging.info(f"Failure email sent for task: {task_instance.task_id}")

    except Exception as e:
        logging.error(f"Error occurred in send_email_on_failure callback: {str(e)}")

    return None

oracle_gather_stats_task = OracleStoredProcedureOperator(
    task_id='gather_stats',
    oracle_conn_id='oracle_dw',
    procedure="sys.prc_mmk_gather_stats",
    on_failure_callback=send_email_on_failure,
    dag=dag,
)