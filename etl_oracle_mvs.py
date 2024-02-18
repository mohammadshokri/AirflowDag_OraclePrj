from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator, OracleStoredProcedureOperator
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
    'etl_oracle_mvs',
    default_args=default_args,
    description='DAG to run Oracle stored procedures',
    schedule='0 4 * * *',
    catchup=False,
)

def send_email_on_failure(context):
    try:
        logging.info("Running send_email_on_failure callback")
        task_instance = context['task_instance']
        logging.info(f"Task instance: {task_instance}")

        if task_instance.task_id == 'mv_sending_email':
            # If the failed task is the email sending task, just log the failure
            logging.error(f"Email sending task {task_instance.task_id} failed. Error: {context['exception']}")
        else:
            subject = f"Task Failure Alert - {task_instance.task_id}"
            content = f"<h2>Task {task_instance.task_id} failed.</h2>\n\nError: {context['exception']}"
            email_operator = EmailOperator(
                task_id=f'send_email_{task_instance.task_id}_failure',
                to='faghihabdollahi.r@tiddev.com',
                cc='shokri.m@tiddev.com,panahi.s@tiddev.com',
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



tasks = {}

for task_id, procedure_name in [
    ('mv_unauthorized_task', 'galaxy_ai.prc_MV_UNAuTHORIZED'),
    ('mv_timeout_task', 'galaxy_ai.prc_mv_timeout'),
    ('mv_recent_summary_task', 'galaxy_ai.prc_mv_recent_summary'),
    ('mv_client_error_task', 'galaxy_ai.prc_mv_client_error'),
    ('mv_daily_error_task', 'galaxy_ai.prc_mv_daily_error'),
    ('mv_daily_status_task', 'galaxy_ai.prc_mv_daily_status'),
    ('SP_INSERT_TO_olap_sum2_task', 'galaxy_aux.SP_INSERT_TO_olap_sum2')
]:
    task = OracleStoredProcedureOperator(
        task_id=task_id,
        oracle_conn_id='oracle_dw',
        procedure=procedure_name,
        on_failure_callback=send_email_on_failure,
        dag=dag
    )
    tasks[task_id] = task

for task in tasks.values():
    task


