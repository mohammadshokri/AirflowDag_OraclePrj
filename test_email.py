from airflow import DAG
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
    'test_email2',
    default_args=default_args,
    description='DAG to send email notification',
    schedule=None,
    catchup=False,
)

email_content = "Test airflow email"

send_email_task = EmailOperator(
    task_id='test_email_task',
    to='faghihabdollahi.r@tiddev.com',
    cc='shokri.m@tiddev.com,panahi.s@tiddev.com',
    subject='Task Statuses for ELT MV DAG',
    html_content=email_content,
    conn_id='email_tid',
    dag=dag,
)
send_email_task