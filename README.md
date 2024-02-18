# airFlowDagPrj
 
Project Name: Airflow ETL Orchestrator

Overview:
This project consists of several Airflow DAGs designed to orchestrate ETL (Extract, Transform, Load) processes for data pipelines in an Oracle database environment.

1. ETL Oracle Service DAG:
   - Description: This DAG runs Oracle stored procedures `galaxy_fin.prc_etl_service` and `galaxy_fin.prc_etl_service_agg` daily at 2:00 AM.
   - Components:
     - `execute_prc_etl_service`: Operator to execute the `galaxy_fin.prc_etl_service` stored procedure.
     - `execute_prc_etl_service_agg`: Operator to execute the `galaxy_fin.prc_etl_service_agg` stored procedure.
     - `send_email_notification`: Operator to send email notification upon successful completion of the DAG.
   - Schedule: Runs daily at 2:00 AM.

2. ETL Oracle MVS DAG:
   - Description: This DAG runs various Oracle stored procedures related to Materialized Views (MVs) and data aggregation.
   - Components:
     - Operators for executing multiple stored procedures (`prc_MV_UNAuTHORIZED`, `prc_mv_timeout`, etc.).
     - Failure callback to send email notification in case of task failure.
   - Schedule: Runs daily at 4:00 AM.

3. Test Email DAG:
   - Description: This DAG sends a test email notification to specified recipients.
   - Components:
     - `test_email_task`: Operator to send the test email notification.
   - This DAG is scheduled to run on demand and is not associated with a specific schedule.

Setup Instructions:
1. Ensure you have Airflow installed and configured.
2. Copy the DAG Python files (`etl_oracle_service.py`, `etl_oracle_mvs.py`, `test_email2.py`) to the Airflow DAGs directory.
3. Update the connection IDs (`oracle_dw`, `email_tid`) in the DAGs to match your Airflow connections setup.
4. Modify the email recipients and content as needed for the `send_email_task` operators.
5. Start the Airflow scheduler and web server to trigger DAG runs and monitor execution.

Author: [Your Name]
Date: [Date]
