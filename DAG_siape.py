from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

with DAG('databricks_dag',
  start_date = days_ago(1),
  description='DAG para a execução da pipeline SIAPE',
  schedule_interval = timedelta(days=30),
  default_args = default_args
  ) as dag:

  opr_run_now = DatabricksSubmitRunOperator(
    task_id = 'run_now',
    databricks_conn_id = 'databricks_connection',
    job_id = 676863208026150
  )