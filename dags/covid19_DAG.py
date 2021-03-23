from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from covid19_ETL_process import get_covid19_report_daily, load_data_into_db

default_args = {
    'owner': 'wanthanai.j',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0,0)),
    'email': ['pentor.wanthanai@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'covid19_data_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    description='A simple data pipeline for COVID-19 report'
)

t1 = PythonOperator(
    task_id='get_covid19_report_today',
    python_callable=get_covid19_report_daily,
    dag=dag
)

t2 = PythonOperator(
    task_id='save_data_into_db',
    python_callable=load_data_into_db,
    dag=dag
)

t3 = EmailOperator(
    task_id='send_email',
    to=['pentorework@gmail.com'],
    subject='COVID-19 report today is ready',
    html_content='Please check your dashboard :)',
    dag=dag
)

t1 >> t2 >> t3