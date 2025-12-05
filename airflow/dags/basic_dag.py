from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def hello_world():
    print("Hello! Airflow DAG is running successfully.")

with DAG(
    dag_id="basic_test_dag",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world
    )

    task1
