from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'mikita_k',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


def greet(ti, age, *args, **kwargs):
    name = ti.xcom_pull(key=None, task_ids='get_name')
    print(f"\n\nHello World!\nMy name is {name}.\nI'm {age} years old.\n\n\n")


def get_name():
    return "Jerry"


with DAG(
        default_args=default_args,
        dag_id="dag_with_python_operator_v04",
        description="Dag with PythonOperator",
        start_date=datetime(2022, 4, 1),
        schedule_interval="@daily",
        catchup=False,

) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={"age": 20},
        provide_context=True
    )
    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    task2 >> task1
