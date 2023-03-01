from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'mikita_k',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id="bash_dag_v2",
        default_args=default_args,
        description="dag with bash operators",
        start_date=datetime(2022, 4, 1, 2),
        schedule_interval='@daily',
        catchup=False
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="\n\necho hello world task 1\n\n"
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command="\n\necho hello world task 2\n\n"
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command="\n\necho hello world task 3\n\n"
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)

    # task1 >> task2
    # task1 >> task3

    # task1 >> [task2, task3]
