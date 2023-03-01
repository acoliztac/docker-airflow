from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    "owner": "mikita_k",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 1),
    "provide_context": True,
}

with DAG('Hello-World', description='Hello-World', schedule_interval='*/1 * * * *', catchup=False,
         default_args=default_args) as dag:  # 0 * * * *    */1 * * * *
    t1 = BashOperator(
        task_id="task_1",
        bash_command="echo 'Hello World from task 1'"
    )
    t2 = BashOperator(
        task_id="task_2",
        bash_command="echo 'Hello World from task 2'"
    )
    t3 = BashOperator(
        task_id="task_3",
        bash_command="echo 'Hello World from task 3'"
    )
    t4 = BashOperator(
        task_id="task_4",
        bash_command="echo 'Hello World from task 4'"
    )

    t1 >> t2
    t1 >> t3
    t2 >> t4
    t3 >> t4
