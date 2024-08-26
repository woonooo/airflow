import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task
from common.common_func import get_sftp



with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(som_input):
        print(som_input)
    
    python_task_1 = print_context("task_decorator 실행")
