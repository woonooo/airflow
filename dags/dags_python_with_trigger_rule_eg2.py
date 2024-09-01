from airflow.models.dag import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_python_with_trigger_rule_eg2',
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 22, tz='Asia/Seoul'),
    catchup=False
) as dag:
    @task.branch(task_id='branching')
    def random_branch():
        import random

        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item == 'B':
            return 'task_b'
        elif selected_item == 'C':
            return 'task_c'
    
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo upstream1'
    )
        
    @task(task_id='task_b')
    def task_b():
        print('정상처리')
    
    @task(task_id='task_c')
    def task_c():
        print('정상처리')

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상처리')


    random_branch() >> [task_a, task_b(), task_c()] >> task_d()