from airflow.models.dag import DAG
import pendulum
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models import Variable

with DAG(
    dag_id='dags_branch_python_operator',
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 22, tz='Asia/Seoul'),
    catchup=False
) as dag:
    def select_Random():
        import random

        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B', 'C']:
            return ['task_b', 'task_c']
        
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_Random
    )

    def common_func(**kwargs):
        print(kwargs['selected'])
    
    task_a = PythonOperator(
        taskk_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected': 'A'}
    )
    task_b = PythonOperator(
        taskk_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected': 'B'}
    )
    task_c = PythonOperator(
        taskk_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected': 'C'}
    )

    python_branch_task >> [task_a, task_b, task_c]
    

