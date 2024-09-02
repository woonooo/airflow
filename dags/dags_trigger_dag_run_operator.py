from airflow.models.dag import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    schedule='30 9 * * *',
    start_date=pendulum.datetime(2024, 9, 1, tz='Asia/Seoul'),
    catchup=False,
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"'
    )

    trigger_dag_run = TriggerDagRunOperator(
        task_id='trigger_dag_run',
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True, # 다시 실행할 것인가
        wait_for_completion=False, # trigger된 dag 기다리는가
        poke_interval=60, # trigger한 dag이 성공인지 살펴보는 시간주기
        allowed_states=['success'],
        failed_states=None 
    )

    start_task >> trigger_dag_run