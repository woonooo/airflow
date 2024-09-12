'''
execution_timeout 구현 dag
즉, task의 timeout이 발생
dag은 timeout 되지 않음
'''
from airflow.models.dag import DAG
import pendulum
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_timeout_example_1',
    start_date=pendulum.datetime(2024, 9, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        'execution_timeout': timedelta(seconds=20),
        'sla': timedelta(seconds=70),
        'email': email_lst
    }
) as dag:
    
    bash_sleep_30 = BashOperator(
        task_id='bash_sleep_30',
        bash_command='sleep 30'
    )

    bash_sleep_10 = BashOperator(
        trigger_rule='all_done',  # 상위 task가 실패해서 돌아가지 않기에 이를 대비하여 trigger_rule작성
        task_id='bash_sleep_10',
        bash_command='sleep 10'
    )

    bash_sleep_30 >> bash_sleep_10