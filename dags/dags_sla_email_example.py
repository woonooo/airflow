'''
1번째 task 이외에 나머지는 sla miss가 나오는 dag
마지막 task가 sla가 따로 걸려있어서(30초) 먼저 miss 이메일이 발송됨
'''

from airflow.models.dag import DAG
import pendulum
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_sla_email_example',
    schedule='*/10 * * * *',
    start_date=pendulum.datetime(2024, 9, 1, tz='Asia/Seoul'),
    catchup=False,
    default_args={
        'sla': timedelta(seconds=70),
        'email': email_lst
    }
) as dag:
    
    task_slp_30s_sla_70s = BashOperator(
        task_id='task_slp_30s_sla_70s',
        bash_command='sleep 30'
    )

    task_slp_60s_sla_70s = BashOperator(
        task_id='task_slp_60s_sla_70s',
        bash_command='sleep 60'
    )
    
    task_slp_10s_sla_70s = BashOperator(
        task_id='task_slp_10s_sla_70s',
        bash_command='sleep 10'
    )

    task_slp_10s_sla_30s = BashOperator(
        task_id='task_slp_10s_sla_30s',
        bash_command='sleep 10',
        sla=timedelta(seconds=30)
    )

    task_slp_30s_sla_70s >> task_slp_60s_sla_70s >> task_slp_10s_sla_70s >> task_slp_10s_sla_30s