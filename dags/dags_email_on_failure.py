from airflow.models import Variable
from airflow.models.dag import DAG
import pendulum
from datetime import timedelta
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator

email_str = Variable.get("email target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_email_on_failure',
    schedule='0 1 * * *',
    start_date=pendulum.datetime(2024, 9, 1, tz='Asia/Seoul'),
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    @task(task_id='python_fail')
    def python_task_func():
        raise AirflowException("에러 발생")
    python_task_func()

    bash_fail = BashOperator(
        task_id='bash_fail',
        bash_command='exit 1',
    )

    bash_success = BashOperator(
        task_id='bash_success',
        bash_command='exit 0',
    )
