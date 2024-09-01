from airflow.models.dag import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="10 0 * * 6#2",
    start_date=pendulum.datetime(2024, 8, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # START_DATE: 2주전 월요일, END_DATE: 2주전 토요일 -> 8월 12일 , 17일
    bash_task_1 = BashOperator( 
        task_id="bash_task_1",
        env={'START_DATE': '{{ data_interval_end.in_timezone("Asia/Seoul") - macro.dateutil.relativedelta.relativedelta(days=16) | ds }}',
             'END_DATE': '{{ data_interval_end.in_timezone("Asia/Seoul") - macro.dateutil.relativedelta.relativedelta(days=11) | ds }}',
        },
        bash_command="echo 'START_DATE: $START_DATE' && echo 'END_DATE: $END_DATE'",
    )
