from airflow.models.dag import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False, 
) as dag:
    bash_t1 = BashOperator( 
        task_id="bash_t1",
        bash_command="echo 'data_interval_end: {{data_interval_end}}'",
    )

    bash_t2 = BashOperator( 
    task_id="bash_t2",
    env={
        'START_DATE': '{{ data_intereval_start | ds }}',
        'END_DATE': '{{ data_interval_end | ds }}'
    },
    bash_command='echo "Start date is $START_DATE" && '
                'echo "End date is $END_DATE"',
    )
    bash_t1 >> bash_t2