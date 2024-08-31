from airflow.models.dag import DAG
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *", 
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = '912wjddk@naver.com',
        cc = 'wjddk912@gmail.com',
        subject = 'Airflow 성공 메일',
        html_content = 'Airflow 작업이 완료되었습니다'
    )