from airflow.models.dag import DAG
import pendulum
from airflow.sensors.date_time import DateTimeSensor

with DAG(
    dag_id='dags_time_sensor',
    schedule='*/10 * * * *',
    start_date=pendulum.datetime(2024, 9, 10, 0, 0, 0),
    end_date=pendulum.datetime(2024, 9, 10, 1, 0, 0),  # 7번 돌게되는 스케줄
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensor(
        task_id='sync_sensor',
        target_time='''{{macros.datetime.utcnow() + macros.timedelta(minutes=5)}}'''
    )