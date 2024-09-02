from airflow.models.dag import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task

with DAG(
    dag_id='dags_simple_http_operator',
    schedule=None,
    start_date=pendulum.datetime(2024, 9, 1, tz='Asia/Seoul'),
    catchup=False,
) as dag:
    
    '''서울시 공공자전거 대여소 정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/12/',
        method='GET',
        headers={
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint(json.load(result))

    tb_cycle_station_info >> python_2()