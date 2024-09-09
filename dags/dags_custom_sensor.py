from airflow import DAG
import pendulum
from sensors.seoul_api_date_sensor import SeoulApiDateSensor 

'''
현재 시점에서 사용불가능한 dag (day_off가 1년 넘게 차이나는 상황) -> 결과가 up_for_reschedule
강의를 진행할 때는 코로나관련 데이터가 실시간으로 반영되었지만
23년 5월 이후로 데이터 업데이트가 진행되지 않는 상황'''

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2024,9,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    tb_corona_19_count_status_sensor = SeoulApiDateSensor(
        task_id='tb_corona_19_count_status_sensor',
        dataset_nm='TbCorona19CountStatus',
        base_dt_col='S_DT',
        day_off=0,
        poke_interval=600,
        mode='reschedule'
    )
    tv_corona_19_vaccine_stat_new_sensor = SeoulApiDateSensor(
        task_id='tv_corona_19_vaccine_stat_new_sensor',
        dataset_nm='tvCorona19VaccinestatNew',
        base_dt_col='S_VC_DT',
        day_off=1,
        poke_interval=600,
        mode='reschedule'
    )