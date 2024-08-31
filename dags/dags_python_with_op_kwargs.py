from airflow.models.dag import DAG
import pendulum
from airflow.operators.python import PythonOperator
from common.common_func import regist2


with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *", 
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,  
) as dag:

    regist2_t1 = PythonOperator(
        task_id = "regist2_t1",
        python_callable=regist2,
        op_args = ['jaheo', 'woman', 'kr', 'seoul'],
        op_kwargs = {'email': 'wjddk912@gmail.com', 'phone': '010-0000-0000'}
    )
    regist2_t1