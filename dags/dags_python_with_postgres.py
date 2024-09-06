from airflow.models.dag import DAG
import pendulum
from airflow.operators.python import PythonOperator
 
with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2024,9,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing
 
		# closing은 with문을 벗어난 순간 자동으로 conn.close() 함수를 실행시켜 주는 역할
        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn: # DB서버와 연결하는 Session 열기
            with closing(conn.cursor()) as cursor: # cursor: Session을 통해서 서버 간 데이터를 옮기는 객체
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);' # SQL문 입력
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()
    
    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.47.0.3', '5432', 'jungah.heo', 'jungah.heo', 'jungah.heo']
    )