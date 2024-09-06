from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id
    
    def get_conn(self): # get_conn() 함수 재정의: get_connection() 메서드 이용해 정보 받아옴
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn
    
    def bulk_load(self, table_name, file_name, delimiter:str, is_header:bool, is_replace:bool):
        from sqlalchemy import create_engine

        self.log.info('적재 파일대상: ' + file_name)
        self.log.info('테이블: ' + table_name)
        self.get_conn()

        header = 0 if is_header else None   # is_header = Ture면 0
        if_exists = 'replace' if is_replace else 'append'
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:
            try:
                # string문자가 아닐경우 아래의 명령문이 실행되지 않도록 try-except문
                file_df[col] = file_df[col].str.replace('\r\n', '') # 줄넘김 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue
        
        self.log.info("적재 건수: " + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        file_df.to_sql(name=table_name,
                       con=engine,
                       schema='public',
                       if_exists=if_exists,
                       index=False)
