import pendulum
import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="0 0 * * 6#1",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # [START howto_operator_bash]
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )
    t2_grape = BashOperator(
        task_id="t2_grape",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh GRAPE",
    )

    t1_orange >> t2_grape
    # [END howto_operator_bash]