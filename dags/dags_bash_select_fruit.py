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
    bash_orange = BashOperator(
        task_id="bash_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )
    bash_grape = BashOperator(
        task_id="bash_grape",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh GRAPE",
    )

    bash_orange >> bash_grape
    # [END howto_operator_bash]