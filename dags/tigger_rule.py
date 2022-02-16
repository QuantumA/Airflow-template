from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


from datetime import datetime


default_args = {
    'start_date': datetime(2022,1,1)
}


def _next():
    return 'will_succed_2'


with DAG('trigger_rule', schedule_interval='@daily',
            default_args=default_args,
            catchup=False) as dag:

            will_fail_1 = BashOperator(
                task_id='will_fail_1',
                bash_command='exit 1',
                do_xcom_push=False
            )

            will_fail_2 = BashOperator(
                task_id='will_fail_2',
                bash_command='exit 1',
                do_xcom_push=False
            )

            succed_on_all_failed = BashOperator(
                task_id='succed_on_all_failed',
                bash_command='exit 0',
                do_xcom_push=False,
                trigger_rule='all_failed'

            )

            will_succed_1 = BashOperator(
                task_id='will_succed_1',
                bash_command='exit 0',
                do_xcom_push=False
            )

            will_fail_3 = BashOperator(
                task_id='will_fail_3',
                bash_command='exit 1',
                do_xcom_push=False
            )

            succed_on_one_failed = PythonOperator(
                task_id='succed_on_one_failed',
                trigger_rule='one_failed',
                python_callable=_next
            )

            
            will_be_skipped = DummyOperator(
                task_id='will_be_skipped'
            )


            will_succed_2 = BashOperator(
                task_id='will_succed_2',
                bash_command='exit 0',
                do_xcom_push=False
            )

            will_succed_on_none_failed_or_skipped = BashOperator(
                task_id='will_succed_on_none_failed_or_skipped',
                bash_command='exit 1',
                do_xcom_push=False,
                trigger_rule='none_failed_or_skipped'
            )




            [will_fail_1, will_fail_2] >> succed_on_all_failed >> [will_succed_1, will_fail_3]
            [will_succed_1, will_fail_3] >> succed_on_one_failed >> [will_be_skipped, will_succed_2]
            [will_be_skipped, will_succed_2] >> will_succed_on_none_failed_or_skipped