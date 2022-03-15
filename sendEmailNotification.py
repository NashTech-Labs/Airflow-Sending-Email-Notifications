from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'Shiv',
    'start_date': datetime(2022, 3, 8),
    'depends_on_past': False,
    'email': ['shiv.oberoi@knoldus.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG("send_email",
         schedule_interval="@daily",
         default_args=default_args,
         ) as dag:

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello Shiv!'",
    )

    send_email = EmailOperator(
        task_id='send_email',
        to='shiv.oberoi@knoldus.com',
        subject='Got Email from Airflow',
        html_content="""<h1>Hi shiv, how are you? You just received an email from Airflow.</h1>""",
    )

    open_temp_folder = BashOperator(
        task_id='open_temp_folder',
        bash_command='cd temp_folder',
    )

say_hello >> send_email >> open_temp_folder