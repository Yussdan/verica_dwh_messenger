from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import boto3
import pendulum

def group_log_s3_file():

    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key='group_log.csv',
        Filename='/data/group_log.csv'
    )



@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def project6_group_log():
    task1 = PythonOperator(
        task_id='group_log',
        python_callable=group_log_s3_file,
        op_kwargs={'bucket': 'data-bucket', 'key': 'group.csv'},
    )
    task1

_ = project6_group_log()