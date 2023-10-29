from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import boto3
import pendulum
from airflow.models.variable import Variable

def group_log_s3_file():
    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
    )
    bucket_name = 'sprint6'
    key_name = 'group_log.csv'
    local_filename = '/data/group_log.csv'
    try: # Проверка на существовании файла в S3
        s3_client.head_object(Bucket=bucket_name, Key=key_name)
    except:
        print(f'The file {key_name} does not exist in the bucket {bucket_name}.')
    else:
        s3_client.download_file(Bucket=bucket_name, Key=key_name, Filename=local_filename)
        print(f'The file {key_name} has been downloaded successfully to {local_filename}.')


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def project6_group_log():
    task1 = PythonOperator(
        task_id='group_log',
        python_callable=group_log_s3_file,
        op_kwargs={'bucket': 'data-bucket', 'key': 'group.csv'},
    )
    task1

_ = project6_group_log()
