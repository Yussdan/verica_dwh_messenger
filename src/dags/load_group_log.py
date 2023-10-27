from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import vertica_python


vertica_conn = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv2023100614',
    'password': 'BfscNtiIJDEXT8M'
}

def group_log_s3_file(table='group_log'):
    with vertica_python.connect(**vertica_conn) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT column_name FROM v_catalog.columns WHERE table_schema = 'STV2023100614__STAGING' AND table_name = '{table}' ORDER BY ordinal_position")
        result = cur.fetchall()
        column_names = [row[0] for row in result]
        columns = ', '.join(column_names)
        cur.execute(f"COPY STV2023100614__STAGING.{table}({columns})  FROM LOCAL '/data/{table}.csv'  DELIMITER ',' ;")




@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def project6_load_group_log():
    task1 = PythonOperator(
        task_id='load_group_log',
        python_callable=group_log_s3_file
    )
    task1

_ = project6_load_group_log()