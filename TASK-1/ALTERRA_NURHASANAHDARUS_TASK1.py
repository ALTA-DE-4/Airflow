from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

# Function to be used by a PythonOperator to print a simple message
def print_hello():
    return 'Hello world from first Airflow DAG!'

# Define the DAG with a schedule interval of every 5 hours
dag = DAG(
    'alterra_hasanahDarus_task1', 
    description='Hello World DAG',
    schedule_interval='0 */5 * * *',
    start_date=datetime(2022, 10, 21), 
    catchup=False
)

# Define a start task using EmptyOperator
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Function to push a value to XCom
def push_xcom(**kwargs):
    value = 'nilai_saya'
    kwargs['ti'].xcom_push(key='kunci_saya', value=value)

# Define a PythonOperator to push a value to XCom
push_task = PythonOperator(
    task_id='push_task',
    provide_context=True,
    python_callable=push_xcom,
    dag=dag,
)

# Another function to push a different value to XCom (for demonstration purposes)
def push_another_xcom(**kwargs):
    another_value = 'nilai_lain'
    kwargs['ti'].xcom_push(key='kunci_lain', value=another_value)

# Define another PythonOperator to push a different value to XCom
push_another_task = PythonOperator(
    task_id='push_another_task',
    provide_context=True,
    python_callable=push_another_xcom,
    dag=dag,
)

# Function to pull multiple values from XCom
def pull_xcoms(**kwargs):
    ti = kwargs['ti']
    value1 = ti.xcom_pull(task_ids='push_task', key='kunci_saya')
    value2 = ti.xcom_pull(task_ids='push_another_task', key='kunci_lain')
    print(f'Nilai yang ditarik: {value1}, {value2}')

# Define a PythonOperator to pull multiple values from XCom
pull_task = PythonOperator(
    task_id='pull_task',
    provide_context=True,
    python_callable=pull_xcoms,
    dag=dag,
)

# Set the task dependencies
start >> push_task >> push_another_task >> pull_task