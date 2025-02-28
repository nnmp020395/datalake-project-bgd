from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
       'my_first_dag',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(minutes=5),
       },
       description='A first DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
   dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """


   def launch_task(**kwargs):
       print("Hello Airflow - This is Task with param1:", kwargs['param1'],
             " and param2: ", kwargs['param2'])

   t1 = PythonOperator(
       task_id='task1',
       python_callable=launch_task,
       provide_context=True,
       op_kwargs={'param1': 'Task 1', 'param2': 'value1'}
   )

   t2 = PythonOperator(
       task_id='task2',
       python_callable=launch_task,
       provide_context=True,
       op_kwargs={'param1': 'Task 2', 'param2': 'value2'}
   )

   # t1 >> t2 can also be written like this:
   t1.set_downstream(t2)
