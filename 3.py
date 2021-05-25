try:

    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator import BashOperator
    from pyspark.sql import SparkSession
    from pyspark.sql import *

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("HELLO ")
    


def second_function_execute(**context):
    print("Is it me you looking for")
   


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
}


with DAG(
        dag_id="31"
        schedule_interval="@once",
        default_args=default_args,
        catchup=False) as f:

    first_f = PythonOperator(
        task_id="first",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Soumil Shah"}
    )
    
    second_f = BashOperator(
        task_id="second",
        bash_command='echo HELLLLLOOOOOOOOOOO'
    )
    
   
first_f >> second_f 