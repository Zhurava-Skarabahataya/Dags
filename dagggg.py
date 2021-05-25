try:

    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.bash_operator import BashOperator
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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


dag = DAG(
        dag_id="dagggg",
        schedule_interval="@once",
        default_args=default_args) as f:

    first_f = PythonOperator(
        task_id="first",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Soumil Shah"}
    )
    
    second_f = BashOperator(
        task_id="second",
        bash_command='echo HELLLLLOOOOOOOOOOO',
        dag = dag
    )
    
    

first_f >> second_f 