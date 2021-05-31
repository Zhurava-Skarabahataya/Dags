try:

    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator import BashOperator
    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
    from pyspark.sql import SparkSession
    from pyspark.sql import *

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("HELLO ")
    sc = pyspark.SparkContext()
   
    


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
        dag_id="only_py",
        schedule_interval="@once",
        default_args=default_args,
        catchup=False) as f:

    first_f = PythonOperator(
        task_id="first",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Soumil Shah"}
    )
    
     
first_f 