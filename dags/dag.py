import requests
from datetime import datetime, timedelta

from airflow import DAG

from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def transform_data(ti: TaskInstance):
    """Access the details of every Pokémon to get "base_happiness" and "capture_rate"

    Args:
        ti (TaskInstance): Task Instance with the HTTP response from get_data step

    Returns:
        list[tuple]: list of tuples with values (pokemon_name, base_happiness, capture_rate)
    """

    # pull data from other task
    data = ti.xcom_pull(task_ids="get_api_data")

    parsed_data = []
    for p in data.get("pokemon_entries")[:30]:
        name = p.get("pokemon_species").get("name")
        complementary_data = requests.get(p.get("pokemon_species").get("url")).json()

        parsed_data.append((
            name,
            complementary_data.get("base_happiness"),
            complementary_data.get("capture_rate")
        ))

    # store data to be accessed by other tasks
    # important note: tuple format is not supported, values need to be converted in the next step
    ti.xcom_push(key="parsed_data", value=parsed_data)


def insert_on_postgres(ti: TaskInstance):
    """Function to insert data retrieved from the API on a postgres database

    Args:
        ti (TaskInstance): Contains information from the steps of a DAG
    """
    data: list[list] = ti.xcom_pull(task_ids="transform_data", key="parsed_data")
    # convert for sql insert
    insert_tuples: list[tuple] = [tuple(x) for x in data]

    pg_hook = PostgresHook(postgres_conn_id="postgresdb")
    pg_hook.insert_rows(
        table="test_output",
        rows=insert_tuples,
        target_fields=["pokemon_name", "base_happiness", "capture_rate"]
    )


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_test",
    default_args=DEFAULT_ARGS,
    description="Test ETL DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime.today(),
    catchup=False,
    tags=["test", "http"],
) as dag:

    # verify if API is up
    verify_api_task = HttpSensor(
        task_id="verify_api_status",
        http_conn_id="poke_api",
        endpoint="/api/v2",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        poke_interval=10,
        timeout=60,
    )

    # make request
    get_data_task = HttpOperator(
        task_id="get_api_data",
        http_conn_id="poke_api",
        endpoint="/api/v2/pokedex/6",
        method="GET",
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    # transform data
    transform_data_task = PythonOperator(
        task_id="transform_data", python_callable=transform_data
    )

    # create table if not exists
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_output_table",
        conn_id="postgresdb",
        sql="""
            CREATE TABLE IF NOT EXISTS test_output (
                pokemon_name VARCHAR,
                base_happiness INT,
                capture_rate INT
            )
        """,
    )

    # insert on db
    insert_data_task = PythonOperator(
        task_id="load_data", python_callable=insert_on_postgres
    )

    # define pipeline order
    verify_api_task >> get_data_task >> transform_data_task >> create_table_task >> insert_data_task
