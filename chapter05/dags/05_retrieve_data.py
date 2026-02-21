from urllib import request

import pendulum
from airflow.sdk import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


with DAG(
    dag_id="05_retrieve_data",
    start_date=pendulum.today("UTC").add(hours=-3),
    schedule=CronTriggerTimetable("@hourly", timezone="UTC"),
    max_active_runs=1,
    catchup=True,
):
    check_data = HttpSensor(
        task_id="check_data",
        http_conn_id="wikipedia_pageviews",
        endpoint=(
            "other/pageviews/"
            "{{ logical_date.year }}/"
            "{{ logical_date.year }}-{{ logical_date.month:0>2 }}/"
            "pageviews-{{ logical_date.year }}{{ logical_date.month:0>2 }}"
            "{{ logical_date.day:0>2 }}-{{ logical_date.hour:0>2 }}0000.gz"
        ),
        response_check=lambda response: response.status_code == 200,
        poke_interval=60 * 60,
        timeout=60 * 60 * 6,
        mode="reschedule",
    )
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={
            "year": "{{ logical_date.year }}",
            "month": "{{ logical_date.month }}",
            "day": "{{ logical_date.day }}",
            "hour": "{{ logical_date.hour }}",
            "output_path": "/tmp/wikipageviews-{{ logical_date.format('YYYYMMDDHH') }}.gz",
        },
    )

    check_data >> get_data
