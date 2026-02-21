from urllib import request

import pendulum
from airflow.sdk import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable


def _get_data(**kwargs):
    year, month, day, hour, *_ = kwargs["logical_date"].timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = "/tmp/wikipageviews.gz"
    request.urlretrieve(url, output_path)

with DAG(
    dag_id="02_stocksense",
    start_date=pendulum.today("UTC").add(hours=-3),
    schedule=CronTriggerTimetable("@hourly", timezone="UTC"),
    max_active_runs=1,
    catchup=False
):
    check_data = HttpSensor(
        task_id="check_data",
        http_conn_id="wikimedia",
        endpoint=(
            "other/pageviews/{{ logical_date.year }}/"
            "{{ logical_date.year }}-{{ '{:02}'.format(logical_date.month) }}/"
            "pageviews-{{ logical_date.year }}"                                                                       
            "{{ '{:02}'.format(logical_date.month) }}"
            "{{ '{:02}'.format(logical_date.day) }}-"
            "{{ '{:02}'.format(logical_date.hour) }}0000.gz"
        ),
        method="HEAD",
        response_check=lambda response: response.status_code == 200,
        poke_interval=60 * 60,
        timeout=60 * 60 * 6,
        mode="reschedule",
    )
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
    )

    check_data >> get_data
