from urllib import request

import pendulum
from airflow.sdk import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


def _fetch_pageviews(pagenames, logical_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open(f"/tmp/wikipageviews-{ logical_date.format('YYYYMMDDHH') }") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = int(view_counts)

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{logical_date}'"
                ");\n"
            )


with DAG(
    dag_id="08_writing_insert_statements",
    start_date=pendulum.today("UTC").add(hours=-3),
    schedule=CronTriggerTimetable("@hourly", timezone="UTC"),
    max_active_runs=1,
    catchup=False,
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
        op_kwargs={
            "year": "{{ logical_date.year }}",
            "month": "{{ logical_date.month }}",
            "day": "{{ logical_date.day }}",
            "hour": "{{ logical_date.hour }}",
            "output_path": "/tmp/wikipageviews-{{ logical_date.format('YYYYMMDDHH') }}.gz",
        },
    )

    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command="gunzip --force /tmp/wikipageviews-{{ logical_date.format('YYYYMMDDHH') }}.gz",
    )

    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    )

    check_data >> get_data >> extract_gz >> fetch_pageviews
