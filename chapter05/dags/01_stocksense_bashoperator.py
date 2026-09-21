import pendulum
from airflow.sdk import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

with DAG(
    dag_id="01_stocksense_bashoperator",
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
    get_data = BashOperator(
        task_id="get_data",
        bash_command=(
            "curl -o /tmp/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ logical_date.year }}/"
            "{{ logical_date.year }}-{{ '{:02}'.format(logical_date.month) }}/"
            "pageviews-{{ logical_date.year }}"
            "{{ '{:02}'.format(logical_date.month) }}"
            "{{ '{:02}'.format(logical_date.day) }}-"
            "{{ '{:02}'.format(logical_date.hour) }}0000.gz"
        ),
    )

    check_data >> get_data
