from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import logging


# args for airflow
args = {
    'owner': 'art',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 10),
    'retries': 1,
    'retry_delay': timedelta(days=1),
    'schedule_interval': '@daily',
}


path_log = 'C:\\Doc\\pythonProject\\log\\'
time_log = datetime.now().strftime("%Y-%m-%d")

logging.basicConfig(level=logging.INFO, filename=f'{path_log}{time_log}_airflow_etl_log.log',
                    format='[%(asctime)s | %(levelname)s]: %(message)s')
logger = logging.getLogger()
logger.info(f'=========== Script launch {__file__} ===========')


def get_dates():
    """
    dates today - yesterday
    """
    date_format = '%Y-%m-%d'
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    logging.info(f'[{__file__} --> {get_dates.__name__}]   make dates start and end')
    return [yesterday.strftime(date_format),
            today.strftime(date_format)]


def request_from_service(start=get_dates()[0],
                         end=get_dates()[1]):
    """
    request from service(event) with date range
    """
    base_url = 'http://localhost:5000/'
    endpoint = 'events?'
    try:
        res = requests.get(f'{base_url}{endpoint}start_date={start}&end_date={end}')
    except (requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException,
            requests.exceptions.TooManyRedirects) as err:
        logging.error(f'{time_log}__{request_from_service.__name__}__{err}')
    else:
        df = pd.read_json(res.text)
        if df.shape[1] < 3:
            pass
        df['date'] = df['date'].dt.strftime("%m.%d.%Y")

        logging.info(f'[{__file__} --> {request_from_service.__name__}]   create df from request service(event)')
        # todo fix duplicate entry info
        return df


# todo make a block data quality


def write_to_postgres():
    """
    write data from request "requests_by_day" in postgres database
    """
    df = request_from_service()
    con = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="0196",  # change for work
        host="127.0.0.1",
        port="5432"
    )
    try:
        print("Database opened successfully")
        cur = con.cursor()

        for index, row in df.iterrows():
            cur.execute("""INSERT INTO event (city_e, date_e, device_e, "user_e")
             VALUES('%s','%s','%s','%s')""" % (row.city, row.date, row.device, row.user))
        con.commit()
        logging.info(f'[{__file__} --> {write_to_postgres.__name__}]   write data in postgres')

    except BaseException as err:
        logging.error(f'[{__file__} --> {write_to_postgres.__name__}    {err}]     !!!!!!!!!!!!!!!')
    finally:
        print("Запись успешно добавлена в таблицу event")
        con.close()


with DAG(dag_id='etl_event_to_postgres',
         description='etl_process_write_data_from_service(event)_to_postgres',
         catchup=False,
         default_args=args,
         tags=['parse_service(event)_write_postgres']) as dag:
    get_dates = PythonOperator(task_id='extract_data', python_callable=get_dates)
    request_from_service = PythonOperator(task_id='transform_data', python_callable=request_from_service)
    write_to_postgres = PythonOperator(task_id='load_data', python_callable=write_to_postgres)

    get_dates >> request_from_service >> write_to_postgres
