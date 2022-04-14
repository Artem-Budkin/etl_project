import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import yaml
import time
import logging
from pathlib import Path


setting = 'setting.yaml'

with open(f'{setting}', encoding='utf-8') as f:
    setting = yaml.safe_load(f)

database_postgres = setting['DB']['DATABASE']
user_postgres = setting['DB']['USER']
password_postgres = setting['DB']['PASSWORD']
host_postgres = setting['DB']['HOST']
port_postgres = setting['DB']['PORT']

time_log = datetime.now().strftime("%Y-%m-%d")
path_log = Path("logs", time_log + "_daily_etl_log.log")
logging.basicConfig(level=logging.INFO, filename=f'{path_log}',
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
        database=database_postgres,
        user=user_postgres,
        password=password_postgres,
        host=host_postgres,
        port=port_postgres
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


if __name__ == '__main__':
    start = time.time()
    write_to_postgres()
    end = time.time()
    logging.info(f'=========== Script end {__file__} ===========')
    print((end - start), "sec")
