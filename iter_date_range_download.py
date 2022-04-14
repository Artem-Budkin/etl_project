import requests
import time
import pandas as pd
from datetime import timedelta, date, datetime
import psycopg2
import yaml
import logging
from pathlib import Path


time_log = datetime.now().strftime("%Y-%m-%d")
path_log = Path("logs", time_log + "_iter_date_range_log.log")
logging.basicConfig(level=logging.INFO, filename=f'{path_log}',
                    format='[%(asctime)s | %(levelname)s]: %(message)s')
logger = logging.getLogger()
logger.info(f'=========== Script launch {__file__} ===========')

setting = 'setting.yaml'

with open(f'{setting}', encoding='utf-8') as f:
    setting = yaml.safe_load(f)

database_postgres = setting['DB']['DATABASE']
user_postgres = setting['DB']['USER']
password_postgres = setting['DB']['PASSWORD']
host_postgres = setting['DB']['HOST']
port_postgres = setting['DB']['PORT']

# database connect
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
except BaseException as err:
    logging.error(f'[{__file__} --> connect tp postgres    {err}]     !!!!!!!!!!!!!!!')


def date_range(start_date_to: date,
               end_date_to: date):
    """
    create list with date start -> end
    """
    for n in range(int((end_date_to - start_date_to).days)):
        yield start_date_to + timedelta(n)


start_date_to = date(2022, 3, 8)  # task start date - 1 month
currentDay = datetime.now().day
currentMonth = datetime.now().month
currentYear = datetime.now().year
end_date_to = date(currentYear, currentMonth, currentDay)
dates_list = []

for single_date in date_range(start_date_to, end_date_to):
    dates_list.append(single_date.strftime("%Y-%m-%d"))


def requests_by_day(date_list: list = dates_list):
    """
    request from service(event) with date range
    """
    for i in range(len(date_list)):
        if i == (len(list(date_list)) - 1):
            pass
        else:
            date_start = date_list[i]
            date_end = date_list[i + 1]
            yield date_start, date_end


#  request with iterative enumeration of dates
def write_to_postgres():
    """
    write data from request "requests_by_day" in postgres database
    """
    # url service
    base_url = 'http://localhost:5000/'
    endpoint = 'events?'

    for start, end in requests_by_day(dates_list):
        try:
            res = requests.get(f'{base_url}{endpoint}start_date={start}&end_date={end}')
        except (requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException,
                requests.exceptions.TooManyRedirects) as err:
            logging.error(f'{time_log}__{write_to_postgres.__name__} request    {err}')
        else:
            df = pd.read_json(res.text)
            if df.shape[1] < 3:  # if table have 2 or less field, this table empty
                continue
            df['date'] = df['date'].dt.strftime("%m.%d.%Y")
            logging.info(f'[{__file__} --> {write_to_postgres.__name__}]\
               create df from request service(event) between {start} - {end}')

        # todo make data quality
        try:
            for index, row in df.iterrows():
                # print(row)
                cur.execute("""INSERT INTO event (city_e, date_e, device_e, "user_e")
                 VALUES('%s','%s','%s','%s')""" % (row.city, row.date, row.device, row.user))
        except BaseException as err:
            logging.error(f'[{__file__} --> {write_to_postgres.__name__} write    {err}]     !!!!!!!!!!!!!!!')
        finally:
            count = cur.rowcount
            con.commit()

        print(count, f"Add data from event {start} - {end}")


if __name__ == '__main__':
    start = time.time()
    write_to_postgres()
    con.close()
    print('Database close')
    end = time.time()
    logging.info(f'=========== Script end {__file__} ===========')
    print((end - start), "sec")


