import psycopg2
import yaml
import os
from pathlib import Path


path_setting = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..'))
setting = 'setting.yaml'
path_setting = Path(path_setting, setting)
with open(f'{path_setting}', encoding='utf-8') as f:
    setting = yaml.safe_load(f)

database_postgres = setting['DB']['DATABASE']
user_postgres = setting['DB']['USER']
password_postgres = setting['DB']['PASSWORD']
host_postgres = setting['DB']['HOST']
port_postgres = setting['DB']['PORT']

con = psycopg2.connect(
    database=database_postgres,
    user=user_postgres,
    password=password_postgres,
    host=host_postgres,
    port=port_postgres
)

print("Database opened successfully")
cur = con.cursor()


# Create table
def create_table_sql():
    """
    Create in postgres database.
    """
    # In test use event2
    cur.execute("""
  CREATE TABLE event(
      "index_e" SERIAL PRIMARY KEY NOT NULL,
      "city_e" VARCHAR(40) NOT NULL,
      "date_e" DATE NOT NULL,
      "device_e" VARCHAR(30) NOT NULL,
      "user_e" VARCHAR(20) NOT NULL
  )
  """)

    print("Table created successfully")
    con.commit()
    con.close()


# create_table_sql()


# Part SQL query
SQL_1_date_city = """
    select 
        to_char(date_e, 'dd.mm.yyyy') as date,
        count(case when city_e = 'Moscow' then city_e end) as Moscow,
        count(case when city_e = 'Saint Petersburg' then city_e end) as Saint_Petersburg,
        count(case when city_e = 'Amsterdam' then city_e end) as Amsterdam
    from event
    group by date_e 
    order by date_e;
"""


SQL_2_date_device = """
    select 
        to_char(date_e, 'dd.mm.yyyy') as date,
        count(case when device_e = 'MOBILE' then device_e end) as mobile,
        count(case when device_e = 'DESKTOP' then device_e end) as desktop
    from event
    group by date_e 
    order by date_e ;
"""


SQL_2_date_device_alter = """
    select 
        city_e as city,
        count(device_e) filter(where device_e = 'MOBILE') as mobile,
        count(device_e) filter(where device_e = 'DESKTOP') as desktop
    from event
    group by city_e ;
"""


SQL_3_city_device = """
    select 
        city_e as city,
        (cast(count(case when device_e = 'MOBILE' then device_e end) as float) / cast(count(device_e) as float)) as mobile,
        (cast(count(case when device_e = 'DESKTOP' then device_e end) as float) / cast(count(device_e) as float)) as desktop
    from event 
    group by city_e;
"""

# todo make round in SQL_3_city_device and write % ????
