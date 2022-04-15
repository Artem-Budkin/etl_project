# ETL_PROJECT
## ETL project (parsing information, writing to the database)

Скрипты:
1. daily_etl.py - для запуска каждый день
2. iter_date_range_download.py - для парсинга всех записей в указанный диапозон (с 08.03.2022 - по настоящее время), итеративно по дням
3. airflow_etl.py - для работы Airflow (файл не проверен, написан по подобию других файлов для airflow)

Папка query содержит запросы по заданию 3 в файле query_sql (SQL_1_date_city, SQL_2_date_device, SQL_3_city_device).

Папка db_dump содержит дамп базы данных по скрипту iter_date_range_download.py (с 08.03.2022 - 07.04.2022).

В папку logs сохраняются логи.

Папка draft для чернового варианта загрузки.
