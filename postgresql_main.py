
import psycopg2
from psycopg2 import Error
# import asyncio
from loguru import logger
from datetime import datetime
import os
import json
import time


# open config files for site/urls
if not os.path.exists("files//config.json"):
    logger.error(f"файл не сушествует по указанному пути files/config.json")
else:
    with open('files//config.json') as file:
        data_config = json.load(file)
        posgresql_config = data_config['postgresql_config']

        list_csv_files = posgresql_config['list_csv_files']
        database = posgresql_config['database']
        user_name_db = posgresql_config['user_name_db']
        password = posgresql_config['password']
        host = posgresql_config['host']
        port = posgresql_config['port']
        logger.info(f"{posgresql_config=}")
        # logger.info(f"{urls_bettings=}, {type(data_config)}")


def postgresql_import_csv(base_path: str, list_csv_files: list, user: str, password: str, host: str, port: str, database: str):
    """
    функция для импорта данных из файлов .csv в базу данных
    """
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(user=user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)

        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        # Распечатать сведения о PostgreSQL
        print("Информация о сервере PostgreSQL")
        print(connection.get_dsn_parameters(), "\n")

        # запуск цикла импорта каждого файла
        if len(list_csv_files) > 0:
            for i, value in enumerate(list_csv_files):
                # Выполнение SQL-запроса
                with open(base_path + value, 'r', encoding="utf8") as file:
                    next(file)  # Skip the header row.
                    cursor.copy_from(file, 'table_1', sep='|')

                connection.commit()
                time.sleep(2)
        else:
            logger.error(f"len of list_csv_files empty or other !!!")

        # cursor.execute(query)

        # Получить результат
        # record = cursor.fetchone()
        # all = cursor.fetchall()
        # connection.commit()
        # print("Вы подключены к - ", record, "\n")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        logger.info(f"Ошибка при работе с PostgreSQL", {error})
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")

# begin func
# postgresql_import_csv(base_path="files//", list_csv_files=list_csv_files, database=database, user=user_name_db, password=password, port=port, host=host)
