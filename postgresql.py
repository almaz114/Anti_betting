
import psycopg2
from psycopg2 import Error

try:
    # Подключение к существующей базе данных
    connection = psycopg2.connect(user="almaz",
                                  # пароль, который указали при установке PostgreSQL
                                  password='02153',
                                  host="localhost",
                                  port="5432",
                                  database="almaz_db")

    # Курсор для выполнения операций с базой данных
    cursor = connection.cursor()
    # Распечатать сведения о PostgreSQL
    print("Информация о сервере PostgreSQL")
    print(connection.get_dsn_parameters(), "\n")
    # Выполнение SQL-запроса
    with open('files//result_table_2.csv', 'r', encoding="utf8") as file:
        # Notice that we don't need the `csv` module.
        next(file)  # Skip the header row.
        cursor.copy_from(file, 'table_1', sep='|')

    connection.commit()

    # cursor.execute(query)

    # Получить результат
    # record = cursor.fetchone()
    # all = cursor.fetchall()
    # connection.commit()
    # print("Вы подключены к - ", record, "\n")

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
# finally:
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Соединение с PostgreSQL закрыто")
