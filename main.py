import os

import psycopg2
import csv
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# Параметры подключения к БД
conn = psycopg2.connect(
    host=os.getenv("DATABASES_HOST"),
    database=os.getenv("DATABASES_NAME"),
    user=os.getenv("DATABASES_USER"),
    password=os.getenv("DATABASES_PASSWORD"),
    port=os.getenv("DATABASES_PORT")
)

# Создание курсора для выполнения запросов
cur = conn.cursor()

# SQL-запрос для получения списка клиентов с номером кредита, у которых баланс больше 1000
query = """
    SELECT
        CONCAT(c.first_name, ' ', c.last_name) AS client_name,
        cr.credit_number
    FROM
        client c
    JOIN
        relation r ON c.id = r.client
    JOIN
        credit cr ON r.credit = cr.id
    WHERE
        cr.balance > 1000
"""

# Выполнение запроса
cur.execute(query)
rows = cur.fetchall()

# Получение текущей даты для имени файла
current_date = datetime.now().strftime("%d.%m.%Y")
file_name = f"report_{current_date}.csv"

# Запись результатов в CSV файл
with open(file_name, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Запись заголовков
    writer.writerow(["Client Name", "Credit Number"])
    # Запись данных
    writer.writerows(rows)

# Закрытие соединения
cur.close()
conn.close()

print(f"Файл {file_name} успешно создан.")

