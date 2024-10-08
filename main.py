import os
import psycopg2
import csv
from datetime import datetime
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

def get_db_connection():
    """Создаю подключение к базе данных."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DATABASES_HOST"),
            database=os.getenv("DATABASES_NAME"),
            user=os.getenv("DATABASES_USER"),
            password=os.getenv("DATABASES_PASSWORD"),
            port=os.getenv("DATABASES_PORT")
        )
        return conn
    except psycopg2.DatabaseError as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def fetch_client_credit_data(conn):
    """Получаю список клиентов и их кредитные номера с балансом больше 1000."""
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
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Ошибка выполнения SQL-запроса: {e}")
        return []

def write_to_csv(rows):
    """Записываю данные в CSV файл с именем, содержащим текущую дату."""
    current_date = datetime.now().strftime("%d.%m.%Y")
    file_name = f"report_{current_date}.csv"

    try:
        with open(file_name, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Client Name", "Credit Number"])
            writer.writerows(rows)
        print(f"Файл {file_name} успешно создан.")
    except Exception as e:
        print(f"Ошибка записи в CSV файл: {e}")

def main():
    """Основная функция, управляющая процессом извлечения данных и их записи в CSV."""
    conn = get_db_connection()
    if conn:
        try:
            rows = fetch_client_credit_data(conn)
            if rows:
                write_to_csv(rows)
        finally:
            conn.close()

if __name__ == "__main__":
    main()

