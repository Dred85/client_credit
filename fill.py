import os
import psycopg2
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

def get_db_connection():
    """Создает и возвращает подключение к базе данных."""
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

def create_tables(conn):
    """Создает таблицы CLIENT, CREDIT и RELATION."""
    create_client_table = """
    CREATE TABLE IF NOT EXISTS client (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        middle_name VARCHAR(50)
    );
    """

    create_credit_table = """
    CREATE TABLE IF NOT EXISTS credit (
        id SERIAL PRIMARY KEY,
        credit_number VARCHAR(20),
        amount DECIMAL(10, 2),
        balance DECIMAL(10, 2)
    );
    """

    create_relation_table = """
    CREATE TABLE IF NOT EXISTS relation (
        id SERIAL PRIMARY KEY,
        client INT REFERENCES client(id),
        credit INT REFERENCES credit(id)
    );
    """

    with conn.cursor() as cur:
        cur.execute(create_client_table)
        cur.execute(create_credit_table)
        cur.execute(create_relation_table)
        conn.commit()

def insert_data(conn):
    """Вставляет 5 значений в каждую таблицу."""
    insert_client_data = """
    INSERT INTO client (first_name, last_name, middle_name) VALUES
    ('Roberto', 'Carlos', 'Michael'),
    ('Garet', 'Beil', 'Franco'),
    ('Lionell', 'Messi', 'David'),
    ('Luis', 'Suares', 'Carlo'),
    ('Zlatan', 'Ibragimovich', 'James');
    """

    insert_credit_data = """
    INSERT INTO credit (credit_number, amount, balance) VALUES
    ('CR123456', 5000.00, 1500.00),
    ('CR123457', 2000.00, 500.00),
    ('CR123458', 7000.00, 2500.00),
    ('CR123459', 3000.00, 800.00),
    ('CR123460', 4000.00, 1200.00);
    """

    insert_relation_data = """
    INSERT INTO relation (client, credit) VALUES
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4),
    (5, 5);
    """

    with conn.cursor() as cur:
        cur.execute(insert_client_data)
        cur.execute(insert_credit_data)
        cur.execute(insert_relation_data)
        conn.commit()

def main():
    """Основная функция для создания таблиц и добавления данных."""
    conn = get_db_connection()
    if conn:
        try:
            create_tables(conn)
            insert_data(conn)
            print("Таблицы созданы и данные добавлены.")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
