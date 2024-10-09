import os
from datetime import datetime
from unittest.mock import MagicMock

import psycopg2

from main import fetch_client_credit_data, get_db_connection, write_to_csv


def test_get_db_connection(mocker):
    """
    Тест проверяет успешное подключение к базе данных.
    Мокирует вызов psycopg2.connect и проверяет, что соединение установлено с правильными параметрами.
    """
    mock_connect = mocker.patch("psycopg2.connect")
    mock_connect.return_value = MagicMock()

    conn = get_db_connection()
    assert conn is not None
    mock_connect.assert_called_once_with(
        host=os.getenv("DATABASES_HOST"),
        database=os.getenv("DATABASES_NAME"),
        user=os.getenv("DATABASES_USER"),
        password=os.getenv("DATABASES_PASSWORD"),
        port=os.getenv("DATABASES_PORT"),
    )


def test_fetch_client_credit_data():
    """
    Тест проверяет успешное выполнение SQL-запроса на получение данных о клиентах.
    Мокирует подключение и курсор, проверяя, что данные корректно извлекаются.
    """
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

    # Имитирую возвращаемые данные
    mock_cursor.fetchall.return_value = [("Roby Williams", "CR123456")]

    result = fetch_client_credit_data(mock_conn)
    assert result == [("Roby Williams", "CR123456")]
    mock_cursor.execute.assert_called_once()  # Убеждаюсь, что SQL-запрос был выполнен


def test_write_to_csv(mocker):
    """Тест проверяет успешное создание csv файла"""
    mock_open = mocker.patch("builtins.open", mocker.mock_open())
    rows = [("Pele", 1000), ("Maradonna", 2000)]
    # Вызваю функцию записи
    write_to_csv(rows)
    # Формирую ожидаемое имя файла
    expected_file_name = f"report_{datetime.now().strftime('%d.%m.%Y')}.csv"
    # Проверяю, что файл был создан с правильным именем
    mock_open.assert_called_once_with(expected_file_name, mode="w", newline="")
    if os.path.exists(expected_file_name):
        os.remove(expected_file_name)
        print(f"{expected_file_name} успешно создан и удалён для проверки.")
    else:
        print(f"Файл {expected_file_name} не был создан.")


def test_get_db_connection_failure(mocker):
    """
    Тест проверяет обработку ошибки при неудачном подключении к базе данных.
    Мокирует вызов psycopg2.connect и имитирует выброс исключения.
    """
    mock_connect = mocker.patch("psycopg2.connect")
    mock_connect.side_effect = psycopg2.DatabaseError("Ошибка подключения")

    conn = get_db_connection()
    assert conn is None
    mock_connect.assert_called_once()


def test_fetch_client_credit_data_error():
    """
    Тест проверяет, что функция корректно обрабатывает ошибку при выполнении SQL-запроса.
    Мокирует выполнение запроса и выбрасывает ошибку.
    """
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    mock_cursor.execute.side_effect = psycopg2.Error("Ошибка выполнения SQL-запроса")

    result = fetch_client_credit_data(mock_conn)
    assert result == []
    mock_cursor.execute.assert_called_once()


def test_fetch_client_credit_data_empty():
    """
    Тест проверяет, что функция корректно обрабатывает ситуацию, когда данные отсутствуют.
    Мокирует подключение и возвращает пустой результат.
    """
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

    mock_cursor.fetchall.return_value = []

    result = fetch_client_credit_data(mock_conn)
    assert result == []
    mock_cursor.execute.assert_called_once()


