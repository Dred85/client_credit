import os
import psycopg2
from unittest.mock import MagicMock, patch
from datetime import datetime
from main import get_db_connection, fetch_client_credit_data,  write_to_csv
from fill import get_db_connection

# Тест на функцию получения соединения с БД
def test_get_db_connection(mocker):
    mock_connect = mocker.patch('psycopg2.connect')
    mock_connect.return_value = MagicMock()  # Имитация подключения

    conn = get_db_connection()
    assert conn is not None
    mock_connect.assert_called_once_with(
        host=os.getenv("DATABASES_HOST"),
        database=os.getenv("DATABASES_NAME"),
        user=os.getenv("DATABASES_USER"),
        password=os.getenv("DATABASES_PASSWORD"),
        port=os.getenv("DATABASES_PORT")
    )

def test_get_db_connection_failure(mocker):
    mock_connect = mocker.patch('psycopg2.connect')
    mock_connect.side_effect = psycopg2.DatabaseError("Ошибка подключения")

    conn = get_db_connection()
    assert conn is None  # Ожидаем, что соединение будет None

# Тест на получение данных о клиентах и кредитах
def test_fetch_client_credit_data(mocker):
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value

    mock_cursor.fetchall.return_value = [("John Doe", "CR123456")]
    result = fetch_client_credit_data(mock_conn)

    assert result == [("John Doe", "CR123456")]
    mock_cursor.execute.assert_called_once()  # Убедимся, что запрос выполнен

def test_fetch_client_credit_data_error(mocker):
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    mock_cursor.execute.side_effect = psycopg2.Error("Ошибка выполнения запроса")

    result = fetch_client_credit_data(mock_conn)

    assert result == []  # Ожидаем, что при ошибке будет возвращен пустой список

# Тест на запись в CSV файл
def test_write_to_csv(mocker):
    mock_open = mocker.patch("builtins.open", mocker.mock_open())
    rows = [("Client Name", "Credit Number")]

    write_to_csv(rows)

    mock_open.assert_called_once_with(f"report_{datetime.now().strftime('%d.%m.%Y')}.csv", mode='w', newline='')
    handle = mock_open()
    handle.write.assert_any_call("Client Name, Credit Number")  # Проверяем заголовок
    handle.writerows.assert_called_once_with(rows)  # Проверяем, что данные записаны

def test_write_to_csv_error(mocker):
    mock_open = mocker.patch("builtins.open", side_effect=Exception("Ошибка записи"))
    rows = [("John Doe", "CR123456")]

    write_to_csv(rows)  # Ожидаем, что в случае ошибки не будет выброшено исключение

    mock_open.assert_called_once()

