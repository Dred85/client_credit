import cx_Oracle
import csv
from datetime import datetime

# Параметры подключения к БД
dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='xe')
connection = cx_Oracle.connect(user='system', password='oracle', dsn=dsn_tns)

# Формирую SQL-запрос
query = """
    SELECT 
        C.First_Name || ' ' || C.Last_Name AS Client_Name,
        CR.Credit_Number
    FROM
        CLIENT C
    JOIN
        RELATION R ON C.id = R.Client
    JOIN
        CREDIT CR ON CR.id = R.Credit
    WHERE
        CR.Balance > 1000
"""

# Получаю данные
cursor = connection.cursor()
cursor.execute(query)
results = cursor.fetchall()

# Получаю текущую дату для имени файла
current_date = datetime.now().strftime("%d.%m.%Y")
filename = f"report_{current_date}.csv"

# Сохраняю данные в CSV файл
with open(filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    # Записываю заголовки
    writer.writerow(["Client Name", "Credit Number"])
    # Записываю строки данных
    for row in results:
        writer.writerow(row)

# Закрываю курсор и соединение
cursor.close()
connection.close()

print(f"Данные успешно сохранены в файл {filename}")
