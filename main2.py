import sqlite3
import orjson, zipfile
import time

print('') # пустая строка для вывода процесса исполнения
db=sqlite3.connect('hw1.db')
cursor = db.cursor()

cursor.execute('CREATE TABLE IF NOT EXISTS telecom_companies (inn WARCHAR(20), name WARCHAR(200), okved_code WARCHAR(20), kpp WARCHAR(20), record_date WARCHAR(20))')
db.commit()
start = time.time()

sql_query = 'INSERT INTO telecom_companies (inn , name , okved_code , kpp , record_date) VALUES (?, ?, ?, ?, ?)'
cur_file=0
records = 0
with zipfile.ZipFile("..\\egrul.json.zip", mode="r") as archive: 
    files_total = len(archive.namelist())
    for js in  archive.namelist(): # Проходим по всем файла ав архиве
        if js[-5:]=='.json':  # Если это действительно то, что нам надо
            cur_file += 1
            with archive.open(js,'r') as txt: # Открываем не распаковывая из архива в физический файл
                spr = orjson.loads(txt.read())  # внутри находим список словарей
            for itm in  spr: # каждый словарь списка - информация о компании
                if 'СвОКВЭД' in itm['data']: # не каждый словарь списка содержит данные об ОКВЭД
                    if 'СвОКВЭДОсн' in itm['data']['СвОКВЭД']:  # каждый словарь с данными ОКВЭД должен содержать сведения об основном коде
                        okv = itm['data']['СвОКВЭД']['СвОКВЭДОсн']  # данные об основном коде - тоже словарь
                        if okv['КодОКВЭД'][:3] == '61.':   # телеком - и точка!
                            records += 1
                            # выполняем запрос с параметрами
                            try:
                                cursor.execute(sql_query,[itm['inn'],itm['name'],okv['КодОКВЭД'],itm['kpp'],okv['ГРНДата']['ДатаЗаписи']])
                            except:
                                print('Что-то пошло не так!')
                                db.close()
                                exit(1)
            db.commit()
            print('\033[F', f'Обработка {cur_file} файла из {files_total}. Найдено {records} компаний') # лог в одной строке экрана
end = time.time()
print(f'Обработка завершена за { int(end-start)} c!')
cursor.close()
db.close() # Правило хорошего кода