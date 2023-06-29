import requests, time
import sqlite3

db=sqlite3.connect('hh_api.db')
cursor = db.cursor()

cursor.execute('CREATE TABLE IF NOT EXISTS vacancies (company_name TEXT, position TEXT, job_description TEXT, key_skills TEXT)')
db.commit()
sql_query = "INSERT INTO vacancies (company_name , position , job_description , key_skills ) VALUES (?, ?, ?, ?)"

ENDPOINT = 'https://api.hh.ru/vacancies'
api_params = {'text':'python middle developer',
              'search_field':'name',
              'per_page': 100,
              'page':0}
start = time.time()
# получаем ответ на запрошенные 100 вакансий
result = requests.get(ENDPOINT, api_params)
vac_list=[]
if result.ok:
    i=0
    # Весь объект - справочник со сложной структурой. Но все вакансии с кратким описанием находим в одном из узлов
    for vacancy in result.json().get('items'): # для каждой вакансии из поискового результата
        vac_result = requests.get(vacancy['url'])
        if vac_result.ok:
            vac_json=vac_result.json()
            position = vac_json['name']
            company_name = vac_json['employer']['name']
            job_description = vac_json['description']
            ks_list = vac_json['key_skills']
            # для записи в базу склеиваем навыки в строку
            key_skills = ', '.join([i['name'] for i in ks_list])
            # сохраняем найденные данные в массив
            vac_list.append([company_name , position , job_description , key_skills ])
            i +=1
    print(f'обработано {i} вакансий за { int(time.time()-start)} c')
    try:
        cursor.executemany(sql_query,vac_list)
        db.commit()
    except:
        print('Ошибка записи в базу данных!')
else:
    print(f'Error! Status {result.status_code}')
cursor.close()
db.close() # Правило хорошего кода
