import requests, orjson
import sqlite3
from bs4 import BeautifulSoup
import asyncio
import time
from aiohttp import ClientSession

async def main_loop(ids):
    async with ClientSession('https://api.hh.ru/') as session:
        tasks = []
        for id in ids:
            tasks.append(asyncio.create_task(get_vacancy(id, session)))

        results = await asyncio.gather(*tasks)

        return results

async def get_vacancy(id, session):
    url = f'/vacancies/{id}'
    
    async with session.get(url=url) as response:
        vacancy_json = await response.json()
        return vacancy_json
    
db=sqlite3.connect('hh_pars_api.db')
cursor = db.cursor()

cursor.execute('CREATE TABLE IF NOT EXISTS vacancies (company_name TEXT, position TEXT, job_description TEXT, key_skills TEXT)')
db.commit()
sql_query = "INSERT INTO vacancies (company_name , position , job_description , key_skills ) VALUES (?, ?, ?, ?)"

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
adr= 'https://hh.ru/search/vacancy?no_magic=true&L_save_area=true&text=python+middle+developer&excluded_text=&area=1&salary=&currency_code=RUR&experience=doesNotMatter&order_by=relevance&search_period=0&items_on_page=100'

start = time.time()

result = requests.get(adr, headers=headers)
# получаем ответ на запрошенные 100 вакансий
vac_to_db=[]

if result.ok:
    soup = BeautifulSoup(result.content.decode(), 'lxml')
    # hh в явном виде выдаёт только 20 ссылок, а весь результат поиска находится в формате json по тегу template. Без декодирования не работает
    templates = orjson.loads(soup.find("template").contents[0].encode('utf-8'))
    i=0
    # Весь объект - справочник со сложной структурой. Но все вакансии с кратким описанием находим в одном из узлов
    vacancies_ids = [key for key in templates['userLabelsForVacancies'].keys()]

    vac_list = asyncio.run(main_loop(vacancies_ids[:50] )) # асинхронно собираем описания вакансий через api 1/2
    vac_list += asyncio.run(main_loop(vacancies_ids[-50:] )) # асинхронно собираем описания вакансий через api 2/2

    for i, vac_json in enumerate(vac_list): # формируем список для записи в таблицу
        if not "name" in vac_json:
            print(i, vac_json["errors"][0])
        else:
            position = vac_json['name']
            company_name = vac_json['employer']['name']
            job_description = vac_json['description']
            ks_list = vac_json['key_skills']
            # для записи в базу склеиваем навыки в строку
            key_skills = ', '.join([i['name'] for i in ks_list])
            vac_to_db.append([company_name , position , job_description , key_skills ])

    print(f'обработано {len(vac_to_db)} вакансий за { int(time.time()-start)} c')
    try:
        cursor.executemany(sql_query,vac_to_db)
        db.commit()
    except:
        print('Ошибка записи в базу данных!')
else:
    print(f'Error! Status {result.status_code}')
cursor.close()
db.close() # Правило хорошего кода


