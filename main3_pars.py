import requests, orjson, time
import sqlite3
from bs4 import BeautifulSoup

db=sqlite3.connect('hh_parsing.db')
cursor = db.cursor()

cursor.execute('CREATE TABLE IF NOT EXISTS vacancies (company_name TEXT, position TEXT, job_description TEXT, key_skills TEXT)')
db.commit()
sql_query = "INSERT INTO vacancies (company_name , position , job_description , key_skills ) VALUES (?, ?, ?, ?)"

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
adr= 'https://hh.ru/search/vacancy?no_magic=true&L_save_area=true&text=python+middle+developer&excluded_text=&area=1&salary=&currency_code=RUR&experience=doesNotMatter&order_by=relevance&search_period=0&items_on_page=100'
# получаем ответ на запрошенные 100 вакансий
result = requests.get(adr, headers=headers)

if result.ok:
    soup = BeautifulSoup(result.content.decode(), 'lxml')
    # hh в явном виде выдаёт только 20 ссылок, а весь результат поиска находится в формате json по тегу template. Без декодирования не работает
    templates = orjson.loads(soup.find("template").contents[0].encode('utf-8'))
    i=1
    # Весь объект - справочник со сложной структурой. Но все вакансии с кратким описанием находим в одном из узлов
    for vacancy in templates['vacancySearchResult']['vacancies']: # для каждой вакансии из поискового результата
        url = vacancy['links']['desktop'] # Обрабатываем отдельными запросами 
        vac_result = requests.get(url, headers=headers)
        # Печать обработанной ссылки 
        print(i, url, vac_result.status_code) 
        if vac_result.ok:
            soup = BeautifulSoup(vac_result.content.decode(), 'lxml')
            position = soup.find('h1').text
            company_name = soup.find('a', attrs={'data-qa': 'vacancy-company-name'}).text
            job_description = soup.find('div', attrs={'data-qa': 'vacancy-description'}).text
            # Ключевые навыки достаём парсингом в список`
            ks_list = soup.findAll('div', attrs={'data-qa': 'bloko-tag bloko-tag_inline skills-element'})
            # для записи в базу склеиваем навыки в строку
            key_skills = ', '.join([i.text for i in ks_list])
            # сохраняем найденные данные в таблицу
            try:
                cursor.execute(sql_query,[company_name , position , job_description , key_skills])
                db.commit()
            except:
                print('Ошибка записи в базу данных!')
        # пауза для защиты от блокировки
        time.sleep(1)
        i +=1
else:
    print(f'Error! Status {result.status_code}')
cursor.close()
db.close() # Правило хорошего кода
