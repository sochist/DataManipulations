import requests, time
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime, timedelta
import configparser, os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
import logging
import orjson, zipfile, pandas

"""
DAG Составление ТОП-10 наиболее востребованных ключевых навыков middle python developer в компаниях, занимающихся деятельностью в сфере телекоммуникаций.
Конфигурация извлекается из TopSkills.cfg
Этапы ELT:
-Создание/инициализация таблиц
=скачивание справочника ЕГРЮЛ - выборка телеком-компаний - запись массива в таблицу
=Чтение справочника индустрий hh - фильтрация индустрии по заданному условию 
-Чтение вакансий по заданному условию - определение индустрии компании - Выборка ключевых навыков из вакансий с фильтром по индустрии
-Подсчет, сортировка и запись ТОП-10 наиболее востребованных ключевых навыков в таблицу

"""
default_args = {
    'owner': 'boss',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
config = configparser.ConfigParser()
logger = logging.getLogger(__name__)

def database_reset(SH):
    # Создание и инициализация таблиц
    SH.run('CREATE TABLE IF NOT EXISTS telecom_companies (inn WARCHAR(20), name WARCHAR(200), okved_code WARCHAR(20), kpp WARCHAR(20), record_date WARCHAR(20))')
    SH.run('CREATE TABLE IF NOT EXISTS top_skills (name WARCHAR(50), counts integer)')
    SH.run('delete from top_skills')
    SH.run('CREATE TABLE IF NOT EXISTS buffer (name WARCHAR(50), val text)')
    SH.run('delete from buffer')

def load_industryes(SH):
    # получаем справочник индустрий hh
    FilteredIndustryList =[]
    result = requests.get(f'{config["API_URLS"]["IndustriesURL"]}')
    if result.ok:
        ind_js=result.json()
        # Ищем подстроку фильтра в названиях групп индустрий
        for ind in  ind_js:
            if config["TOPSKILLS"]["IndustryFilter"] in ind['name']:
                for chld in ind['industries']:
                    FilteredIndustryList.append(chld['id'])
        if len(FilteredIndustryList) ==0:
            # Ищем подстроку фильтра в названиях индустрий
            for ind in  ind_js:
                for chld in ind['industries']:
                    if config["TOPSKILLS"]["IndustryFilter"] in chld['name']:
                        FilteredIndustryList.append(chld['id'])
        logger.info(f"Отфильтровано {len(FilteredIndustryList)} индустрий")
        if len(FilteredIndustryList) >0:
            logger.info(f"Коды индустрий: {FilteredIndustryList}")
        else: 
            # Фильтр не даёт результатов. Дальше искать нечего
            logger.error("Не найдены индустрии по заданному фильтру.")
            raise ValueError("Не найдены индустрии по заданному фильтру.")
    else:
        # нет связи, либо бан.
        logger.error("Ошибка загрузки справочника индустрий")
        raise ValueError("Ошибка загрузки справочника индустрий")
    # Результат отбора индустрий сохраняем в буферную таблицу
    SH.insert_rows(
        table='buffer',
        rows=[['FilteredIndustryList',[','.join(FilteredIndustryList)]]],
        target_fields=['name', 'val'],
    )

def load_vacancyes(SH):
    api_params = {'text':config["TOPSKILLS"]["VacansyFilter"],
              'search_field':'name',
              'per_page': 100,
              'page':0}
    
    txt = SH.get_records(sql="SELECT val FROM buffer where name = 'FilteredIndustryList'")[0][0]
    FilteredIndustryList = txt.split(',')
    keyskills_list=[]
    request_count = 1
    vac_count = 0
    result = requests.get(config["API_URLS"]["VacanciesURL"], api_params)
    while result.ok:
        for vacancy in result.json().get('items'): # для каждой вакансии из поискового результата
            vac_result = requests.get(vacancy['url'])
            if vac_result.ok:
                vac_json=vac_result.json()
                ks_list = vac_json['key_skills']
                if len(ks_list) > 0 and 'url' in vac_json['employer'].keys(): # ищем вакансии с ключевыми навыками и наличием ссылки
                    company_result = requests.get(vac_json['employer']['url']) 
                    if company_result.ok:
                        # определяем соответствие индустрии заданному фильтру
                        company_info = company_result.json()
                        logger.info(f"Получена информация о компании {company_info['name']} {[ind['id'] for ind in company_info['industries']]}")
                        filtered = False
                        for ind in company_info['industries']: 
                            if ind['id'] in FilteredIndustryList:
                                filtered = True
                                break
                        if filtered:
                            vac_count += 1
                            logger.info(f"добавление навыков {ks_list}")
                            [keyskills_list.append(ks['name']) for ks in ks_list ]
                    else:
                        logger.info(f'Запрос {vac_json["employer"]["url"]} не завершен. {company_result.reason}')
            else:
                logger.info(f'Запрос {request_count}: {vacancy["url"]} не завершен. {vac_result.reason}')
            time.sleep(0.4)
            request_count +=1
        api_params['page'] +=1
        result = requests.get(config["API_URLS"]["VacanciesURL"], api_params)
    if len(keyskills_list) >0:
    # Результат отбора навыков сохраняем в буферную таблицу
        SH.insert_rows(
            table='buffer',
            rows=[['keyskills_list',','.join(keyskills_list)]],
            target_fields=['name', 'val'],
        )
        logger.info(f'Обработка завершена. {result.reason}')
        logger.info(f'Отобрано {len(keyskills_list)} неуникальных ключевых навыков из {vac_count} вакансий')
    else: 
        logger.error("Не найдены вакансии по заданному фильтру.")
        raise ValueError("Не найдены вакансии по заданному фильтру.")

def get_egrul(SH):
    if not os.path.isfile(f'{config["DATA"]["Data_Path"]}/{config["EGRUL"]["DataFileName"]}'):
        command = f'wget {config["EGRUL"]["DataURL"]}/{config["EGRUL"]["DataFileName"]} -O {config["DATA"]["Data_Path"]}/{config["EGRUL"]["DataFileName"]}'
        os.system(command)
        logger.info(f'{config["DATA"]["Data_Path"]} загружен')
        SH.run('delete from telecom_companies')

    else:
        logger.info(f'{config["DATA"]["Data_Path"]} загружен ранее')

def save_top_skills(SH):
    txt = SH.get_records(sql="SELECT val FROM buffer where name = 'keyskills_list'")[0][0]
    KS = txt.split(',')
    df = pandas.DataFrame({'name':KS})
    # Подсчет количества, сортировка и отбор первых N 
    TopSkils = df.value_counts(subset=None, normalize=False, sort=True, ascending=False, dropna=True).head(int(config["TOPSKILLS"]["TopSkillsCount"])).reset_index()
    fields = ['name', 'counts']
    SH.insert_rows(
        table='top_skills',
        rows=TopSkils.values.tolist(),
        target_fields=fields,
    )
    
def save_filtered_company(SH):
    records = 0
    fields = ['inn', 'name' , 'okved_code' , 'kpp' , 'record_date']
    with zipfile.ZipFile(f'{config["DATA"]["Data_Path"]}/{config["EGRUL"]["DataFileName"]}', mode="r") as archive: 
        for js in  archive.namelist(): # Проходим по всем файлам в архиве
            with archive.open(js,'r') as txt: # Открываем не распаковывая из архива в физический файл
                spr = orjson.loads(txt.read())  # внутри находим список словарей
            comp2load =[]
            for itm in  spr: # каждый словарь списка - информация о компании
                if 'СвОКВЭД' in itm['data']: # не каждый словарь списка содержит данные об ОКВЭД
                    if 'СвОКВЭДОсн' in itm['data']['СвОКВЭД']:  # каждый словарь с данными ОКВЭД должен содержать сведения об основном коде
                        okv = itm['data']['СвОКВЭД']['СвОКВЭДОсн']  # данные об основном коде - тоже словарь
                        if okv['КодОКВЭД'][:len(config["EGRUL"]["IndustryFilter"])] == config["EGRUL"]["IndustryFilter"]:   # Фильтр из параметров
                            records += 1
                            # список найденных компаний
                            comp2load.append([itm['inn'],itm['name'],okv['КодОКВЭД'],itm['kpp'],okv['ГРНДата']['ДатаЗаписи']])
            # запись в базу после обраотки файла
            SH.insert_rows(
                table='telecom_companies',
                rows=comp2load,
                target_fields=fields,
                )
            logger.info(f'{js} обработан')
    logger.info(f'Найдено {records} организаций')

with DAG(
    dag_id='FinalTesting',
    default_args=default_args,
    description='DAG for finsl project',
    start_date=datetime(2023, 7, 15, 8),
    schedule_interval='@daily'
) as dag:
    config.read('/home/db/airflow/dags/TopSkills.cfg')
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    init_db = PythonOperator(
        task_id='database_reset',
        python_callable=database_reset,
        op_kwargs={'SH':sqlite_hook}
    )

    get_file_egrul = PythonOperator(
        task_id='get_egrul',
        python_callable=get_egrul,
        op_kwargs={'SH':sqlite_hook},
    )

    filter_egrul = PythonOperator(
        task_id='save_filtered_company',
        python_callable=save_filtered_company,
        op_kwargs={'SH':sqlite_hook}
    )

    get_industryes = PythonOperator(
        task_id='load_industryes',
        python_callable=load_industryes,
        op_kwargs={'SH':sqlite_hook},
    )
    get_vacancyes = PythonOperator(
        task_id='load_vacancyes',
        python_callable=load_vacancyes,
        op_kwargs={'SH':sqlite_hook}
    )

    save_skills  = PythonOperator(
        task_id='save_top_skills',
        python_callable=save_top_skills,
        op_kwargs={'SH':sqlite_hook}
    )
    print_rows = SqliteOperator(
        task_id='print_rows',
        sqlite_conn_id='sqlite_default',
        sql='SELECT name, counts from top_skills'
    )

    init_db >> get_file_egrul >> filter_egrul
    init_db >> get_industryes >> get_vacancyes >> save_skills >> print_rows
