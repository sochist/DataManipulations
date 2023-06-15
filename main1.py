import sqlite3
import orjson

db=sqlite3.connect('hw1.db')
cursor = db.cursor()

cursor.execute('CREATE TABLE IF NOT EXISTS okved (code WARCHAR(20), parent_code WARCHAR(20), section WARCHAR(5), name WARCHAR(500), comment WARCHAR(500))')
db.commit()

i=0
with open(r'..\\okved_2.json','r',encoding='utf-8') as txt:
    spr = orjson.loads(txt.read()) # внутри список словарей

sql_query = """INSERT INTO okved (code, parent_code, section, name, comment) 
VALUES (:code, :parent_code, :section, :name, :comment)"""
i=len(spr)
cursor.executemany(sql_query,spr)
db.commit()
cursor.execute('SELECT count(*) from okved')
res = cursor.fetchall()
print(i,res[0] )     # дополнительная проверка
cursor.close()
db.close()   # Правило хорошего кода
