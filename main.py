import sqlite3
import pymysql

# Основные данные для работы с базами данных
# SQL_LITE_STR = r'C:\Users\vital\AppData\Roaming\MKS\data.sqlite'
SQL_LITE_STR = 'data.sqlite'
MY_SQL_IP  = '********'
MY_SQL_USER = '********'
MY_SQL_PASS = '********'
MY_SQL_DB_TEST = 'test_imp'
MY_SQL_DB_WORK = 'instrumentoz_zubr'

# Создадим соединения и курсоры+
# К базе данных поставщика
try:
    conn_sqlite = sqlite3.connect(SQL_LITE_STR)
    cursor_sqllite = conn_sqlite.cursor()
except:
    print('Не удалось подключиться к базе поставщика')
    conn_sqlite.close()
    raise
else:
    print('Подключение к БД поставщика удалось')

# К базе данных магазина
try:
    conn_mysql = pymysql.connect(MY_SQL_IP, MY_SQL_USER,MY_SQL_PASS,MY_SQL_DB_TEST)
    cursor_mysql = conn_mysql.cursor()
except:
    print('Не удалось подключиться к базе магазина')
    conn_mysql.close()
    raise
else:
    print('Подключение к БД магазина удалось')

# тексты sql команд

# Сколько товаров в БД поставщика
get_count_provider = """SELECT 
	Count(*)
	FROM ClPrice c	
	JOIN Price p 
		ON c.KodTov=p.KodTov"""

# Получение данных из базы данных поставщика
get_data = """SELECT p.artikul as model,
CASE WHEN c.NoSkidka=1 THEN c.Cena ELSE (c.Cena*(100-(SELECT Proc_Skid from Clients limit 1))/100 ) END as price, 
CASE WHEN ost=1 THEN 100 ELSE 0 END as quantity,
trim(Tov_Name) as name,
KolvoMin as minimum,
trim(model) as sku,
trim(Brand_Nm) as Brand_Nm,
TovGroup
	FROM ClPrice c	
	JOIN Price p 
		ON c.KodTov=p.KodTov"""

# Текст запроса на вставку во временную таблицу БД магазина
insert_into_temp = '''insert into test_imp.import_data values( Null, %s,%s,%s,%s,%s,%s,%s,%s);'''

# текст запроса - сколько получилось в БД магазина товара во временной таблице
count_item_temp = '''select COUNT(*) FROM  test_imp.import_data'''

count_products = 'select count(*) from instrumentoz_zubr.product'



# реализация
# Напечатем количество товара в БД у поставщика
try:
    cursor_sqllite.execute(get_count_provider)
except:
    print("Не удалось выплнить запрос 'Напечатем количество товара в БД у поставщика' ")
    conn_mysql.close()
    conn_sqlite.close()
    raise
else:
    print(f'В базе данных поставщика {cursor_sqllite.fetchone()} элементов')

# Получим эти данные
try:
    cursor_sqllite.execute(get_data)
    new_data = cursor_sqllite.fetchall()
except:
    print("Не удалось выплнить запрос на получение данных у поставщика ")
    conn_mysql.close()
    conn_sqlite.close()
    raise


# Очистим временную таблицу в БД магазина
try:
    cursor_mysql.execute("""TRUNCATE test_imp.import_data""")
except:
    print("Не удалось очистить временную таблицу магазина ")
    conn_mysql.close()
    conn_sqlite.close()
    raise

# Загрузим данные во временную таблицу БД магазина
try:
    cursor_mysql.executemany(insert_into_temp,new_data)
except:
    print("Не удалось загрузить данные' ")
    conn_mysql.close()
    conn_sqlite.close()
    raise
else:
    conn_mysql.commit()

# Сообщим сколкьо перенеслось позиций во временную базу магазина
try:
    cursor_mysql.execute(count_item_temp)
except:
    print("Не удалось получить данные из временной таблицы магазина' ")
    conn_mysql.close()
    conn_sqlite.close()
    raise
else:
    print(f'Во временной базе магазина {cursor_mysql.fetchone()} позиций')

# Посмотрим сколько позиций в БД магазина до изменений
try:
    cursor_mysql.execute(count_products)
except:
    print("Не удалось получить данные из БД магазина' ")
    conn_mysql.close()
    conn_sqlite.close()
    raise
else:
    print(f'Во  базе магазина {cursor_mysql.fetchone()} позиций')

# Выполним процедуры в БД магазина
try:
    cursor_mysql.execute('call instrumentoz_zubr.add_products()')
except:
    print("Не удалось выполнить процедуру обновления товаров")
    conn_mysql.close()
    conn_sqlite.close()
    raise
else:
    print('Процедура обновления успешно завершена')

# Выполним процедуру обновления цен и остатков
try:
    cursor_mysql.execute('call instrumentoz_zubr.update_price_count()')
except:
    print("Не удалось выполнить процедуру обновления товаров")
    conn_mysql.close()
    conn_sqlite.close()
    raise
else:
    print('Процедура обновления цен и остатокв')

# Посмотрим сколько позиций в БД магазина после изменений
try:
    cursor_mysql.execute(count_products)
except:
    print("Не удалось получить данные из БД магазина' ")
    conn_mysql.close()
    conn_sqlite.close()
    raise
else:
    print(f'Во  базе магазина {cursor_mysql.fetchone()} позиций после изменений')

print('Работа завершена')
conn_mysql.close()
conn_sqlite.close()
