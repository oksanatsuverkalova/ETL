import pandas as pd
import psycopg2

conn_string_a = "host='192.168.99.100' port=54320 dbname='my_database' user='root' password='postgres'"  # postgres-источник
conn_string_b = "host='192.168.99.100' port=5433 dbname='my_database' user='root' password='postgres'"  # postgres-приемник
tables_list = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']  # cписок таблиц

#курсор
with psycopg2.connect(conn_string_a) as conn, conn.cursor() as cursor:
    # запрос к БД
    query = """
    SELECT * 
    FROM pg_catalog.pg_tables pt  
    WHERE schemaname = 'public'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    for row in result:
        tables_list.append(row[1])

for table in tables_list:
    with psycopg2.connect(conn_string_a) as conn, conn.cursor() as cursor:
        with open('resultsfile.csv', 'w') as f:
            q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            cursor.copy_expert(q, f)

    # запись данных в postgres-приемник
    with psycopg2.connect(conn_string_b) as conn, conn.cursor() as cursor:
        with open('resultsfile.csv', 'r') as f:
            q = f"COPY {table} FROM STDIN WITH DELIMITER ',' CSV HEADER;"
            cursor.copy_expert(q, f)