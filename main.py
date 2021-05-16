import psycopg2
from datetime import datetime, timedelta
import re
import csv
#import logger как журнал. Для записывания steps, которые виконались
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="mylog.log", #файл-обработчик
    level=logging.DEBUG, #уровень обработки - 10.
    format="%(asctime)s %(levelname)s at row #%(lineno)d %(message)s", datefmt="%m-%d-%Y %H:%M:%S",
)

#название таблицы, в которой будут храниться данные с Odata files
Table_name = "ZNO_RESULTS_19_20"

#название буферной таблицы, в которой будут хранится данные о том, какая последняя строчка доабвилась в Table_name
#из какого файла и сколько времени на это потратилось
Buffer_table = "Buffer_table"
Years = [2019, 2020]

def custom_query(cursor):
    query = open('queries/custom_query.sql', encoding='utf8')
    cursor.execute(query.read())
    data = cursor.fetchall()
    file_result = 'queries/custom_query_result.csv'
    with open(file_result, 'w') as f:
        f.write('\n'.join(['The worst grade for Hist test in %s is %s' % (i,j) for (i, j) in data]))
    logger.info("COPY TO CSV SUCCESSFUL")
    return file_result


def prepare_insert(row, year):
    #немного форматируются данные: там, где был результат зно 129,0 становится 129.
    buffer = [row[el].replace("'", "`").split(",")[0] for el in row]
    #добавляем столбец с годом, чтобы потом понимать с какого файла загрузились данные.
    buffer.append(year)
    insert_buffer = f"insert into {Table_name} values" + str(tuple(buffer))
    #убираем кавычки перед и после слова null, чтобы бд нормально распознала значение null не как строчку
    insert_query = re.sub(r"'null'", "null", insert_buffer)
    return insert_query


def get_previous_run_time_buffer(cursor):
    try:
        cursor.execute(f"SELECT execute_time FROM {Buffer_table};")
        buffer = cursor.fetchall()
        previous_run_work_time = buffer[0][0]
    except Exception as e:
        logger.info(f"We can not get data from {Buffer_table}: {e}")
        previous_run_work_time = None
    return timedelta(microseconds=previous_run_work_time)

def create_tables(conn, cursor):
    logger.info("Creating tables")
    #запросы, записанные по созданию таблиц считываются и создаются таблицы
    with open("queries/CREATE_BUFFER_TABLE.sql") as create_file:
        create_buffer_table = create_file.read().format(table_name=Buffer_table)
    with open("queries/CREATE_TABLE.sql") as create_file:
        create_zno_table = create_file.read().format(table_name=Table_name)
    try:
        cursor.execute(create_zno_table)
    except Exception as e:
        logger.error(f"Table {Table_name}, {e}")
    try:
        cursor.execute(create_buffer_table)
        #вставляем начальные (стартовые значения) в табличку buffer по 0, потому что мы еще не запускали программу
        cursor.execute(f"INSERT INTO {Buffer_table} VALUES (0, 0, 0)")
    except Exception as e:
        logger.error(f"{e}")
    #выполнение программы
    conn.commit()
    logger.info("Tables ZNO and Buffer created")



def populate(conn, cursor, csv_filename, year, last_row_number, start_time):
    previous_time = start_time
    logger.info(f"Inserting data from {csv_filename}")

    with open(csv_filename, encoding="cp1251") as csv_file:
        file_reader = csv.DictReader(csv_file, delimiter=";")
        i = 0
        #проганяем по циклу табличку. Заносим данные и каждую n-ую строчку считаем и заносим данные в буферную таблицу.
        for row in file_reader:

            i += 1

            #проверяем, на какой строчке закончили вставлять данные в таблицу и продолжай с того на чем stopped
            if i <= last_row_number:
                continue

            #воспользуемся функцией, где мы форматировали данные
            insert_zno_query = prepare_insert(row, year)
            try:
                cursor.execute(insert_zno_query)
            except Exception as e:
                #делаем откат у разі помилки
                logger.error(f"Smth went wrong details ->: {e}")
                conn.rollback()
                return 1


            if i % 100 == 0:
                now = datetime.now()
                try:
                    cursor.execute(
                        f"UPDATE {Buffer_table} SET last_row_num={i}, year_of_zno={year}, "
                        "execute_time=execute_time+"
                        f"{(now - previous_time).microseconds};"
                    )

                    conn.commit()

                    print('added ', i, ' rows')
                except Exception as e:
                    logger.error(f"Connection with db is broken: {e}")
                    conn.rollback()
                    return 1
                previous_time = now

        conn.commit()

    logger.info(f"Inserting from {csv_filename} is finished")



def main():
    #визначаємо начальное время и запишем в логгер
    start_time = datetime.now()
    logger.info(f"Start time {start_time}")

    #подключаемся к бд
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        host="192.168.99.100",
        password="postgres",
        #password="xu40e5",
        #host="localhost",
    )
    cursor = conn.cursor()

    #создаем таблицы
    create_tables(conn, cursor)

    try:
        cursor.execute(f"SELECT * FROM {Buffer_table};")
        buffer = cursor.fetchall()
        year_of_zno = buffer[0][0]
        last_row_number = buffer[0][1]
    except Exception as e:
        logger.warning(f"We can not get data from {Buffer_table}: {e}")
        year_of_zno = Years[0]
        last_row_number = 0

    conn.commit()

    logger.info(f"Starting inserting from {last_row_number} row from file for {year_of_zno} year")

    if year_of_zno:
        index_year = Years.index(year_of_zno)
        for year in Years[index_year:]:
            populate(conn, cursor, f"Odata{year}File.csv", year, last_row_number, start_time)
            last_row_number = 0
    else:
        for year in Years:
            populate(conn, cursor, f"Odata{year}File.csv", year, last_row_number, start_time)
            last_row_number = 0

    file_result = custom_query(cursor)
    print('Custom query result is stored in %s' % file_result)

    inserting_time = get_previous_run_time_buffer(cursor)

    end_time = datetime.now()

    logger.info(f"End time {end_time}")
    logger.info(f"Inserting executing time {inserting_time}")
    cursor.close()
    conn.close()
    logger.info("The end. Program is finished")


if __name__ == "__main__":
    main()
