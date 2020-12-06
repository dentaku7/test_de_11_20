import csv
import glob
import logging
import os
import random
from datetime import datetime
from pathlib import Path
from random import randrange
from uuid import uuid4

import psycopg2
from faker import Faker

from db_schemas import (TABLE_SCHEMAS, CREATE_TABLE)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Task1DataGenerator')

DATA_DIR = Path(os.environ["TASK_1_DATA_DIR"])

USERS_CSV = DATA_DIR / Path('users.csv')
LOGINS_CSV = DATA_DIR / Path('logins.csv')
OPERATIONS_CSV = DATA_DIR / Path('operations.csv')
ORDERS_CSV = DATA_DIR / Path('orders.csv')

CSV_OPTIONS = {'delimiter': ',',
               'quotechar': '\\',
               'quoting': csv.QUOTE_MINIMAL}

START_DATETIME = datetime.fromisoformat('2019-06-01')

FILES_FOR_TABLES = {
    'tb_users': USERS_CSV,
    'tb_logins': LOGINS_CSV,
    'tb_operations': OPERATIONS_CSV,
    'tb_orders': ORDERS_CSV
}

PG_SETTINGS = {
    "host": "db",
    "port": 5432,
    "user": "postgres",
    "password": "postgres"
}

USED_LOGINS = []

fake = Faker()


def get_unique_login():
    while True:
        login = fake.domain_word()
        if login not in USED_LOGINS:
            USED_LOGINS.append(login)
            return login


def generate_users(users):
    logger.info(f"Generating {users} users")
    with open(USERS_CSV, 'w') as csv_file:
        csv_writer = csv.writer(csv_file, **CSV_OPTIONS)
        for _ in range(users):
            row = [uuid4(),
                   fake.date_time_ad(start_datetime=START_DATETIME),
                   fake.country()]
            csv_writer.writerow(row)


def generate_logins(up_to_per_user):
    logger.info(f"Generating ~{up_to_per_user} logins per users")
    with open(USERS_CSV, 'r') as users_csv_file:
        users_csv = csv.reader(users_csv_file, **CSV_OPTIONS)
        with open(LOGINS_CSV, 'w') as logins_csv_file:
            csv_writer = csv.writer(logins_csv_file, **CSV_OPTIONS)
            for line in users_csv:
                for _ in range(randrange(up_to_per_user) + 1):
                    row = [line[0],  # user
                           get_unique_login(),
                           random.choice(['real', 'demo']),
                           fake.date_time_ad(start_datetime=datetime.fromisoformat(line[1]))]
                    csv_writer.writerow(row)


def generate_operations(up_to_per_login):
    logger.info(f"Generating ~{up_to_per_login} operations per login")
    with open(LOGINS_CSV, 'r') as logins_file:
        logins_csv = csv.reader(logins_file, **CSV_OPTIONS)
        with open(OPERATIONS_CSV, 'w') as operations_file:
            csv_writer = csv.writer(operations_file, **CSV_OPTIONS)
            for line in logins_csv:
                deposit_amount = randrange(2000)
                start_date = fake.date_time_ad(start_datetime=datetime.fromisoformat(line[3]))
                row = ['deposit',
                       start_date,
                       line[1],
                       deposit_amount]
                csv_writer.writerow(row)
                for _ in range(randrange(up_to_per_login) + 1):
                    row = [random.choice(['deposit', 'withdrawal']),
                           fake.date_time_ad(start_datetime=start_date),
                           line[1],  # login
                           round(random.random() * deposit_amount, 2)]
                    csv_writer.writerow(row)


def generate_orders(up_to_per_login):
    logger.info(f"Generating ~{up_to_per_login} orders per login")
    with open(OPERATIONS_CSV, 'r') as operations_file:
        operations_csv = csv.reader(operations_file, **CSV_OPTIONS)
        with open(ORDERS_CSV, 'w') as orders_file:
            csv_writer = csv.writer(orders_file, **CSV_OPTIONS)
            previous_login = None
            for line in operations_csv:
                login = line[2]
                if previous_login == login:
                    continue
                for _ in range(randrange(up_to_per_login)):
                    row = [login,
                           fake.date_time_ad(start_datetime=datetime.fromisoformat(line[1])),
                           round(random.uniform(-1, 1) * 1000, 2)]
                    csv_writer.writerow(row)
                previous_login = login


def get_pg_cursor(dbname):
    pg_connect = psycopg2.connect(dbname=dbname, **PG_SETTINGS)
    pg_connect.set_session(autocommit=True)
    return pg_connect.cursor()


def delete_files(path):
    file_list = glob.glob(os.path.join(path, "*"))
    for f in file_list:
        os.remove(f)


def generate_data(users,
                  logins_up_to_per_user,
                  operations_up_to_per_login,
                  orders_up_to_per_login):
    logger.info("Generating fake data for task 1")

    delete_files(DATA_DIR)

    generate_users(users=users)
    generate_logins(up_to_per_user=logins_up_to_per_user)
    generate_operations(up_to_per_login=operations_up_to_per_login)
    generate_orders(up_to_per_login=orders_up_to_per_login)

    for dbname, tables in TABLE_SCHEMAS.items():
        try:
            get_pg_cursor(None).execute(f"CREATE DATABASE {DB_NAME}")
        except Exception:
            pass

        for table_name, columns in tables.items():
            logger.info(f"Creating table {table_name}")
            sql_columns = ",\n".join(map(lambda x: " ".join(x), columns.items()))
            query = CREATE_TABLE.format(table=table_name, columns=sql_columns)
            try:
                get_pg_cursor(dbname).execute(query)
            except Exception:
                get_pg_cursor(dbname).execute(f"TRUNCATE TABLE {table_name}")

            logger.info(f"Loading data into table {table_name}")
            get_pg_cursor(dbname).copy_from(open(FILES_FOR_TABLES[table_name]), table_name, sep=',')

    logger.info("DONE")


if __name__ == '__main__':
    generate_data(users=1000,
                  logins_up_to_per_user=3,
                  operations_up_to_per_login=10,
                  orders_up_to_per_login=100)
