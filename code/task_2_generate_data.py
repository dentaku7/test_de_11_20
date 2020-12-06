import csv
import glob
import logging
import os
import random
import shutil
from datetime import datetime
from pathlib import Path
from random import randrange
from uuid import uuid4

import psycopg2
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Task2DataGenerator')

DATA_DIR = Path(os.environ["TASK_2_DATA_DIR"])

CSV_OPTIONS = {'delimiter': ',',
               'quotechar': '\\',
               'quoting': csv.QUOTE_MINIMAL}

START_DATETIME = datetime.fromisoformat('2019-01-01')

DB_NAME = 'task_2'

PG_SETTINGS = {
    "host": "db",
    "port": 5432,
    "user": "postgres",
    "password": "postgres"
}

TABLE_SCHEMA = """
CREATE TABLE tb_transactions (
    user_uid varchar(255),
    transaction_id varchar(255),
    transaction_date timestamp,
    transaction_type varchar(20),
    amount float
);
create index tb_transactions_transaction_id_index
    on tb_transactions (transaction_id);
"""


def generate_transactions(total_files, lines_per_file):
    fake = Faker()
    for file_no in range(total_files):
        total_lines_count = 0
        file_name = f'transactions_{file_no}.csv'
        logger.info(f"Saving file {file_name}")
        with open(DATA_DIR / Path(file_name), 'w') as csv_file:
            csv_writer = csv.writer(csv_file, **CSV_OPTIONS)
            while True:
                if total_lines_count >= lines_per_file:
                    break
                user_uid = uuid4()
                for i in range(randrange(10)):
                    transaction_type = random.choice(['order', 'commission'])
                    amount_scale = 10 if transaction_type == 'commission' else 1000
                    row = [user_uid,
                           uuid4(),
                           fake.date_time_ad(start_datetime=START_DATETIME),
                           transaction_type,
                           round(random.random() * amount_scale, 2)]
                    csv_writer.writerow(row)
                    total_lines_count += 1
                    if total_lines_count >= lines_per_file:
                        break


def get_pg_cursor(dbname):
    pg_connect = psycopg2.connect(dbname=dbname, **PG_SETTINGS)
    pg_connect.set_session(autocommit=True)
    return pg_connect.cursor()


def delete_files(path):
    file_list = glob.glob(os.path.join(path, "*"))
    for f in file_list:
        os.remove(f)


def generate_data(total_files, lines_per_file, changed_lines_per_files):
    delete_files(DATA_DIR)
    try:
        get_pg_cursor(None).execute(f"CREATE DATABASE {DB_NAME}")
        get_pg_cursor(DB_NAME).execute(TABLE_SCHEMA)
    except Exception:
        get_pg_cursor(DB_NAME).execute("TRUNCATE TABLE tb_transactions")

    # Generate data
    logger.info(f"Generating {total_files} CSV files with {lines_per_file} lines each")
    generate_transactions(total_files=total_files, lines_per_file=lines_per_file)

    # Copy date to db table tb_transactions
    logger.info("Loading CSV files into database")
    for file_name in os.listdir(DATA_DIR):
        logger.info(f"Loading file {file_name} into database")
        file_path = DATA_DIR / Path(file_name)
        get_pg_cursor(DB_NAME).copy_from(open(file_path), 'tb_transactions', sep=',')

        # Change data in CSV to have some differences with DB data
        new_file_path = DATA_DIR / Path(file_name + '.new')
        logger.info(f"Changing {changed_lines_per_files} lines in file {file_name} afterwards")
        with open(file_path, 'r') as orig_tr_csv_file:
            orig_tr_csv = csv.reader(orig_tr_csv_file, **CSV_OPTIONS)
            with open(new_file_path, 'w') as new_tr_csv_file:
                new_tr_csv = csv.writer(new_tr_csv_file, **CSV_OPTIONS)
                for line in orig_tr_csv:
                    if orig_tr_csv.line_num < changed_lines_per_files:
                        line[4] = round(float(line[4]) * random.random(), 2)
                    new_tr_csv.writerow(line)
        shutil.move(new_file_path, file_path)

    logger.info("DONE")


if __name__ == '__main__':
    logger.info("Generating fake data for task 2")
    generate_data(total_files=10,
                  lines_per_file=1000,
                  changed_lines_per_files=10)
