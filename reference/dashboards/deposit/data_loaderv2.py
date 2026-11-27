import os
import shutil
import pandas as pd
import psycopg2
from datetime import datetime
from pathlib import Path
import numpy as np
import logging
import traceback
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ─────────────────────────────────────────────
# 📌 DATABASE CONFIG
# ─────────────────────────────────────────────
DB_PARAMS = {
    'dbname': 'dashboard_db',
    'user': 'postgres',
    'password': '<PG_PASSWORD>',
    'host': 'localhost',
    'port': '5428'
}

# ─────────────────────────────────────────────
# 📁 PATH CONFIG
# ─────────────────────────────────────────────
BASE_PATH = Path("D:/DATA_ROOM/10. Nguyen Thi Thao Ly/PROJECT/Dashboard14/Data")
SOURCE_DIR = Path("<FTP_RCV_DIR>")
WORKING_DIR = BASE_PATH / "data"
ERROR_DIR = BASE_PATH / "error"
LOG_DIR = BASE_PATH / "logs"

# ─────────────────────────────────────────────
# 📜 LOGGING
# ─────────────────────────────────────────────
def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime('%Y%m%d')
    log_file = LOG_DIR / f'deposit_{today}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# ─────────────────────────────────────────────
# 🏗️ TABLE INIT (NO DROP)
# ─────────────────────────────────────────────
def create_tables():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bang3 (
                id SERIAL PRIMARY KEY,
                stnd_dt DATE,
                region VARCHAR(50),
                branch VARCHAR(10),
                retail_customer INTEGER,
                casa_customer INTEGER,
                active1_casa INTEGER,
                dormant1_casa INTEGER
            );
            CREATE TABLE IF NOT EXISTS bang5 (
                id SERIAL PRIMARY KEY,
                stnd_dt DATE,
                region VARCHAR(50),
                branch VARCHAR(10),
                tennor VARCHAR(50),
                min_curr_intrt DECIMAL(5,2),
                max_curr_intrt DECIMAL(5,2),
                avgintrt DECIMAL(5,2),
                total_bal DECIMAL(15,2)
            );
            CREATE TABLE IF NOT EXISTS bang6 (
                id SERIAL PRIMARY KEY,
                stnd_dt DATE,
                region VARCHAR(50),
                branch VARCHAR(10),
                no_ac_edusave INTEGER
            );
            CREATE TABLE IF NOT EXISTS bang1247 (
                id SERIAL PRIMARY KEY,
                stnd_dt DATE,
                account_group VARCHAR(100),
                group_class VARCHAR(50),
                currency VARCHAR(10),
                manage_branch VARCHAR(10),
                region VARCHAR(50),
                bal DECIMAL(20,2),
                port DECIMAL(10,4),
                diff_t_t1 DECIMAL(10,4),
                diff_t_m1 DECIMAL(10,4),
                diff_t_y1 DECIMAL(10,4),
                mon_avg_bal_t DECIMAL(20,2),
                diff_mon_avg_t_t1 DECIMAL(10,4),
                diff_mon_avg_t_m1 DECIMAL(10,4),
                diff_mon_avg_t_y1 DECIMAL(10,4),
                nation VARCHAR(50)
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("✅ Tables created successfully")
    except Exception as e:
        logging.error(f"❌ Error creating tables: {e}")
        logging.error(traceback.format_exc())
        raise e

# ─────────────────────────────────────────────
# 📅 DATE CONVERSION
# ─────────────────────────────────────────────
def convert_date_format(date_str):
    try:
        return datetime.strptime(str(date_str), '%Y%m%d').strftime('%Y-%m-%d')
    except:
        return date_str

# ─────────────────────────────────────────────
# 📄 LOAD FILE TO DB
# ─────────────────────────────────────────────
def load_file_to_db(file_path):
    try:
        filename = os.path.basename(file_path)
        table_name = filename.split('_')[0].lower()
        date_str = filename.split('_')[1].split('.')[0]

        df = pd.read_csv(file_path, sep='\t')
        df.replace(['nan', 'NaN', 'n/a', 'N/A', 'null', 'None'], 0, inplace=True)
        df.fillna(0, inplace=True)

        column_mapping = {
            'STND_DT': 'stnd_dt',
            'ACCOUNT_GROUP': 'account_group',
            'GROUP_CLASS': 'group_class',
            'CURRENCY': 'currency',
            'MANAGE_BRANCH': 'manage_branch',
            'REGION': 'region',
            'BAL': 'bal',
            'PORT': 'port',
            'DIFF_T_T1': 'diff_t_t1',
            'DIFF_T_M1': 'diff_t_m1',
            'DIFF_T_Y1': 'diff_t_y1',
            'MON_AVG_BAL_T': 'mon_avg_bal_t',
            'DIFF_MON_AVG_T_T1': 'diff_mon_avg_t_t1',
            'DIFF_MON_AVG_T_M1': 'diff_mon_avg_t_m1',
            'DIFF_MON_AVG_T_Y1': 'diff_mon_avg_t_y1',
            'NATION': 'nation'
        }
        df.rename(columns=column_mapping, inplace=True)

        if 'group_class' in df.columns:
            df['group_class'] = df['group_class'].replace('1-DEPOSIT', '1-CASA')

        if 'stnd_dt' in df.columns:
            df['stnd_dt'] = df['stnd_dt'].apply(convert_date_format)
            stnd_dt = df['stnd_dt'].iloc[0]
        else:
            stnd_dt = convert_date_format(date_str)
            df['stnd_dt'] = stnd_dt

        df = df.loc[:, ~df.columns.duplicated()]

        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table_name} WHERE stnd_dt = %s", (stnd_dt,))
        conn.commit()

        columns = ', '.join(df.columns)
        values = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        cur.executemany(insert_query, df.values.tolist())
        conn.commit()
        logging.info(f"✅ Loaded {len(df)} rows into {table_name} for {stnd_dt}")
        return True

    except Exception as e:
        logging.error(f"❌ Error loading {file_path.name}: {e}")
        logging.error(traceback.format_exc())
        if 'conn' in locals(): conn.rollback()
        return False
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

# ─────────────────────────────────────────────
# 📂 FILE HANDLER
# ─────────────────────────────────────────────
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory: return
        try:
            file_path = Path(event.src_path)
            logging.info(f"📥 Detected: {file_path.name}")

            WORKING_DIR.mkdir(exist_ok=True, parents=True)
            target_path = WORKING_DIR / file_path.name
            shutil.copy(file_path, target_path)

            success = load_file_to_db(target_path)

            ERROR_DIR.mkdir(exist_ok=True, parents=True)
            if not success:
                shutil.move(str(target_path), ERROR_DIR / file_path.name)
                logging.info(f"❌ Moved to error: {file_path.name}")
            else:
                # giữ file ở WORKING_DIR cho dashboard dùng
                if file_path.exists():
                    os.remove(file_path)

        except Exception as e:
            logging.error(f"❌ Handler error: {e}")
            logging.error(traceback.format_exc())

# ─────────────────────────────────────────────
# 🚀 MAIN
# ─────────────────────────────────────────────
def main():
    setup_logger()
    create_tables()

    logging.info(f"👀 Watching for files in {SOURCE_DIR}")
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=str(SOURCE_DIR), recursive=False)
    observer.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == '__main__':
    main()

