"""
Data loader: watch thư mục nguồn, load file vào PostgreSQL.

- Bảng cũ (bang3, bang5, bang6, bang1247, overdue, branch, credit_card_overdue): giữ nguyên.
- Bảng 1->9 (bang_01..bang_09): cấu trúc Trx_Date \\t Metric_Code \\t Metric_Value,
  tên file: bang_01_member_eop_YYYYMMDD.txt ... bang_09_risk_dpd_YYYYMMDD.txt.
"""
import logging
import os
import re
import shutil
import time
import traceback
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import psycopg2
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# =============================================================================
# CẤU HÌNH
# =============================================================================

DB_PARAMS = {
    "dbname": "dashboard_db",
    "user": "postgres",
    "password": "<PG_PASSWORD>",
    "host": "localhost",
    "port": "5428",
}

BASE_PATH = Path(__file__).resolve().parent
# Thư mục FTP nhận file (watch và load tại chỗ)
FTP_RCV_DIR = Path(r"<FTP_RCV_DIR>")
SOURCE_DIR = FTP_RCV_DIR
WORKING_DIR = FTP_RCV_DIR
ERROR_DIR = BASE_PATH / "error"
LOG_DIR = BASE_PATH / "logs"

TSV_DELIMITER = "\t"
NULL_VALUES = ["nan", "NaN", "n/a", "N/A", "null", "None"]
CARD_FILE_PATTERN = re.compile(r"^bang_0[1-9]_.+_\d{8}\.txt$", re.IGNORECASE)

# Bảng 1->9: cấu trúc trx_date, metric_code, metric_value
CARD_TABLE_NAMES = [
    "bang_01_member_eop",
    "bang_02_member_usage",
    "bang_03_new_consumer",
    "bang_04_member_closed",
    "bang_05_balance_eop",
    "bang_06_spend_sector",
    "bang_07_spend_location",
    "bang_08_spend_channel",
    "bang_09_risk_dpd",
]

# Dùng cho build_dynamic_table (bảng cũ / logical key)
CARD_METRIC_STRUCTURE = {
    "member_eop": [
        "EOP_Consumer",
        "EOP_Corporate",
    ],
    "member_usage": [
        "Using_Consumer",
        "Using_Corporate",
    ],
    "new_consumer": [
        "New_Consumer",
        "New_Corporate",
        "New_Member",
    ],
    "member_closed": [
        "Consumer_Closed",
        "Corporate_Closed",
        "Member_Closed",
    ],
    "balance_eop": [
        "Total_Outstanding_Balance",
    ],
    "spend_sector": [
        "Purchasing_Amt",
        "Cash_Advance_Amt",
        "Installment_Amt",
        "Flexi_Amt",
        "Total_Spending_Amt",
    ],
    "spend_location": [
        "Domestic_Spending_Amt",
        "XBorder_Spending_Amt",
        "Domestic_Rate",
        "XBorder_Rate",
    ],
    "spend_channel": [
        "POS_Spending_Amt",
        "Online_Spending_Amt",
        "Offline_Rate",
        "Online_Rate",
    ],
    "risk_dpd": [
        "Revolving_Balance",
        "Revolving_Member",
        "Revolving_Rate",
        "DPD_1P_Member",
        "DPD_10P_Member",
        "DPD_30P_Member",
        "NPL_90P_Member",
        "DPD_1P_Balance",
        "DPD_10P_Balance",
        "DPD_30P_Balance",
        "NPL_90P_Balance",
        "DPD_1P_Rate",
        "DPD_10P_Rate",
        "DPD_30P_Rate",
        "NPL_90P_Rate",
    ],
}

# =============================================================================
# LOGGING
# =============================================================================


def setup_logger() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime('%Y%m%d')
    log_file = LOG_DIR / f'rcd_{today}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


# =============================================================================
# TẠO BẢNG (bảng cũ giữ nguyên; bảng 1->9: trx_date, metric_code, metric_value)
# =============================================================================


def create_tables() -> None:
    """Tạo tất cả bảng nếu chưa có (bảng cũ + bảng 1->9)."""
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
            CREATE TABLE IF NOT EXISTS overdue (
                id SERIAL PRIMARY KEY,
                report_dt DATE,
                prdt_code VARCHAR(20),
                product_type VARCHAR(20),
                product_group VARCHAR(20),
                product_name VARCHAR(50),
                mng_br VARCHAR(10),
                sal_br VARCHAR(10),
                del_grp VARCHAR(10),
                port_acc INTEGER,
                ovd_acc INTEGER,
                port_bal DECIMAL(15,2),
                ovd_bal DECIMAL(15,2)
            );
            CREATE TABLE IF NOT EXISTS branch (
                STND_DT DATE,
                branch_code VARCHAR(10),
                branch_name VARCHAR(100)
            );
            CREATE TABLE IF NOT EXISTS credit_card_overdue (
                id SERIAL PRIMARY KEY,
                report_dt DATE,
                prdt_code VARCHAR(50),
                product_type VARCHAR(50),
                product_group VARCHAR(50),
                product_name VARCHAR(50),
                mng_br VARCHAR(10),
                sal_br VARCHAR(10),
                del_grp VARCHAR(10),
                port_acc INTEGER,
                ovd_acc INTEGER,
                port_bal DECIMAL(15,2),
                ovd_bal DECIMAL(15,2)
            );
            -- Bảng 1->9: cấu trúc trx_date, metric_code, metric_value
            CREATE TABLE IF NOT EXISTS bang_01_member_eop (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
            CREATE TABLE IF NOT EXISTS bang_02_member_usage (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
            CREATE TABLE IF NOT EXISTS bang_03_new_consumer (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
            CREATE TABLE IF NOT EXISTS bang_04_member_closed (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
            CREATE TABLE IF NOT EXISTS bang_05_balance_eop (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
            CREATE TABLE IF NOT EXISTS bang_06_spend_sector (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
            CREATE TABLE IF NOT EXISTS bang_07_spend_location (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
            CREATE TABLE IF NOT EXISTS bang_08_spend_channel (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
            CREATE TABLE IF NOT EXISTS bang_09_risk_dpd (
                trx_date DATE,
                metric_code TEXT,
                metric_value NUMERIC
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Tables created successfully")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        logging.error(traceback.format_exc())
        raise e


# =============================================================================
# TIỆN ÍCH: NGÀY & GIÁ TRỊ
# =============================================================================


def _parse_yyyymmdd(x) -> Optional[str]:
    """Từ int/float/string có 8 chữ số -> 'YYYY-MM-DD', không được thì None."""
    if x is None:
        return None
    s = str(int(x)) if isinstance(x, (int, float)) else str(x).strip()
    m = re.search(r"(\d{8})", s)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return None


def norm_trx_date(x) -> str:
    """Chuẩn hóa trx_date: 20260131 -> 2026-01-31."""
    s = str(x).strip()
    if len(s) == 8 and s.isdigit():
        try:
            return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            pass
    return s


def clean_metric_value(x) -> Optional[float]:
    """Chuẩn hóa metric_value: bỏ dấu phẩy và %, trả về float hoặc None."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().replace(",", "").rstrip("%")
    if not s or s.lower() in ("nan", "none", "null", "n/a"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def convert_date_format_v1(x) -> Optional[str]:
    """Alias _parse_yyyymmdd (tương thích)."""
    return _parse_yyyymmdd(x)


def pick_date_with_priority(
    df: pd.DataFrame,
    candidate_cols: list,
    fallback_from_filename: str,
    table_name: str,
    filename: str,
) -> Optional[str]:
    """Lấy ngày từ cột (ưu tiên candidate_cols), không có thì lấy từ tên file."""
    for col in candidate_cols:
        if col not in df.columns:
            continue

        sample_vals = df[col].head(3).tolist()
        logging.info(f"{filename} ({table_name}): Cột {col} có dữ liệu mẫu: {sample_vals}")

        tmp = df[col].map(convert_date_format_v1)
        vals = [v for v in tmp.tolist() if v]

        if not vals:
            logging.warning(f"{filename} ({table_name}): Cột {col} không có ngày hợp lệ")
            continue

        mode_val, _ = Counter(vals).most_common(1)[0]
        unique_vals = set(vals)

        logging.info(f"{filename} ({table_name}): Sử dụng ngày từ cột {col}: {mode_val}")

        if len(unique_vals) > 1:
            logging.warning(
                f"{filename} ({table_name}): phát hiện {len(unique_vals)} ngày khác nhau trong cột {col}. "
                f"Dùng ngày phổ biến nhất: {mode_val}"
            )
        return mode_val

    logging.info(f"{filename} ({table_name}): Fallback sang ngày từ tên file: {fallback_from_filename}")
    fb = convert_date_format_v1(fallback_from_filename)
    if fb:
        logging.info(f"{filename} ({table_name}): Sử dụng ngày từ tên file: {fb}")
        return fb

    logging.error(f"Không xác định được ngày cho {filename} (bảng {table_name}).")
    return None


def convert_date_format(date_str) -> str:
    """Parse YYYYMMDD -> 'YYYY-MM-DD'; thất bại thì trả lại input."""
    try:
        return datetime.strptime(str(date_str), '%Y%m%d').strftime('%Y-%m-%d')
    except Exception:
        return date_str


# =============================================================================
# TIỆN ÍCH: DATAFRAME (bảng cũ)
# =============================================================================


def _to_branch_code(x) -> str:
    """Chuẩn hóa mã chi nhánh: số hoặc '0'."""
    if pd.isna(x) or str(x).lower() in ("nan", "0", "0.0"):
        return "0"
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return "0"


def normalize_branch_codes(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Chuẩn hóa MNG_BR, SAL_BR về chuỗi số."""
    for col in ("MNG_BR", "SAL_BR"):
        if col in df.columns:
            df[col] = df[col].apply(_to_branch_code)
            logging.info(f"{filename}: {col} đã chuẩn hóa")
    return df


def clean_generic_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Thay null/chuỗi rỗng bằng 0."""
    df = df.copy()
    df = df.replace(NULL_VALUES, 0).fillna(0)
    return df


def build_dynamic_table(
    conn, table_name: str, df: pd.DataFrame, excluded_cols: Optional[list] = None
) -> None:
    """Tạo bảng động theo schema DataFrame nếu chưa tồn tại (dùng cho bảng cũ)."""
    excluded_cols = excluded_cols or []

    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = %s
        );
        """,
        (table_name,),
    )
    table_exists = cur.fetchone()[0]

    if not table_exists:
        columns_def = ["id SERIAL PRIMARY KEY", "stnd_dt DATE"]
        for col in df.columns:
            if col in excluded_cols:
                continue
            dtype = df[col].dtype
            if dtype in ["int64", "int32"]:
                columns_def.append(f'"{col}" INTEGER')
            elif dtype in ["float64", "float32"]:
                columns_def.append(f'"{col}" DECIMAL(20,2)')
            else:
                columns_def.append(f'"{col}" VARCHAR(255)')

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {', '.join(columns_def)}
            );
        """
        cur.execute(create_table_sql)
        conn.commit()
        logging.info(f"Created table {table_name}")

    cur.close()


# Mapping cột cho bảng overdue / credit_card_overdue
OVERDUE_COLUMN_MAPPING = {
    "REPORT_DT": "report_dt",
    "PRDT_CODE": "prdt_code",
    "PRODUCT_TYPE": "product_type",
    "PRODUCT_GROUP": "product_group",
    "PRODUCT_NAME": "product_name",
    "MNG_BR": "mng_br",
    "SAL_BR": "sal_br",
    "DEL_GRP": "del_grp",
    "PORT_ACC": "port_acc",
    "OVD_ACC": "ovd_acc",
    "PORT_BAL": "port_bal",
    "OVD_BAL": "ovd_bal",
    "STND_DT": "stnd_dt",
}

# Mapping cột cho bảng legacy (bang1247, bang3, bang5, bang6...)
LEGACY_COLUMN_MAPPING = {
    "STND_DT": "stnd_dt",
    "ACCOUNT_GROUP": "account_group",
    "GROUP_CLASS": "group_class",
    "CURRENCY": "currency",
    "MANAGE_BRANCH": "manage_branch",
    "REGION": "region",
    "BAL": "bal",
    "PORT": "port",
    "DIFF_T_T1": "diff_t_t1",
    "DIFF_T_M1": "diff_t_m1",
    "DIFF_T_Y1": "diff_t_y1",
    "MON_AVG_BAL_T": "mon_avg_bal_t",
    "DIFF_MON_AVG_T_T1": "diff_mon_avg_t_t1",
    "DIFF_MON_AVG_T_M1": "diff_mon_avg_t_m1",
    "DIFF_MON_AVG_T_Y1": "diff_mon_avg_t_y1",
    "NATION": "nation",
    "RETAIL_CUSTOMER": "retail_customer",
    "CASA_CUSTOMER": "casa_customer",
    "ACTIVE1_CASA": "active1_casa",
    "DORMANT1_CASA": "dormant1_casa",
    "BRANCH": "branch",
    "TENNOR": "tennor",
    "MIN_CURR_INTRT": "min_curr_intrt",
    "MAX_CURR_INTRT": "max_curr_intrt",
    "AVGINTRT": "avgintrt",
    "TOTAL_BAL": "total_bal",
    "NO_AC_EDUSAVE": "no_ac_edusave",
}


# =============================================================================
# LOAD FILE VÀO DB
# =============================================================================


def _parse_filename(filename: str) -> Tuple[str, str, str]:
    """Từ tên file (stem hoặc full) -> (table_name, logical_key, date_str)."""
    stem = Path(filename).stem
    parts = stem.split("_")
    if len(parts) >= 2:
        table_name_raw = "_".join(parts[:-1]).lower()
        date_str = parts[-1]
    else:
        table_name_raw = stem.lower()
        date_str = stem

    logical_key = table_name_raw
    m = re.match(r"^bang_\d+_(.+)$", table_name_raw)
    if m:
        logical_key = m.group(1)

    if table_name_raw.startswith("bang_"):
        table_name = table_name_raw
    elif table_name_raw and table_name_raw[0].isdigit():
        table_name = f"bang_{table_name_raw}"
    else:
        table_name = table_name_raw

    return table_name, logical_key, date_str


def _normalize_card_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hóa tên cột Trx_Date/Metric_Code/Metric_Value -> lowercase."""
    col_rename = {}
    for c in df.columns:
        u = str(c).strip().upper()
        if u == "TRX_DATE":
            col_rename[c] = "trx_date"
        elif u == "METRIC_CODE":
            col_rename[c] = "metric_code"
        elif u == "METRIC_VALUE":
            col_rename[c] = "metric_value"
    if col_rename:
        df = df.rename(columns=col_rename)
    return df


def _drop_null_dates(df: pd.DataFrame, date_col: str, filename: str) -> pd.DataFrame:
    """Loại dòng có date_col null/rỗng."""
    if date_col not in df.columns:
        return df
    before = len(df)
    df = df[df[date_col].notna() & (df[date_col] != "") & (df[date_col] != "None")]
    if before > len(df):
        logging.info(f"{filename}: Đã xóa {before - len(df)} dòng {date_col} null")
    return df


def _table_exists(conn, table_name: str) -> bool:
    """Kiểm tra bảng có tồn tại trong public schema."""
    with conn.cursor() as c:
        c.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)",
            (table_name,),
        )
        return c.fetchone()[0]


def load_file_to_db(file_path: Path) -> bool:
    """Đọc 1 file, nhận diện bảng, chuẩn hóa và insert vào Postgres. Trả về True nếu thành công."""
    filename = os.path.basename(file_path)

    try:
        table_name, logical_key, date_str = _parse_filename(filename)
        stnd_dt = None

        if not file_path.exists():
            logging.error(f"File does not exist: {file_path}")
            return False
        if file_path.stat().st_size == 0:
            logging.error(f"File is empty: {file_path}")
            return False

        try:
            df = pd.read_csv(file_path, sep=TSV_DELIMITER)
        except pd.errors.EmptyDataError:
            logging.error(f"File empty or invalid: {file_path}")
            return False
        if df.empty or len(df.columns) == 0:
            logging.error(f"File has no columns or data: {file_path}")
            return False

        df = _normalize_card_columns(df)
        is_card = (
            table_name in CARD_TABLE_NAMES
            and "trx_date" in df.columns
            and "metric_code" in df.columns
            and "metric_value" in df.columns
        )

        if table_name == "credit_card_overdue":
            df = normalize_branch_codes(df, filename)

        if is_card:
            df = df[["trx_date", "metric_code", "metric_value"]].copy()
            df["trx_date"] = df["trx_date"].map(norm_trx_date)
            df["metric_value"] = df["metric_value"].map(clean_metric_value)
            df = df.replace(NULL_VALUES, None)
        else:
            df = clean_generic_nulls(df)

        # --- Bảng cũ: column mapping và xử lý ngày ---
        if not is_card and table_name in ("overdue", "credit_card_overdue"):
            column_mapping = OVERDUE_COLUMN_MAPPING

            if table_name == "credit_card_overdue":
                if "REPORT_DT" in df.columns:
                    df["REPORT_DT"] = df["REPORT_DT"].astype(str)
                    logging.info(f"{filename}: Convert REPORT_DT sang string trước")

                stnd_dt = pick_date_with_priority(
                    df,
                    candidate_cols=["REPORT_DT"],
                    fallback_from_filename=date_str,
                    table_name=table_name,
                    filename=filename,
                )

                if "REPORT_DT" in df.columns:
                    sample_before = df["REPORT_DT"].head(3).tolist()
                    logging.info(f"{filename}: Chuyển đổi REPORT_DT từ {sample_before}")
                    df["REPORT_DT"] = df["REPORT_DT"].map(convert_date_format_v1)
                    sample_after = df["REPORT_DT"].head(3).tolist()
                    logging.info(f"{filename}: REPORT_DT sau chuyển đổi: {sample_after}")
                else:
                    df["REPORT_DT"] = stnd_dt
                    logging.info(f"{filename}: Thêm cột REPORT_DT: {stnd_dt}")

            else:
                if "REPORT_DT" in df.columns:
                    df["REPORT_DT"] = df["REPORT_DT"].apply(convert_date_format)
                    stnd_dt = df["REPORT_DT"].iloc[0]
                else:
                    stnd_dt = convert_date_format(date_str)
                    df["REPORT_DT"] = stnd_dt
        else:
            column_mapping = LEGACY_COLUMN_MAPPING

            if not is_card:
                if "STND_DT" in df.columns:
                    df["STND_DT"] = df["STND_DT"].apply(convert_date_format)
                    valid_dates = df[df["STND_DT"].notna() & (df["STND_DT"] != "")]["STND_DT"]
                    if len(valid_dates) > 0:
                        stnd_dt = valid_dates.iloc[0]
                    else:
                        stnd_dt = convert_date_format(date_str)
                        df["STND_DT"] = stnd_dt
                else:
                    stnd_dt = convert_date_format(date_str)
                    if stnd_dt and stnd_dt != date_str:
                        df["STND_DT"] = stnd_dt

        if not is_card:
            df.rename(columns=column_mapping, inplace=True)

        if "group_class" in df.columns:
            df["group_class"] = df["group_class"].replace("1-DEPOSIT", "1-CASA")

        df = df.loc[:, ~df.columns.duplicated()]

        if table_name in ("overdue", "credit_card_overdue"):
            df = _drop_null_dates(df, "report_dt", filename)
        else:
            df = _drop_null_dates(df, "stnd_dt", filename)

        # Ngày chuẩn cho insert/delete
        if not stnd_dt or stnd_dt == date_str:
            stnd_dt = convert_date_format(date_str)
            if not stnd_dt or stnd_dt == date_str:
                logging.error(f"Không thể parse ngày từ {filename}: {date_str}")
                return False

        conn = psycopg2.connect(**DB_PARAMS)
        try:
            cur = conn.cursor()
            exists = _table_exists(conn, table_name)
            if not exists and not is_card and logical_key in CARD_METRIC_STRUCTURE:
                cur.close()
                build_dynamic_table(conn, table_name, df, excluded_cols=["stnd_dt", "Trx_Date", "STND_DT"])
                exists = _table_exists(conn, table_name)
                cur = conn.cursor()

            if not exists:
                logging.warning(f"{filename}: Table {table_name} does not exist, INSERT will fail")
                conn.commit()
                return False

            if not is_card and table_name in ("overdue", "credit_card_overdue"):
                cur.execute(f'DELETE FROM "{table_name}" WHERE report_dt = %s', (stnd_dt,))
                conn.commit()
            elif not is_card:
                cur.execute(f'DELETE FROM "{table_name}" WHERE stnd_dt = %s', (stnd_dt,))
                conn.commit()

            if is_card:
                df = df[["trx_date", "metric_code", "metric_value"]].copy()
                columns, values = "trx_date, metric_code, metric_value", "%s, %s, %s"
            else:
                columns = ", ".join(f'"{c}"' for c in df.columns)
                values = ", ".join(["%s"] * len(df.columns))

            cur.executemany(
                f'INSERT INTO "{table_name}" ({columns}) VALUES ({values})',
                df.values.tolist(),
            )
            conn.commit()
            logging.info(f"Loaded {len(df)} rows into {table_name} for {stnd_dt or 'card_metrics'}")
            return True
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
        logging.error(f"Error loading {file_path.name}: {e}")
        logging.error(traceback.format_exc())
        return False

# =============================================================================
# WATCH: FILE HANDLER (file mới → copy nếu cần → load)
# =============================================================================


def _move_to_error(target_path: Path, error_dest: Path, filename: str, max_retries: int = 3) -> bool:
    """Chuyển file lỗi vào ERROR_DIR. Trả về True nếu thành công."""
    ERROR_DIR.mkdir(exist_ok=True, parents=True)
    if error_dest.exists():
        try:
            error_dest.unlink()
        except OSError as e:
            logging.warning(f"Không xóa file cũ trong error: {e}")
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(0.5 * attempt)
            if not target_path.exists():
                return False
            shutil.move(str(target_path), str(error_dest))
            logging.info(f"Moved to error: {filename}")
            return True
        except PermissionError:
            if attempt == max_retries - 1:
                try:
                    shutil.copy2(str(target_path), str(error_dest))
                    target_path.unlink()
                    logging.info(f"Copied to error (fallback): {filename}")
                    return True
                except Exception as e:
                    logging.error(f"Không thể chuyển file vào error: {filename} - {e}")
        except Exception as e:
            logging.error(f"Không thể chuyển file vào error: {filename} - {e}")
            break
    return False


class FileHandler(FileSystemEventHandler):
    """Khi có file mới trong SOURCE_DIR: copy sang WORKING_DIR (nếu khác thư mục) rồi load vào DB."""

    def on_created(self, event):
        if event.is_directory:
            return
        try:
            file_path = Path(event.src_path)
            logging.info(f"Detected: {file_path.name}")
            if not file_path.exists():
                logging.warning(f"File no longer exists: {file_path.name}")
                return

            same_dir = file_path.resolve().parent == WORKING_DIR.resolve()
            if same_dir:
                target_path = file_path
            else:
                WORKING_DIR.mkdir(exist_ok=True, parents=True)
                target_path = WORKING_DIR / file_path.name
                try:
                    shutil.copy2(file_path, target_path)
                except (FileNotFoundError, OSError) as e:
                    logging.error(f"Copy failed: {file_path.name} - {e}")
                    return

            success = load_file_to_db(target_path)

            if not success:
                _move_to_error(target_path, ERROR_DIR / file_path.name, file_path.name)
            elif not same_dir and file_path.exists():
                try:
                    file_path.unlink()
                except OSError as e:
                    logging.warning(f"Không xóa file gốc: {file_path.name} - {e}")
        except Exception as e:
            logging.error(f"Handler error: {e}\n{traceback.format_exc()}")

def load_all_from_data_dir() -> int:
    """Load toàn bộ file bang_01..bang_09 trong WORKING_DIR vào DB. Trả về số file load thành công."""
    if not WORKING_DIR.is_dir():
        logging.warning(f"Thư mục không tồn tại: {WORKING_DIR}")
        return 0
    files = sorted(f for f in WORKING_DIR.glob("*.txt") if CARD_FILE_PATTERN.match(f.name))
    logging.info(f"Load {len(files)} file từ {WORKING_DIR}")
    ok = sum(1 for f in files if load_file_to_db(f))
    logging.info(f"Đã load: {ok}/{len(files)} file.")
    return ok


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    """Chạy loader: tạo bảng, watch SOURCE_DIR, load file mới vào DB."""
    setup_logger()
    create_tables()
    logging.info(f"Watching: {SOURCE_DIR}")
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


if __name__ == "__main__":
    main()
