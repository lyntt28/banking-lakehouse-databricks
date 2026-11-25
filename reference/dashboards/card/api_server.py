from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from datetime import datetime, timedelta
from contextlib import contextmanager
import calendar
from decimal import Decimal
import math
import time

app = Flask(__name__)

def sanitize_for_json(obj):
    """Recursively sanitize values for JSON serialization (handle NaN, Infinity, Decimal)"""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, Decimal):
        f = float(obj)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj

CORS(app)

# In-memory cache for full-stats (avoid repeated DB load for same date/type)
FULL_STATS_CACHE_TTL = 90  # seconds
_full_stats_cache = {}  # key: (date_param, customer_type), value: (cached_at, response)
_full_stats_cache_max = 30

# Global lightweight request/response logging for debugging
@app.before_request
def _log_request():
    try:
        print(f"[REQ] {request.method} {request.path} args={dict(request.args)}", flush=True)
    except Exception:
        pass

@app.after_request
def _log_response(resp):
    try:
        print(f"[RES] {request.path} -> {resp.status_code}", flush=True)
    except Exception:
        pass
    return resp

DB_PARAMS = {
    'dbname': 'dashboard_db',
    'user': 'postgres',
    'password': '<PG_PASSWORD>',
    'host': 'localhost',
    'port': '5428'
}

# Table mapping: logical key -> table name pattern
TABLE_MAP = {
    'member_eop': 'bang_01_member_eop',
    'member_usage': 'bang_02_member_usage',
    'new_consumer': 'bang_03_new_consumer',
    'member_closed': 'bang_04_member_closed',
    'balance_eop': 'bang_05_balance_eop',
    'spend_sector': 'bang_06_spend_sector',
    'spend_location': 'bang_07_spend_location',
    'spend_channel': 'bang_08_spend_channel',
    'risk_dpd': 'bang_09_risk_dpd',
}

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        yield conn
    finally:
        conn.close()

def handle_error(e, context=''):
    return jsonify({'error': str(e), 'context': context}), 500

def get_table_name(logical_key):
    """Get actual table name from logical key"""
    return TABLE_MAP.get(logical_key, f'bang_{logical_key}')

def normalize_date_to_ymd(s):
    """Chuẩn hóa chuỗi ngày về YYYY-MM-DD (để so khớp với DB). Chấp nhận: Y-m-d, DD.MM.YYYY, DD/MM/YYYY, YYYYMMDD."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    # Đã đúng Y-m-d
    if len(s) == 10 and s[4] == '-' and s[7] == '-':
        try:
            datetime.strptime(s, '%Y-%m-%d')
            return s
        except ValueError:
            pass
    # DD.MM.YYYY hoặc DD/MM/YYYY
    for sep in ['.', '/', '-']:
        if sep in s and len(s) >= 8:
            parts = s.split(sep)
            if len(parts) == 3:
                try:
                    if len(parts[2]) == 4 and len(parts[0]) <= 2 and len(parts[1]) <= 2:
                        d = datetime.strptime(s, f'%d{sep}%m{sep}%Y')
                        return d.strftime('%Y-%m-%d')
                    if len(parts[0]) == 4 and len(parts[2]) <= 2:
                        d = datetime.strptime(s, f'%Y{sep}%m{sep}%d')
                        return d.strftime('%Y-%m-%d')
                except ValueError:
                    pass
    # YYYYMMDD (8 chữ số)
    if len(s) == 8 and s.isdigit():
        try:
            d = datetime.strptime(s, '%Y%m%d')
            return d.strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None  # Không parse được -> không dùng chuỗi gốc (tránh lỗi strptime sau)

def normalize_date_param(date_param):
    """Chuẩn hóa tham số date từ request về Y-m-d; None nếu không parse được."""
    if not date_param:
        return None
    return normalize_date_to_ymd(date_param)

def get_value_from_table(table_name, column_name, date_str):
    """Get a tsingle value from a table for a specific date"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            # Query using normalized structure with lowercase column names
            query = f'SELECT metric_value FROM {table_name} WHERE metric_code = %s AND trx_date = %s ORDER BY trx_date DESC LIMIT 1'
            cur.execute(query, (column_name, date_str))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        print(f"Error getting value from {table_name}.{column_name}: {e}")
        return 0.0

def get_value_with_cur(cur, table_name, metric_code, date_str):
    """Get single value for a date - reuses cursor (no new connection)."""
    try:
        cur.execute(
            'SELECT metric_value FROM {} WHERE metric_code = %s AND trx_date = %s LIMIT 1'.format(table_name),
            (metric_code, date_str)
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        print(f"Error get_value_with_cur {table_name}.{metric_code}: {e}")
        return 0.0

def get_table_data(table_name, date_str):
    """Get all data from a table for a specific date"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            # Query all rows for the date with lowercase column names
            query = f'SELECT metric_code, metric_value FROM {table_name} WHERE trx_date = %s'
            cur.execute(query, (date_str,))
            rows = cur.fetchall()
            # Pivot: convert rows to dict keyed by metric_code
            result = {}
            for row in rows:
                metric_code = row[0]
                metric_value = row[1]
                result[metric_code] = metric_value
            return result
    except Exception as e:
        print(f"Error getting data from {table_name}: {e}")
        return {}

# ============ OPTIMIZED FUNCTIONS (reuse connection) ============

def get_table_data_with_conn(cur, table_name, date_str):
    """Get all data from a table for a specific date - OPTIMIZED: reuses cursor"""
    try:
        query = f'SELECT metric_code, metric_value FROM {table_name} WHERE trx_date = %s'
        cur.execute(query, (date_str,))
        rows = cur.fetchall()
        result = {}
        for row in rows:
            result[row[0]] = row[1]
        return result
    except Exception as e:
        print(f"Error getting data from {table_name}: {e}")
        return {}

def get_all_tables_data_batch(cur, date_str):
    """Get data from ALL tables for a specific date in ONE function call - VERY FAST"""
    tables = ['member_eop', 'member_usage', 'new_consumer', 'member_closed', 
              'balance_eop', 'spend_sector', 'spend_location', 'spend_channel', 'risk_dpd']
    
    all_data = {}
    for table_key in tables:
        table_name = get_table_name(table_key)
        all_data[table_key] = get_table_data_with_conn(cur, table_name, date_str)
    return all_data

def calculate_avg_balance_with_conn(cur, date_str):
    """Calculate average balance per member - OPTIMIZED"""
    try:
        # Get total outstanding
        cur.execute(f"SELECT metric_value FROM {get_table_name('balance_eop')} WHERE metric_code = 'Total_Outstanding_Balance' AND trx_date = %s", (date_str,))
        row = cur.fetchone()
        total_outstanding = float(row[0]) if row and row[0] else 0.0
        
        # Get EOP consumer
        cur.execute(f"SELECT metric_value FROM {get_table_name('member_eop')} WHERE metric_code = 'EOP_Consumer' AND trx_date = %s", (date_str,))
        row = cur.fetchone()
        eop_consumer = float(row[0]) if row and row[0] else 0.0
        
        if eop_consumer > 0:
            return total_outstanding / eop_consumer
        return 0.0
    except Exception as e:
        print(f"Error calculating avg balance: {e}")
        return 0.0

def get_cumulative_sum_to_date(cur, table_name, metric_code, date_str):
    """
    Get cumulative SUM from start of month to the specified date.
    For New Consumer/New Corporate: SUM from day 1 of the month to date_str.
    """
    try:
        if not date_str:
            print(f"[DEBUG] get_cumulative_sum_to_date: date_str is empty")
            return 0.0
        
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        year = date_obj.year
        month = date_obj.month
        
        # SUM from start of month (YYYY-MM-01) to date_str
        # Cast metric_value to numeric to handle any text formatting issues
        query = f"""
            SELECT COALESCE(SUM(CAST(metric_value AS NUMERIC)), 0) as total
            FROM {table_name}
            WHERE metric_code = %s
              AND EXTRACT(YEAR FROM trx_date) = %s
              AND EXTRACT(MONTH FROM trx_date) = %s
              AND trx_date <= %s
              AND metric_value IS NOT NULL
        """
        cur.execute(query, (metric_code, year, month, date_str))
        row = cur.fetchone()
        result = float(row[0]) if row and row[0] is not None else 0.0
        print(f"[DEBUG] get_cumulative_sum_to_date: {table_name}.{metric_code} to {date_str} (year={year}, month={month}) = {result}")
        
        # Debug: Check if there's any data at all
        debug_query = f"""
            SELECT COUNT(*), MIN(trx_date), MAX(trx_date)
            FROM {table_name}
            WHERE metric_code = %s
              AND EXTRACT(YEAR FROM trx_date) = %s
              AND EXTRACT(MONTH FROM trx_date) = %s
        """
        cur.execute(debug_query, (metric_code, year, month))
        debug_row = cur.fetchone()
        if debug_row:
            print(f"[DEBUG] Available data: count={debug_row[0]}, min_date={debug_row[1]}, max_date={debug_row[2]}")
        
        return result
    except Exception as e:
        print(f"Error getting cumulative sum for {table_name}.{metric_code} to {date_str}: {e}")
        import traceback
        print(traceback.format_exc())
        return 0.0

def get_comparison_values_batch(cur, dates_dict, metrics_list):
    """
    Get comparison values for multiple dates in batch.
    dates_dict: {'last_day': date1, 'last_month': date2, 'last_year': date3}
    metrics_list: [('table_key', 'metric_code', 'result_key', is_cumulative), ...]
                  is_cumulative: True for New Consumer/New Corporate (SUM from start of month), False for others
    Returns: {'last_day': {result_key: value, ...}, 'last_month': {...}, 'last_year': {...}}
    """
    result = {period: {} for period in dates_dict.keys()}
    
    for metric_tuple in metrics_list:
        if len(metric_tuple) == 4:
            table_key, metric_code, result_key, is_cumulative = metric_tuple
        else:
            # Backward compatibility: default to False (not cumulative)
            table_key, metric_code, result_key = metric_tuple
            is_cumulative = False
        
        table_name = get_table_name(table_key)
        for period, date_str in dates_dict.items():
            if date_str:
                if is_cumulative:
                    # For New Consumer/New Corporate: SUM from start of month to date_str
                    result[period][result_key] = get_cumulative_sum_to_date(cur, table_name, metric_code, date_str)
                else:
                    # For other metrics: get value at that specific date
                    cur.execute(
                        f"SELECT metric_value FROM {table_name} WHERE metric_code = %s AND trx_date = %s LIMIT 1",
                        (metric_code, date_str)
                    )
                    row = cur.fetchone()
                    result[period][result_key] = float(row[0]) if row and row[0] else 0.0
            else:
                result[period][result_key] = 0.0
    
    # Add avg_balance calculations
    for period, date_str in dates_dict.items():
        if date_str:
            result[period]['avg_balance'] = calculate_avg_balance_with_conn(cur, date_str)
        else:
            result[period]['avg_balance'] = 0.0
    
    return result

def get_last_6_dates_and_labels(cur, date_param):
    """
    Build 6 data points: last day in DB for each of the 5 months before the selected date's month,
    plus the selected date. Dùng member_eop làm reference để mọi chart (Customer Base, Balance, Sales Volume, Spend per cus) dùng chung 6 ngày có data.
    Returns (list of 6 date strings, list of 6 labels).
    """
    try:
        date_obj = datetime.strptime(date_param, '%Y-%m-%d')
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        member_eop = get_table_name('member_eop')
        five_dates = []
        for i in range(1, 6):
            if date_obj.month - i >= 1:
                y, m = date_obj.year, date_obj.month - i
            else:
                y, m = date_obj.year - 1, date_obj.month - i + 12
            cur.execute(
                f"SELECT MAX(trx_date) FROM {member_eop} WHERE EXTRACT(YEAR FROM trx_date)=%s AND EXTRACT(MONTH FROM trx_date)=%s",
                (y, m)
            )
            row = cur.fetchone()
            if row and row[0]:
                five_dates.append(row[0].strftime('%Y-%m-%d'))
            else:
                last_d = calendar.monthrange(y, m)[1]
                five_dates.append(datetime(y, m, last_d).strftime('%Y-%m-%d'))
        # Trả về thứ tự thời gian (cũ nhất trước) để khớp với formatAxisLabel trên frontend (Sep -> Oct -> ... -> selected date)
        five_reversed = list(reversed(five_dates))
        six_dates = five_reversed + [date_param]
        labels = []
        for d in five_reversed:
            dt = datetime.strptime(d, '%Y-%m-%d')
            labels.append(f"{month_names[dt.month - 1]} {dt.year}")
        labels.append(f"{date_obj.day} {month_names[date_obj.month - 1]} {date_obj.year}")
        return six_dates, labels
    except Exception as e:
        print(f"Error get_last_6_dates_and_labels: {e}")
        import traceback
        print(traceback.format_exc())
        return [date_param] * 6, [date_param[:10]] * 6

def get_values_for_dates(cur, table_name, metric_code, date_list):
    """Return list of metric_value for each date in date_list (snapshot at that date)."""
    try:
        if not date_list:
            return []
        out = []
        for d in date_list:
            cur.execute(
                f"SELECT metric_value FROM {table_name} WHERE metric_code = %s AND trx_date = %s LIMIT 1",
                (metric_code, d)
            )
            row = cur.fetchone()
            out.append(float(row[0]) if row and row[0] is not None else 0.0)
        return out
    except Exception as e:
        print(f"Error get_values_for_dates {table_name}.{metric_code}: {e}")
        return [0.0] * len(date_list)

def get_multi_metrics_for_dates(cur, table_name, metric_codes, date_list):
    """Fetch multiple metrics for multiple dates in ONE query. Returns dict: metric_code -> list of values (same order as date_list)."""
    if not date_list or not metric_codes:
        return {m: [0.0] * len(date_list) for m in metric_codes}
    try:
        placeholders_dates = ','.join(['%s'] * len(date_list))
        placeholders_metrics = ','.join(['%s'] * len(metric_codes))
        query = (
            f"SELECT trx_date, metric_code, metric_value FROM {table_name} "
            f"WHERE trx_date IN ({placeholders_dates}) AND metric_code IN ({placeholders_metrics})"
        )
        cur.execute(query, list(date_list) + list(metric_codes))
        rows = cur.fetchall()
        # Build (date, metric) -> value; luôn dùng Y-m-d để so khớp với DB
        by_key = {}
        for row in rows:
            d = row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0])
            d = normalize_date_to_ymd(d) or d
            by_key[(d, row[1])] = float(row[2]) if row[2] is not None else 0.0
        result = {}
        for m in metric_codes:
            result[m] = [by_key.get((normalize_date_to_ymd(d) or d, m), 0.0) for d in date_list]
        return result
    except Exception as e:
        print(f"Error get_multi_metrics_for_dates {table_name}: {e}")
        return {m: [0.0] * len(date_list) for m in metric_codes}

def get_cumulative_sums_for_dates(cur, table_name, metric_code, date_list):
    """Return list of cumulative SUM from start of month to each date in date_list."""
    return [get_cumulative_sum_to_date(cur, table_name, metric_code, d) for d in date_list]

def get_monthly_sum_data(table_name, column_name, year=None, include_prev_year_dec=True):
    """Get monthly SUM data for a column - SUM all values from start to end of each month (for New Members)"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            return get_monthly_sum_data_with_conn(cur, table_name, column_name, year, cutoff_date_str=None, include_prev_year_dec=include_prev_year_dec)
    except Exception as e:
        print(f"Error getting monthly SUM data from {table_name}.{column_name}: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_monthly_data_with_conn(cur, table_name, column_name, year=None, months_back=6, include_prev_year_dec=True):
    """Get monthly data for a column using existing cursor - OPTIMIZED"""
    try:
        if year:
            prev_year = year - 1
            
            # Get December of previous year (last day of month)
            dec_prev_value = 0.0
            if include_prev_year_dec:
                dec_prev_query = f"""
                    SELECT metric_value as value
                    FROM {table_name}
                    WHERE metric_code = %s
                      AND EXTRACT(YEAR FROM trx_date) = %s
                      AND EXTRACT(MONTH FROM trx_date) = 12
                    ORDER BY trx_date DESC
                    LIMIT 1
                """
                cur.execute(dec_prev_query, (column_name, prev_year))
                dec_prev_row = cur.fetchone()
                if dec_prev_row and dec_prev_row[0] is not None:
                    dec_prev_value = float(dec_prev_row[0])
            
            # Get data for current year - get last day of each month using window function
            query = f"""
                WITH monthly_data AS (
                    SELECT 
                        EXTRACT(MONTH FROM trx_date) as month,
                        metric_value as value,
                        trx_date,
                        ROW_NUMBER() OVER (PARTITION BY EXTRACT(MONTH FROM trx_date) ORDER BY trx_date DESC) as rn
                    FROM {table_name}
                    WHERE metric_code = %s
                      AND EXTRACT(YEAR FROM trx_date) = %s
                )
                SELECT month, value
                FROM monthly_data
                WHERE rn = 1
                ORDER BY month
            """
            cur.execute(query, (column_name, year))
            rows = cur.fetchall()
            
            # Group by month - each row is already the last day of that month
            monthly_values = {}
            for row in rows:
                month = int(row[0])
                value = float(row[1]) if row[1] is not None else 0.0
                monthly_values[month] = value

            # Build result: [Dec_prev_year, Jan, Feb, ..., last_month_with_data]
            result = []
            if include_prev_year_dec:
                result.append(dec_prev_value)
            for month in range(1, 13):
                if month in monthly_values:
                    result.append(monthly_values[month])
            return result
        else:
            return []
    except Exception as e:
        print(f"Error getting monthly data from {table_name}.{column_name}: {e}")
        return []

def get_monthly_sum_data_with_conn(cur, table_name, column_name, year=None, cutoff_date_str=None, include_prev_year_dec=True):
    """Get monthly SUM data using existing cursor - OPTIMIZED.
    Behavior:
    - For months BEFORE cutoff month: SUM full month.
    - For cutoff month (same month as selected date): SUM from month start up to cutoff_date_str.
    - Only returns months up to cutoff month when cutoff_date_str is provided.
    """
    try:
        if year:
            prev_year = year - 1

            cutoff_date = None
            cutoff_month = None
            if cutoff_date_str:
                try:
                    cutoff_date = datetime.strptime(cutoff_date_str, "%Y-%m-%d").date()
                    cutoff_month = cutoff_date.month
                except Exception:
                    cutoff_date = None
                    cutoff_month = None
            
            # Get December of previous year (SUM from start of Dec to latest date in Dec)
            dec_prev_value = 0.0
            if include_prev_year_dec:
                dec_prev_query = f"""
                    SELECT COALESCE(SUM(metric_value), 0) as total
                    FROM {table_name}
                    WHERE metric_code = %s
                      AND EXTRACT(YEAR FROM trx_date) = %s
                      AND EXTRACT(MONTH FROM trx_date) = 12
                """
                cur.execute(dec_prev_query, (column_name, prev_year))
                dec_prev_row = cur.fetchone()
                if dec_prev_row and dec_prev_row[0] is not None:
                    dec_prev_value = float(dec_prev_row[0])
            
            # Get SUM data per month
            # For the cutoff month: SUM from start of month to cutoff_date only
            # For other months: SUM full month (for comparison)
            if cutoff_date_str and cutoff_date:
                # Get cutoff month and year
                cutoff_year = cutoff_date.year
                cutoff_month = cutoff_date.month
                
                query = f"""
                    SELECT
                        EXTRACT(MONTH FROM trx_date) AS month,
                        COALESCE(SUM(metric_value), 0) AS total
                    FROM {table_name}
                    WHERE metric_code = %s
                      AND EXTRACT(YEAR FROM trx_date) = %s
                      AND (
                          -- Months before cutoff month: SUM full month
                          EXTRACT(MONTH FROM trx_date) < %s
                          OR
                          -- Cutoff month: SUM only from start of month to cutoff_date
                          (EXTRACT(MONTH FROM trx_date) = %s AND trx_date <= %s)
                      )
                    GROUP BY EXTRACT(MONTH FROM trx_date)
                    ORDER BY month
                """
                cur.execute(query, (column_name, year, cutoff_month, cutoff_month, cutoff_date_str))
            else:
                # No cutoff date: SUM all months fully
                query = f"""
                    SELECT
                        EXTRACT(MONTH FROM trx_date) AS month,
                        COALESCE(SUM(metric_value), 0) AS total
                    FROM {table_name}
                    WHERE metric_code = %s
                      AND EXTRACT(YEAR FROM trx_date) = %s
                    GROUP BY EXTRACT(MONTH FROM trx_date)
                    ORDER BY month
                """
                cur.execute(query, (column_name, year))
            rows = cur.fetchall()
            
            # Group by month
            monthly_values = {}
            for row in rows:
                month = int(row[0])
                value = float(row[1]) if row[1] is not None else 0.0
                monthly_values[month] = value
            
            # Build result: [Dec_prev_year, Jan, Feb, ..., last_month_with_data]
            result = []
            if include_prev_year_dec:
                result.append(dec_prev_value)
            
            # Add months from current year - only add months that have data.
            # If cutoff is provided, only go up to cutoff_month.
            end_month = cutoff_month if cutoff_month else 12
            for month in range(1, end_month + 1):
                if month in monthly_values:
                    result.append(monthly_values[month])
            
            return result
        else:
            return []
    except Exception as e:
        print(f"Error getting monthly SUM data from {table_name}.{column_name}: {e}")
        import traceback
        print(traceback.format_exc())
        return []

def get_monthly_data(table_name, column_name, year=None, months_back=6, include_prev_year_dec=True):
    """Get monthly data for a column - includes Dec of previous year + all months of current year"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            return get_monthly_data_with_conn(cur, table_name, column_name, year, months_back, include_prev_year_dec)
            cur = conn.cursor()
            
            if year:
                prev_year = year - 1
                
                # Get December of previous year (last day of month)
                dec_prev_value = 0.0
                if include_prev_year_dec:
                    dec_prev_query = f"""
                        SELECT metric_value as value
                        FROM {table_name}
                        WHERE metric_code = %s
                          AND EXTRACT(YEAR FROM trx_date) = %s
                          AND EXTRACT(MONTH FROM trx_date) = 12
                        ORDER BY trx_date DESC
                        LIMIT 1
                    """
                    cur.execute(dec_prev_query, (column_name, prev_year))
                    dec_prev_row = cur.fetchone()
                    if dec_prev_row and dec_prev_row[0] is not None:
                        dec_prev_value = float(dec_prev_row[0])
                
                # Get data for current year - get last day of each month using window function
                query = f"""
                    WITH monthly_data AS (
                        SELECT 
                            EXTRACT(MONTH FROM trx_date) as month,
                            metric_value as value,
                            trx_date,
                            ROW_NUMBER() OVER (PARTITION BY EXTRACT(MONTH FROM trx_date) ORDER BY trx_date DESC) as rn
                        FROM {table_name}
                        WHERE metric_code = %s
                          AND EXTRACT(YEAR FROM trx_date) = %s
                    )
                    SELECT month, value
                    FROM monthly_data
                    WHERE rn = 1
                    ORDER BY month
                """
                cur.execute(query, (column_name, year))
                rows = cur.fetchall()
                
                # Group by month - each row is already the last day of that month
                monthly_values = {}
                for row in rows:
                    month = int(row[0])
                    value = float(row[1]) if row[1] is not None else 0.0
                    monthly_values[month] = value
                
                # Build result: [Dec_prev_year, Jan, Feb, ..., last_month_with_data]
                result = []
                if include_prev_year_dec:
                    result.append(dec_prev_value)
                
                # Add months from current year (1-12) - only add months that have data
                for month in range(1, 13):
                    if month in monthly_values:
                        result.append(monthly_values[month])
                
                return result
            else:
                # Get last N months
                query = f"""
                    SELECT 
                        EXTRACT(MONTH FROM trx_date) as month,
                        EXTRACT(YEAR FROM trx_date) as year,
                        metric_value as value
                    FROM {table_name}
                    WHERE metric_code = %s
                      AND trx_date >= CURRENT_DATE - INTERVAL '%s months'
                    ORDER BY trx_date DESC
                """
                cur.execute(query, (column_name, months_back))
                rows = cur.fetchall()
                
                # Group by month and get latest value for each month
                monthly_values = {}
                for row in rows:
                    month = int(row[0])
                    value = float(row[1]) if row[1] is not None else 0.0
                    if month not in monthly_values:
                        monthly_values[month] = value
                
                # Return values in order
                result = []
                for month in range(1, 13):
                    if month in monthly_values:
                        result.append(monthly_values[month])
                    elif len(result) > 0:
                        result.append(result[-1])
                return result[:months_back]
    except Exception as e:
        print(f"Error getting monthly data from {table_name}.{column_name}: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_latest_date():
    """Get the latest date available in any card table"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            latest_date = None
            for table_name in TABLE_MAP.values():
                try:
                    cur.execute(f'SELECT MAX(trx_date) FROM {table_name}')
                    row = cur.fetchone()
                    if row and row[0]:
                        table_date = row[0]
                        if latest_date is None or table_date > latest_date:
                            latest_date = table_date
                except Exception as table_err:
                    print(f"Error querying {table_name}: {table_err}")
                    conn.rollback()
                    continue
            return latest_date.strftime('%Y-%m-%d') if latest_date else None
    except Exception as e:
        print(f"Error getting latest date: {e}")
        return None

def get_available_dates():
    """Get all available dates from card tables"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            all_dates = set()
            for table_name in TABLE_MAP.values():
                try:
                    cur.execute(f'SELECT DISTINCT trx_date FROM {table_name} ORDER BY trx_date DESC')
                    rows = cur.fetchall()
                    for row in rows:
                        if row[0]:
                            all_dates.add(row[0].strftime('%Y-%m-%d'))
                except:
                    continue
            return sorted(list(all_dates), reverse=True)
    except Exception as e:
        print(f"Error getting available dates: {e}")
        return []

def calculate_attrition_rate(date_str):
    """Calculate Attrition Rate = (Member_Closed / EOP_Consumer) * 100"""
    try:
        member_closed = get_value_from_table(get_table_name('member_closed'), 'Member_Closed', date_str)
        eop_consumer = get_value_from_table(get_table_name('member_eop'), 'EOP_Consumer', date_str)
        if eop_consumer > 0:
            return (member_closed / eop_consumer) * 100
        return 0.0
    except Exception as e:
        print(f"Error calculating attrition rate: {e}")
        return 0.0

def calculate_avg_balance_per_member(date_str):
    """Calculate Avg Balance Per Member = Total_Outstanding_Balance / (EOP_Consumer + EOP_Corporate)"""
    try:
        total_balance = get_value_from_table(get_table_name('balance_eop'), 'Total_Outstanding_Balance', date_str)
        eop_consumer = get_value_from_table(get_table_name('member_eop'), 'EOP_Consumer', date_str)
        eop_corporate = get_value_from_table(get_table_name('member_eop'), 'EOP_Corporate', date_str)
        total_members = eop_consumer + eop_corporate
        if total_members > 0:
            return total_balance / total_members
        return 0.0
    except Exception as e:
        print(f"Error calculating avg balance per member: {e}")
        return 0.0

def calculate_avg_spending_per_member(date_str, customer_type='all'):
    """
    Calculate Avg Spending Per Member.
    - all: Total_Spending_Amt / (EOP_Consumer + EOP_Corporate)
    - consumer: Total_Spending_Amt_Consumer / EOP_Consumer
    - corporate: Total_Spending_Amt_Corporate / EOP_Corporate
    """
    try:
        customer_type = (customer_type or 'all').strip().lower()
        if customer_type == 'consumer':
            total_spending = get_value_from_table(get_table_name('spend_sector'), 'Total_Spending_Amt_Consumer', date_str)
            members = get_value_from_table(get_table_name('member_eop'), 'EOP_Consumer', date_str)
        elif customer_type == 'corporate':
            total_spending = get_value_from_table(get_table_name('spend_sector'), 'Total_Spending_Amt_Corporate', date_str)
            members = get_value_from_table(get_table_name('member_eop'), 'EOP_Corporate', date_str)
        else:
            total_spending = get_value_from_table(get_table_name('spend_sector'), 'Total_Spending_Amt', date_str)
            eop_consumer = get_value_from_table(get_table_name('member_eop'), 'EOP_Consumer', date_str)
            eop_corporate = get_value_from_table(get_table_name('member_eop'), 'EOP_Corporate', date_str)
            members = eop_consumer + eop_corporate

        if members > 0:
            return total_spending / members
        return 0.0
    except Exception as e:
        print(f"Error calculating avg spending per member: {e}")
        return 0.0

def calculate_usage_rate(date_str):
    """Calculate Usage Rate = (Using_Consumer + Using_Corporate) / (EOP_Consumer + EOP_Corporate) * 100"""
    try:
        using_consumer = get_value_from_table(get_table_name('member_usage'), 'Using_Consumer', date_str)
        using_corporate = get_value_from_table(get_table_name('member_usage'), 'Using_Corporate', date_str)
        eop_consumer = get_value_from_table(get_table_name('member_eop'), 'EOP_Consumer', date_str)
        eop_corporate = get_value_from_table(get_table_name('member_eop'), 'EOP_Corporate', date_str)
        total_eop = eop_consumer + eop_corporate
        if total_eop > 0:
            return ((using_consumer + using_corporate) / total_eop) * 100.0
        return 0.0
    except Exception as e:
        print(f"Error calculating usage rate: {e}")
        return 0.0

def calculate_attrition_rate_combined(date_str):
    """Calculate Attrition Rate = (Consumer_Closed + Corporate_Closed) / (EOP_Consumer + EOP_Corporate) * 100"""
    try:
        consumer_closed = get_value_from_table(get_table_name('member_closed'), 'Consumer_Closed', date_str)
        corporate_closed = get_value_from_table(get_table_name('member_closed'), 'Corporate_Closed', date_str)
        eop_consumer = get_value_from_table(get_table_name('member_eop'), 'EOP_Consumer', date_str)
        eop_corporate = get_value_from_table(get_table_name('member_eop'), 'EOP_Corporate', date_str)
        total_eop = eop_consumer + eop_corporate
        if total_eop > 0:
            return ((consumer_closed + corporate_closed) / total_eop) * 100.0
        return 0.0
    except Exception as e:
        print(f"Error calculating combined attrition rate: {e}")
        return 0.0

def calculate_usage_rate_with_conn(cur, date_str):
    """Usage Rate reusing cursor - avoids extra DB connections."""
    try:
        meop = get_table_name('member_eop')
        musage = get_table_name('member_usage')
        using_consumer = get_value_with_cur(cur, musage, 'Using_Consumer', date_str)
        using_corporate = get_value_with_cur(cur, musage, 'Using_Corporate', date_str)
        eop_consumer = get_value_with_cur(cur, meop, 'EOP_Consumer', date_str)
        eop_corporate = get_value_with_cur(cur, meop, 'EOP_Corporate', date_str)
        total_eop = eop_consumer + eop_corporate
        if total_eop > 0:
            return ((using_consumer + using_corporate) / total_eop) * 100.0
        return 0.0
    except Exception as e:
        print(f"Error calculate_usage_rate_with_conn: {e}")
        return 0.0

def calculate_attrition_rate_combined_with_conn(cur, date_str):
    """Attrition Rate combined reusing cursor."""
    try:
        mclosed = get_table_name('member_closed')
        meop = get_table_name('member_eop')
        consumer_closed = get_value_with_cur(cur, mclosed, 'Consumer_Closed', date_str)
        corporate_closed = get_value_with_cur(cur, mclosed, 'Corporate_Closed', date_str)
        eop_consumer = get_value_with_cur(cur, meop, 'EOP_Consumer', date_str)
        eop_corporate = get_value_with_cur(cur, meop, 'EOP_Corporate', date_str)
        total_eop = eop_consumer + eop_corporate
        if total_eop > 0:
            return ((consumer_closed + corporate_closed) / total_eop) * 100.0
        return 0.0
    except Exception as e:
        print(f"Error calculate_attrition_rate_combined_with_conn: {e}")
        return 0.0

def calculate_avg_balance_per_member_with_conn(cur, date_str):
    """Avg Balance Per Member = Total_Outstanding_Balance / (EOP_Consumer + EOP_Corporate) - reuses cursor."""
    try:
        bal = get_table_name('balance_eop')
        meop = get_table_name('member_eop')
        total_balance = get_value_with_cur(cur, bal, 'Total_Outstanding_Balance', date_str)
        eop_consumer = get_value_with_cur(cur, meop, 'EOP_Consumer', date_str)
        eop_corporate = get_value_with_cur(cur, meop, 'EOP_Corporate', date_str)
        total_members = eop_consumer + eop_corporate
        if total_members > 0:
            return total_balance / total_members
        return 0.0
    except Exception as e:
        print(f"Error calculate_avg_balance_per_member_with_conn: {e}")
        return 0.0

def calculate_avg_spending_per_member_with_conn(cur, date_str, customer_type='all'):
    """Avg Spending Per Member reusing cursor."""
    try:
        customer_type = (customer_type or 'all').strip().lower()
        spend = get_table_name('spend_sector')
        meop = get_table_name('member_eop')
        if customer_type == 'consumer':
            total_spending = get_value_with_cur(cur, spend, 'Total_Spending_Amt_Consumer', date_str)
            members = get_value_with_cur(cur, meop, 'EOP_Consumer', date_str)
        elif customer_type == 'corporate':
            total_spending = get_value_with_cur(cur, spend, 'Total_Spending_Amt_Corporate', date_str)
            members = get_value_with_cur(cur, meop, 'EOP_Corporate', date_str)
        else:
            total_spending = get_value_with_cur(cur, spend, 'Total_Spending_Amt', date_str)
            eop_c = get_value_with_cur(cur, meop, 'EOP_Consumer', date_str)
            eop_co = get_value_with_cur(cur, meop, 'EOP_Corporate', date_str)
            members = eop_c + eop_co
        if members > 0:
            return total_spending / members
        return 0.0
    except Exception as e:
        print(f"Error calculate_avg_spending_per_member_with_conn: {e}")
        return 0.0

def get_comparison_dates(target_date):
    """
    Get comparison dates for overview ticker (vs Last Month / vs Last Year).
    Returns:
      last_day:  nearest date in DB <= (target_date - 1 day)
      last_month: last date in DB that falls in the previous calendar month
                 (e.g. target 14 Jan 2026 → last date in Dec 2025)
      last_year:  last date in DB that falls in the previous calendar year
                 (e.g. target 14 Jan 2026 → last date in 2025, often 31 Dec 2025)
    Overview % = (value_at_target_date - value_at_comparison_date) / value_at_comparison_date * 100.
    """
    try:
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        
        # Helper function to find nearest available date in database
        def find_nearest_date(target_dt):
            try:
                with get_db_connection() as conn:
                    cur = conn.cursor()
                    query = f"""
                        SELECT MAX(trx_date) as nearest_date
                        FROM {get_table_name('member_eop')}
                        WHERE trx_date <= %s
                    """
                    cur.execute(query, (target_dt.strftime('%Y-%m-%d'),))
                    row = cur.fetchone()
                    if row and row[0]:
                        return row[0].strftime('%Y-%m-%d')
            except Exception as e:
                print(f"Error finding nearest date for {target_dt}: {e}")
            return None
        
        # Helper function to find last date of a specific month/year
        def find_last_date_of_month(year, month):
            try:
                with get_db_connection() as conn:
                    cur = conn.cursor()
                    query = f"""
                        SELECT MAX(trx_date) as last_date
                        FROM {get_table_name('member_eop')}
                        WHERE EXTRACT(YEAR FROM trx_date) = %s 
                        AND EXTRACT(MONTH FROM trx_date) = %s
                    """
                    cur.execute(query, (year, month))
                    row = cur.fetchone()
                    if row and row[0]:
                        return row[0].strftime('%Y-%m-%d')
            except Exception as e:
                print(f"Error finding last date of {year}-{month}: {e}")
            return None
        
        # Last day (previous day) - find nearest available
        last_day_target = (date_obj - timedelta(days=1)).strftime('%Y-%m-%d')
        last_day = find_nearest_date(date_obj - timedelta(days=1)) or last_day_target
        
        # Last month: last day of previous month (same logic as Dashboard_RCD)
        if date_obj.month == 1:
            prev_month = 12
            prev_year = date_obj.year - 1
        else:
            prev_month = date_obj.month - 1
            prev_year = date_obj.year
        
        # Find last date of previous month in database
        last_month = find_last_date_of_month(prev_year, prev_month)
        if not last_month:
            # Fallback to calculated last day of month
            last_day_of_prev_month = calendar.monthrange(prev_year, prev_month)[1]
            last_month = datetime(prev_year, prev_month, last_day_of_prev_month).strftime('%Y-%m-%d')
        
        # Last year: MAX date of previous year in DB
        prev_year_for_comparison = date_obj.year - 1
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                query = f"""
                    SELECT MAX(trx_date) as last_date
                    FROM {get_table_name('member_eop')}
                    WHERE EXTRACT(YEAR FROM trx_date) = %s
                """
                cur.execute(query, (prev_year_for_comparison,))
                row = cur.fetchone()
                if row and row[0]:
                    last_year = row[0].strftime('%Y-%m-%d')
                else:
                    # Fallback to Dec 31 of previous year
                    last_year = datetime(prev_year_for_comparison, 12, 31).strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error finding last year date: {e}")
            last_year = datetime(prev_year_for_comparison, 12, 31).strftime('%Y-%m-%d')
        
        return last_day, last_month, last_year
    except Exception as e:
        print(f"Error getting comparison dates: {e}")
        return None, None, None

@app.route('/api/dashboard/available-dates')
def available_dates():
    """Get all available dates"""
    try:
        print(f"[API] available-dates called", flush=True)
        dates = get_available_dates()
        print(f"[API] Found {len(dates)} available dates", flush=True)
        return jsonify({'dates': dates})
    except Exception as e:
        import traceback
        print(f"[API ERROR] available-dates error: {str(e)}", flush=True)
        print(f"[API ERROR] Traceback: {traceback.format_exc()}", flush=True)
        return handle_error(e, 'available-dates')

# Ticker config: (table_key, metric_consumer, metric_corporate). Ticker 4 & 5 in K USD (divide by 1000).
TICKER_CONFIG = [
    ('new_consumer', 'New_Consumer', 'New_Corporate', True),   # 1 New Acquisition (cumulative)
    ('member_eop', 'EOP_Consumer', 'EOP_Corporate', False),   # 2 Customer Base
    ('member_usage', 'Using_Consumer', 'Using_Corporate', False),  # 3 Using Customer
    ('spend_sector', 'Total_Spending_Amt_Consumer', 'Total_Spending_Amt_Corporate', False),  # 4 Sales Volume K USD
    ('balance_eop', 'Balance_Consumer', 'Balance_Corporate', False),  # 5 Balance K USD
]

# rptmon table mapping: ticker_idx -> (cat1, values_already_in_K_USD)
# cat2: Consumer, Corporate, Total <- from customer_type
RPTMON_CAT1 = [
    'New Mem',             # 1 New Acquisition
    'Customer Base',       # 2 Customer Base
    'Accum Using Member',  # 3 Using Customer
    'Sales Volume (1000)', # 4 Sales Volume K (rptmon already in K)
    'Balance (1000)',      # 5 Balance K (rptmon already in K)
]


def get_rptmon_value(cur, rptmon_yyyymm, cat1, cat2, card_type='All Card'):
    """Get value from rptmon table. Returns None if not found or table missing."""
    try:
        cur.execute(
            """SELECT "value" FROM rptmon WHERE rptmon = %s AND card_type = %s AND cat1 = %s AND cat2 = %s LIMIT 1""",
            (str(rptmon_yyyymm), card_type, cat1, cat2)
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None
    except Exception as e:
        print(f"[API] get_rptmon_value error (table may not exist): {e}")
        return None


def get_rptmon_ytd_sum(cur, year, to_month, cat1, cat2, card_type='All Card'):
    """Sum rptmon values from Jan to to_month (inclusive) for YTD target."""
    try:
        rptmons = [f"{year}{m:02d}" for m in range(1, to_month + 1)]
        placeholders = ','.join(['%s'] * len(rptmons))
        cur.execute(
            f"""SELECT COALESCE(SUM("value"), 0) FROM rptmon
                WHERE rptmon IN ({placeholders}) AND card_type = %s AND cat1 = %s AND cat2 = %s""",
            rptmons + [card_type, cat1, cat2]
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        print(f"[API] get_rptmon_ytd_sum error: {e}")
        return 0.0

@app.route('/api/dashboard/full-stats')
def full_stats():
    """Get full dashboard statistics. Query: date, customer_type=consumer|corporate."""
    try:
        start_time = time.time()
        date_param = request.args.get('date')
        date_param = normalize_date_param(date_param) or date_param  # Chuẩn hóa DD.MM.YYYY / YYYYMMDD -> Y-m-d
        customer_type = (request.args.get('customer_type') or 'consumer').strip().lower()
        if customer_type not in ('consumer', 'corporate', 'all'):
            customer_type = 'consumer'
        print(f"[API] full-stats date_param={date_param} customer_type={customer_type}")
        if not date_param:
            date_param = get_latest_date()
            print(f"[API] Using latest date: {date_param}")
            if not date_param:
                return jsonify({'error': 'No data available', 'message': 'No dates found in database'}), 404

        cache_key = (date_param, customer_type)
        now = time.time()
        if cache_key in _full_stats_cache:
            cached_at, cached_resp = _full_stats_cache[cache_key]
            if now - cached_at <= FULL_STATS_CACHE_TTL:
                print(f"[API] full-stats cache HIT for {cache_key}")
                return jsonify(sanitize_for_json(cached_resp))
        
        # Get comparison dates
        last_day, last_month, last_year = get_comparison_dates(date_param)
        print(f"[API] Comparison dates - last_day: {last_day}, last_month: {last_month}, last_year: {last_year}")
        
        # Helper function to calculate percentage change
        def calc_pct_change(current, previous):
            if previous == 0:
                return 0.0 if current == 0 else 100.0
            return ((current - previous) / previous) * 100.0
        
        # ============ OPTIMIZED: Use single connection for all queries ============
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Get all current date data in batch (ONE connection, multiple tables)
            print(f"[API] Fetching data for date: {date_param}")
            all_current = get_all_tables_data_batch(cur, date_param)
            print(f"[API] Data fetched - member_eop keys: {list(all_current.get('member_eop', {}).keys())[:5]}")
            print(f"[API] Data fetched - risk_dpd keys: {list(all_current.get('risk_dpd', {}).keys())[:5]}")
            
            # Get comparison data using batch function
            dates_dict = {
                'last_day': last_day,
                'last_month': last_month,
                'last_year': last_year
            }
            
            metrics_to_fetch = [
                ('member_eop', 'EOP_Consumer', 'eop_consumer', False),
                ('member_usage', 'Using_Consumer', 'using_consumer', False),
                ('balance_eop', 'Total_Outstanding_Balance', 'total_outstanding', False),
                ('spend_sector', 'Total_Spending_Amt', 'total_spending', False),
                ('new_consumer', 'New_Consumer', 'new_consumer', True),  # Cumulative: SUM from start of month
                ('new_consumer', 'New_Corporate', 'new_corporate', True),  # Cumulative: SUM from start of month
                ('risk_dpd', 'DPD_1P_Rate', 'dpd_1_rate', False),
                ('risk_dpd', 'DPD_10P_Rate', 'dpd_10_rate', False),
                ('risk_dpd', 'DPD_30P_Rate', 'dpd_30_rate', False),
                ('risk_dpd', 'NPL_90P_Rate', 'npl_rate', False),
            ]
            
            comparison_data = get_comparison_values_batch(cur, dates_dict, metrics_to_fetch)
            print(f"[API] Comparison data fetched - last_day dpd_1_rate: {comparison_data.get('last_day', {}).get('dpd_1_rate', 'N/A')}")
            
            # Monthly data: 6 points = last day in DB for 5 months before selected month + selected date
            date_obj = datetime.strptime(date_param, '%Y-%m-%d')
            year = date_obj.year
            current_month = date_obj.month
            six_dates, months_label = get_last_6_dates_and_labels(cur, date_param)
            months_with_data = list(range(1, 7))  # 6 slots for charts
            table_date_labels = months_label

            monthly_data_start = time.time()
            print(f"[API] Fetching 6-point monthly data (5 months + selected date): {months_label}")

            meop, musage, mclosed, bal, spend, risk_dpd, spend_loc, new_consumer_t = (
                get_table_name('member_eop'), get_table_name('member_usage'), get_table_name('member_closed'),
                get_table_name('balance_eop'), get_table_name('spend_sector'), get_table_name('risk_dpd'),
                get_table_name('spend_location'), get_table_name('new_consumer')
            )

            # Batch fetch snapshot metrics (one query per table instead of per metric per date)
            meop_b = get_multi_metrics_for_dates(cur, meop, ['EOP_Consumer', 'EOP_Corporate'], six_dates)
            musage_b = get_multi_metrics_for_dates(cur, musage, ['Using_Consumer', 'Using_Corporate'], six_dates)
            mclosed_b = get_multi_metrics_for_dates(cur, mclosed, ['Consumer_Closed', 'Corporate_Closed', 'Member_Closed'], six_dates)
            bal_b = get_multi_metrics_for_dates(cur, bal, ['Total_Outstanding_Balance'], six_dates)
            spend_b = get_multi_metrics_for_dates(cur, spend, ['Total_Spending_Amt', 'Total_Spending_Amt_Consumer', 'Total_Spending_Amt_Corporate'], six_dates)
            risk_b = get_multi_metrics_for_dates(cur, risk_dpd, ['Revolving_Balance', 'Revolving_Rate', 'DPD_1P_Rate', 'DPD_10P_Rate', 'DPD_30P_Rate', 'NPL_90P_Rate', 'Revolving_Member'], six_dates)
            spend_loc_b = get_multi_metrics_for_dates(cur, spend_loc, ['Domestic_Spending_Amt', 'XBorder_Spending_Amt'], six_dates)
            # Cumulative metrics: still one call per metric
            new_consumer_6 = get_cumulative_sums_for_dates(cur, new_consumer_t, 'New_Consumer', six_dates)
            new_corporate_6 = get_cumulative_sums_for_dates(cur, new_consumer_t, 'New_Corporate', six_dates)
            # Consumer breakdown: Smart Credit + Branches (để chart New Acquisition hiển thị 2 cột khi chọn Consumer)
            new_consumer_smartcredit_6 = get_cumulative_sums_for_dates(cur, new_consumer_t, 'New_Consumer_SmartCredit', six_dates)
            new_consumer_branches_6 = get_cumulative_sums_for_dates(cur, new_consumer_t, 'New_Consumer_Branches', six_dates)
            new_corporate_smartcredit_6 = get_cumulative_sums_for_dates(cur, new_consumer_t, 'New_Corporate_SmartCredit', six_dates)
            new_corporate_branches_6 = get_cumulative_sums_for_dates(cur, new_consumer_t, 'New_Corporate_Branches', six_dates)

            charts_monthly = {
                'member_growth': {
                    'eop_consumer': meop_b['EOP_Consumer'],
                    'using_consumer': musage_b['Using_Consumer'],
                },
                'new_closed': {
                    'new_consumer': new_consumer_6,
                    'new_consumer_smartcredit': new_consumer_smartcredit_6,
                    'new_consumer_branches': new_consumer_branches_6,
                    'consumer_closed': mclosed_b['Consumer_Closed'],
                    'new_corporate': new_corporate_6,
                    'new_corporate_smartcredit': new_corporate_smartcredit_6,
                    'new_corporate_branches': new_corporate_branches_6,
                    'corporate_closed': mclosed_b['Corporate_Closed']
                },
                'financial_trends': {
                    'outstanding': bal_b['Total_Outstanding_Balance'],
                    'spending': (
                        spend_b['Total_Spending_Amt_Consumer'] if customer_type == 'consumer' else
                        spend_b['Total_Spending_Amt_Corporate'] if customer_type == 'corporate' else
                        spend_b['Total_Spending_Amt']
                    )
                },
                'revolving': {
                    'balance': risk_b['Revolving_Balance'],
                    'rate': risk_b['Revolving_Rate']
                },
                'location_mom': {
                    'domestic': spend_loc_b['Domestic_Spending_Amt'],
                    'x_border': spend_loc_b['XBorder_Spending_Amt']
                }
            }

            risk_monthly = {
                'trend_lines': {
                    'dpd_1': risk_b['DPD_1P_Rate'],
                    'dpd_30': risk_b['DPD_30P_Rate'],
                    'npl': risk_b['NPL_90P_Rate']
                }
            }

            table_monthly = {
                'eop_consumer': meop_b['EOP_Consumer'],
                'using_consumer': musage_b['Using_Consumer'],
                'new_consumer': new_consumer_6,
                'new_consumer_smartcredit': new_consumer_smartcredit_6,
                'new_consumer_branches': new_consumer_branches_6,
                'consumer_closed': mclosed_b['Consumer_Closed'],
                'member_closed': mclosed_b['Member_Closed'],
                'attrition_rate': [],
                'eop_corporate': meop_b['EOP_Corporate'],
                'using_corporate': musage_b['Using_Corporate'],
                'new_corporate': new_corporate_6,
                'new_corporate_smartcredit': new_corporate_smartcredit_6,
                'new_corporate_branches': new_corporate_branches_6,
                'corporate_closed': mclosed_b['Corporate_Closed'],
                'total_outstanding': bal_b['Total_Outstanding_Balance'],
                'total_spending': (
                    spend_b['Total_Spending_Amt_Consumer'] if customer_type == 'consumer' else
                    spend_b['Total_Spending_Amt_Corporate'] if customer_type == 'corporate' else
                    spend_b['Total_Spending_Amt']
                ),
                'revolving_balance': risk_b['Revolving_Balance'],
                'revolving_member': risk_b['Revolving_Member'],
                'revolving_rate': risk_b['Revolving_Rate'],
                'dpd_1_rate': risk_b['DPD_1P_Rate'],
                'dpd_10_rate': risk_b['DPD_10P_Rate'],
                'dpd_30_rate': risk_b['DPD_30P_Rate'],
                'npl_rate': risk_b['NPL_90P_Rate']
            }

            monthly_data_time = time.time() - monthly_data_start
            print(f"[API] All 6-point monthly data fetched in {monthly_data_time:.2f}s")
            
            # Extract current data from batch results - MUST BE INSIDE with block to use cursor
            current_data = {}
            
            # Member EOP
            member_eop = all_current.get('member_eop', {})
            current_data['eop_consumer'] = float(member_eop.get('EOP_Consumer', 0) or 0)
            current_data['eop_corporate'] = float(member_eop.get('EOP_Corporate', 0) or 0)
            
            # Member Usage
            member_usage = all_current.get('member_usage', {})
            current_data['using_consumer'] = float(member_usage.get('Using_Consumer', 0) or 0)
            current_data['using_corporate'] = float(member_usage.get('Using_Corporate', 0) or 0)
            
            # New Consumer - Calculate cumulative from start of month to date_param
            # MUST be inside with block because it uses cursor
            print(f"[DEBUG] Calculating New Consumer/Corporate for date: {date_param}")
            try:
                # Check if cursor is still open
                if cur.closed:
                    print("[ERROR] Cursor is closed! Creating new cursor...")
                    cur = conn.cursor()
                
                current_data['new_consumer'] = get_cumulative_sum_to_date(cur, get_table_name('new_consumer'), 'New_Consumer', date_param)
                current_data['new_corporate'] = get_cumulative_sum_to_date(cur, get_table_name('new_consumer'), 'New_Corporate', date_param)
                print(f"[DEBUG] Final values - new_consumer: {current_data['new_consumer']}, new_corporate: {current_data['new_corporate']}")
            except Exception as e:
                print(f"[ERROR] Error calculating New Consumer/Corporate: {e}")
                import traceback
                print(traceback.format_exc())
                # Fallback: use values from all_current (single day value, not cumulative)
                new_consumer_fallback = all_current.get('new_consumer', {})
                current_data['new_consumer'] = float(new_consumer_fallback.get('New_Consumer', 0) or 0)
                current_data['new_corporate'] = float(new_consumer_fallback.get('New_Corporate', 0) or 0)
            
            new_consumer = all_current.get('new_consumer', {})
            current_data['new_member'] = float(new_consumer.get('New_Member', 0) or 0)
            
            # Member Closed
            member_closed = all_current.get('member_closed', {})
            current_data['consumer_closed'] = float(member_closed.get('Consumer_Closed', 0) or 0)
            current_data['corporate_closed'] = float(member_closed.get('Corporate_Closed', 0) or 0)
            current_data['member_closed'] = float(member_closed.get('Member_Closed', 0) or 0)
            
            # Balance EOP
            balance_eop = all_current.get('balance_eop', {})
            current_data['total_outstanding'] = float(balance_eop.get('Total_Outstanding_Balance', 0) or 0)
            
            # Spend Sector
            spend_sector = all_current.get('spend_sector', {})
            current_data['total_spending'] = float(spend_sector.get('Total_Spending_Amt', 0) or 0)
            current_data['purchasing_amt'] = float(spend_sector.get('Purchasing_Amt', 0) or 0)
            current_data['cash_advance_amt'] = float(spend_sector.get('Cash_Advance_Amt', 0) or 0)
            current_data['installment_amt'] = float(spend_sector.get('Installment_Amt', 0) or 0)
            current_data['flexi_amt'] = float(spend_sector.get('Flexi_Amt', 0) or 0)
            
            # Spend Location (from batch)
            spend_location = all_current.get('spend_location', {})
            current_data['domestic_spending'] = float(spend_location.get('Domestic_Spending_Amt', 0) or 0)
            current_data['xborder_spending'] = float(spend_location.get('XBorder_Spending_Amt', 0) or 0)
            
            # Spend Channel (from batch)
            spend_channel = all_current.get('spend_channel', {})
            current_data['pos_spending'] = float(spend_channel.get('POS_Spending_Amt', 0) or 0)
            current_data['online_spending'] = float(spend_channel.get('Online_Spending_Amt', 0) or 0)
            
            # Risk DPD (from batch)
            risk_dpd = all_current.get('risk_dpd', {})
            print(f"[API] risk_dpd keys available: {list(risk_dpd.keys())}")
            current_data['revolving_balance'] = float(risk_dpd.get('Revolving_Balance', 0) or 0)
            current_data['revolving_member'] = float(risk_dpd.get('Revolving_Member', 0) or 0)
            current_data['revolving_rate'] = float(risk_dpd.get('Revolving_Rate', 0) or 0)
            current_data['dpd_1_rate'] = float(risk_dpd.get('DPD_1P_Rate', 0) or 0)
            current_data['dpd_10_rate'] = float(risk_dpd.get('DPD_10P_Rate', 0) or 0)
            current_data['dpd_30_rate'] = float(risk_dpd.get('DPD_30P_Rate', 0) or 0)
            current_data['npl_rate'] = float(risk_dpd.get('NPL_90P_Rate', 0) or 0)
            print(f"[API] DPD rates - dpd_1: {current_data['dpd_1_rate']}, dpd_10: {current_data['dpd_10_rate']}, dpd_30: {current_data['dpd_30_rate']}, npl: {current_data['npl_rate']}")
            
            # Calculate derived metrics - MUST be inside with block to reuse cursor
            # Calculate attrition rate using current_data values (already fetched)
            total_eop = current_data['eop_consumer'] + current_data['eop_corporate']
            if total_eop > 0:
                current_data['attrition_rate'] = ((current_data['consumer_closed'] + current_data['corporate_closed']) / total_eop) * 100.0
            else:
                current_data['attrition_rate'] = 0.0
            
            # Calculate avg balance per member
            if total_eop > 0:
                current_data['avg_balance'] = current_data['total_outstanding'] / total_eop
            else:
                current_data['avg_balance'] = 0.0
            
            # Calculate avg spending per member - respect customer_type
            if customer_type == 'consumer':
                denom = current_data['eop_consumer']
                numer = current_data['total_spending']  # Total_Spending_Amt_Consumer used when building current_data
            elif customer_type == 'corporate':
                denom = current_data['eop_corporate']
                numer = current_data['total_spending']  # Total_Spending_Amt_Corporate in corporate view
            else:
                denom = total_eop
                numer = current_data['total_spending']

            if denom > 0:
                current_data['avg_spending'] = numer / denom
            else:
                current_data['avg_spending'] = 0.0

            # --- Tickers 1-5 by customer_type (Consumer vs Corporate vs All) ---
            # Current values: from all_current / current_data; K USD for tickers 4 & 5
            spend_sector = all_current.get('spend_sector', {})
            balance_eop = all_current.get('balance_eop', {})
            if customer_type == 'all':
                sales_c = float(spend_sector.get('Total_Spending_Amt_Consumer', 0) or 0)
                sales_co = float(spend_sector.get('Total_Spending_Amt_Corporate', 0) or 0)
                balance_c = float(balance_eop.get('Balance_Consumer', 0) or 0)
                balance_co = float(balance_eop.get('Balance_Corporate', 0) or 0)
                sales_k = (sales_c + sales_co) / 1000.0
                balance_k = (balance_c + balance_co) / 1000.0
                ticker_current = [
                    current_data['new_consumer'] + current_data['new_corporate'],
                    current_data['eop_consumer'] + current_data['eop_corporate'],
                    current_data['using_consumer'] + current_data['using_corporate'],
                    sales_k,
                    balance_k,
                ]
            else:
                spend_key = 'Total_Spending_Amt_Consumer' if customer_type == 'consumer' else 'Total_Spending_Amt_Corporate'
                balance_key = 'Balance_Consumer' if customer_type == 'consumer' else 'Balance_Corporate'
                sales_k = (float(spend_sector.get(spend_key, 0) or 0) / 1000.0)
                balance_k = (float(balance_eop.get(balance_key, 0) or 0) / 1000.0)
                ticker_current = [
                    current_data['new_consumer'] if customer_type == 'consumer' else current_data['new_corporate'],
                    current_data['eop_consumer'] if customer_type == 'consumer' else current_data['eop_corporate'],
                    current_data['using_consumer'] if customer_type == 'consumer' else current_data['using_corporate'],
                    sales_k,
                    balance_k,
                ]
            ticker_labels = ['New Acquisition', 'Customer Base', 'Using Customer', 'Sales Volume (K USD)', 'Balance (K USD)']
            ticker_last_month = []
            ticker_last_year = []

            def _fetch_val(tn, m, dt, is_cum):
                if is_cum:
                    return get_cumulative_sum_to_date(cur, tn, m, dt)
                cur.execute("SELECT metric_value FROM {} WHERE metric_code = %s AND trx_date = %s LIMIT 1".format(tn), (m, dt))
                row = cur.fetchone()
                return float(row[0]) if row and row[0] else 0.0

            for i, (table_key, metric_consumer, metric_corporate, is_cumulative) in enumerate(TICKER_CONFIG):
                table_name = get_table_name(table_key)
                div_k = (i in (3, 4))

                if customer_type == 'all':
                    if last_month:
                        v_c = _fetch_val(table_name, metric_consumer, last_month, is_cumulative)
                        v_co = _fetch_val(table_name, metric_corporate, last_month, is_cumulative)
                        v = v_c + v_co
                        if div_k:
                            v = v / 1000.0
                        ticker_last_month.append(v)
                    else:
                        ticker_last_month.append(0.0)
                    if last_year:
                        v_c = _fetch_val(table_name, metric_consumer, last_year, is_cumulative)
                        v_co = _fetch_val(table_name, metric_corporate, last_year, is_cumulative)
                        v = v_c + v_co
                        if div_k:
                            v = v / 1000.0
                        ticker_last_year.append(v)
                    else:
                        ticker_last_year.append(0.0)
                else:
                    metric = metric_consumer if customer_type == 'consumer' else metric_corporate
                    if last_month:
                        v = _fetch_val(table_name, metric, last_month, is_cumulative)
                        if div_k:
                            v = v / 1000.0
                        ticker_last_month.append(v)
                    else:
                        ticker_last_month.append(0.0)
                    if last_year:
                        v = _fetch_val(table_name, metric, last_year, is_cumulative)
                        if div_k:
                            v = v / 1000.0
                        ticker_last_year.append(v)
                    else:
                        ticker_last_year.append(0.0)
            tickers = {}
            cat2_map = {'consumer': 'Consumer', 'corporate': 'Corporate', 'all': 'Total'}
            rptmon_cat2 = cat2_map.get(customer_type, 'Total')
            # Tháng target = đúng tháng của report date (120% of Month Target)
            rptmon_yyyymm = year * 100 + current_month

            # 6-point arrays: index 5 = current period (selected date), 4 = previous period
            tnew_c = table_monthly.get('new_consumer', [])
            tnew_co = table_monthly.get('new_corporate', [])
            tspend = table_monthly.get('total_spending', [])

            # Overview ticker: vs_last_month / vs_last_year = % change from comparison date to selected date.
            # cur_val = value at date_param; prev_m = value at last_month; prev_y = value at last_year.
            # Snapshot metrics: value at that date. New Acquisition: cumulative in that month to that date.
            for i in range(5):
                cur_val = ticker_current[i]
                prev_m = ticker_last_month[i]
                prev_y = ticker_last_year[i]
                vs_m = ((cur_val - prev_m) / prev_m * 100.0) if prev_m else (100.0 if cur_val else 0.0)
                vs_y = ((cur_val - prev_y) / prev_y * 100.0) if prev_y else (100.0 if cur_val else 0.0)
                month_target_pct = None
                year_target_pct = None

                cat1 = RPTMON_CAT1[i] if i < len(RPTMON_CAT1) else None
                if cat1:
                    month_target = get_rptmon_value(cur, rptmon_yyyymm, cat1, rptmon_cat2, 'All Card')
                    if month_target is not None and month_target > 0:
                        if i == 3:
                            val_cur = tspend[5] if len(tspend) > 5 else None
                            val_prev = tspend[4] if len(tspend) > 4 else 0
                            cur_val_month = (float(val_cur or 0) - float(val_prev or 0)) / 1000.0 if val_cur is not None else None
                            if cur_val_month is not None and cur_val_month >= 0:
                                month_target_pct = round(100.0 * cur_val_month / month_target, 2)
                            else:
                                month_target_pct = round(100.0 * cur_val / month_target, 2)
                        else:
                            month_target_pct = round(100.0 * cur_val / month_target, 2)

                    year_target = get_rptmon_ytd_sum(cur, year, 12, cat1, rptmon_cat2, 'All Card')
                    if year_target > 0:
                        if i == 0:
                            ytd_actual = (float(tnew_c[5] or 0) + float(tnew_co[5] or 0)) if len(tnew_c) > 5 and len(tnew_co) > 5 else None
                            year_target_pct = round(100.0 * ytd_actual / year_target, 2) if ytd_actual else None
                        elif i == 3:
                            ytd_actual_k = float(tspend[5] or 0) / 1000.0 if len(tspend) > 5 else 0.0
                            year_target_pct = round(100.0 * ytd_actual_k / year_target, 2)
                        else:
                            year_target_pct = round(100.0 * cur_val / year_target, 2)

                tickers['ticker_{}'.format(i + 1)] = {
                    'label': ticker_labels[i],
                    'value': cur_val,
                    'vs_last_month': vs_m,
                    'vs_last_year': vs_y,
                    'month_target_pct': month_target_pct,
                    'year_target_pct': year_target_pct,
                }
            # Snapshots for New Issuance (bang_03) and In & Out closed (bang_04)
            new_issuance_snapshot = dict(all_current.get('new_consumer', {}))
            member_closed_snapshot = dict(all_current.get('member_closed', {}))
            balance_snapshot = dict(all_current.get('balance_eop', {}))
            spend_sector_snapshot = dict(all_current.get('spend_sector', {}))
        
        total_time = time.time() - start_time
        print(f"[API] Total request processing time: {total_time:.2f}s")
        
        # Prepare response (use correct METRIC_CODE names)
        response = {
            'meta': {
                'query_date': date_param,
                'customer_type': customer_type,
                'months_label': months_label,
                'table_date_labels': table_date_labels,
                'six_dates': six_dates
            },
            'tickers': tickers,
            'kpi_cards': {
                'card_1': {
                    'value': current_data['eop_consumer'],
                    'pct_change_vs_last_date': calc_pct_change(current_data['eop_consumer'], comparison_data['last_day']['eop_consumer']),
                    'pct_change_vs_last_month': calc_pct_change(current_data['eop_consumer'], comparison_data['last_month']['eop_consumer']),
                    'pct_change_vs_last_year': calc_pct_change(current_data['eop_consumer'], comparison_data['last_year']['eop_consumer'])
                },
                'card_2': {
                    'value': current_data['using_consumer'],
                    'pct_change_vs_last_date': calc_pct_change(current_data['using_consumer'], comparison_data['last_day']['using_consumer']),
                    'pct_change_vs_last_month': calc_pct_change(current_data['using_consumer'], comparison_data['last_month']['using_consumer']),
                    'pct_change_vs_last_year': calc_pct_change(current_data['using_consumer'], comparison_data['last_year']['using_consumer'])
                },
                'card_3': {
                    'value': current_data['total_outstanding'],
                    'pct_change_vs_last_date': calc_pct_change(current_data['total_outstanding'], comparison_data['last_day']['total_outstanding']),
                    'pct_change_vs_last_month': calc_pct_change(current_data['total_outstanding'], comparison_data['last_month']['total_outstanding']),
                    'pct_change_vs_last_year': calc_pct_change(current_data['total_outstanding'], comparison_data['last_year']['total_outstanding'])
                },
                'card_4': {
                    'value': current_data['total_spending'],
                    'pct_change_vs_last_date': calc_pct_change(current_data['total_spending'], comparison_data['last_day']['total_spending']),
                    'pct_change_vs_last_month': calc_pct_change(current_data['total_spending'], comparison_data['last_month']['total_spending']),
                    'pct_change_vs_last_year': calc_pct_change(current_data['total_spending'], comparison_data['last_year']['total_spending']),
                    'abs_change_vs_last_date': current_data['total_spending'] - comparison_data['last_day']['total_spending'],
                    'abs_change_vs_last_month': current_data['total_spending'] - comparison_data['last_month']['total_spending'],
                    'abs_change_vs_last_year': current_data['total_spending'] - comparison_data['last_year']['total_spending']
                },
                'card_5': {
                    'value': current_data['new_consumer'],
                    'pct_change_vs_last_date': calc_pct_change(current_data['new_consumer'], comparison_data['last_day']['new_consumer']),
                    'pct_change_vs_last_month': calc_pct_change(current_data['new_consumer'], comparison_data['last_month']['new_consumer']),
                    'pct_change_vs_last_year': calc_pct_change(current_data['new_consumer'], comparison_data['last_year']['new_consumer'])
                },
                'card_6': {
                    'value': current_data['avg_balance'],
                    'pct_change_vs_last_date': calc_pct_change(current_data['avg_balance'], comparison_data['last_day']['avg_balance']),
                    'pct_change_vs_last_month': calc_pct_change(current_data['avg_balance'], comparison_data['last_month']['avg_balance']),
                    'pct_change_vs_last_year': calc_pct_change(current_data['avg_balance'], comparison_data['last_year']['avg_balance'])
                }
            },
            'charts': {
                'member_growth': {
                    'eop_consumer': charts_monthly['member_growth']['eop_consumer'],
                    'using_consumer': charts_monthly['member_growth']['using_consumer'],
                    'usage_rate': []
                },
                'new_closed': charts_monthly['new_closed'],
                'financial_trends': charts_monthly['financial_trends'],
                'revolving': charts_monthly['revolving'],
                'pie_snapshot': {
                    'sector': {
                        'purchasing': current_data.get('purchasing_amt', 0),
                        'installment': current_data.get('installment_amt', 0),
                        'x_border': 0,
                        'cash': current_data.get('cash_advance_amt', 0),
                        'flexi': current_data.get('flexi_amt', 0)
                    },
                    'location': {
                        'domestic': current_data.get('domestic_spending', 0),
                        'x_border': current_data.get('xborder_spending', 0)
                    },
                    'channel': {
                        'pos': current_data.get('pos_spending', 0),
                        'online': current_data.get('online_spending', 0)
                    }
                },
                'location_mom': charts_monthly['location_mom'],
                'new_issuance': new_issuance_snapshot,
                'in_out_closed': member_closed_snapshot,
                'balance_snapshot': balance_snapshot,
                'spend_sector_snapshot': spend_sector_snapshot
            },
            'risk_analysis': {
                'cards': {
                    'dpd_1': current_data['dpd_1_rate'],
                    'dpd_10': current_data['dpd_10_rate'],
                    'dpd_30': current_data['dpd_30_rate'],
                    'npl': current_data['npl_rate']
                },
                'comparisons': {
                    'dpd_1': {
                        'pct_change_vs_last_date': calc_pct_change(current_data['dpd_1_rate'], comparison_data['last_day']['dpd_1_rate']),
                        'pct_change_vs_last_month': calc_pct_change(current_data['dpd_1_rate'], comparison_data['last_month']['dpd_1_rate']),
                        'pct_change_vs_last_year': calc_pct_change(current_data['dpd_1_rate'], comparison_data['last_year']['dpd_1_rate'])
                    },
                    'dpd_10': {
                        'pct_change_vs_last_date': calc_pct_change(current_data['dpd_10_rate'], comparison_data['last_day']['dpd_10_rate']),
                        'pct_change_vs_last_month': calc_pct_change(current_data['dpd_10_rate'], comparison_data['last_month']['dpd_10_rate']),
                        'pct_change_vs_last_year': calc_pct_change(current_data['dpd_10_rate'], comparison_data['last_year']['dpd_10_rate'])
                    },
                    'dpd_30': {
                        'pct_change_vs_last_date': calc_pct_change(current_data['dpd_30_rate'], comparison_data['last_day']['dpd_30_rate']),
                        'pct_change_vs_last_month': calc_pct_change(current_data['dpd_30_rate'], comparison_data['last_month']['dpd_30_rate']),
                        'pct_change_vs_last_year': calc_pct_change(current_data['dpd_30_rate'], comparison_data['last_year']['dpd_30_rate'])
                    },
                    'npl': {
                        'pct_change_vs_last_date': calc_pct_change(current_data['npl_rate'], comparison_data['last_day']['npl_rate']),
                        'pct_change_vs_last_month': calc_pct_change(current_data['npl_rate'], comparison_data['last_month']['npl_rate']),
                        'pct_change_vs_last_year': calc_pct_change(current_data['npl_rate'], comparison_data['last_year']['npl_rate'])
                    }
                },
                'balance_breakdown': {
                    'dpd_1': float(risk_dpd.get('DPD_1P_Balance', 0) or 0),
                    'dpd_10': float(risk_dpd.get('DPD_10P_Balance', 0) or 0),
                    'dpd_30': float(risk_dpd.get('DPD_30P_Balance', 0) or 0),
                    'npl': float(risk_dpd.get('NPL_90P_Balance', 0) or 0)
                },
                'member_breakdown': {
                    'dpd_1': float(risk_dpd.get('DPD_1P_Member', 0) or 0),
                    'dpd_10': float(risk_dpd.get('DPD_10P_Member', 0) or 0),
                    'dpd_30': float(risk_dpd.get('DPD_30P_Member', 0) or 0),
                    'npl': float(risk_dpd.get('NPL_90P_Member', 0) or 0)
                },
                'trend_lines': risk_monthly['trend_lines']
            },
            'table_data': table_monthly
        }
        
        # Calculate combined metrics for each month in months_label (BEFORE closing response dict)
        prev_year = year - 1
        new_consumer_data = response['table_data']['new_consumer']
        new_corporate_data = response['table_data']['new_corporate']
        
        # Initialize arrays
        usage_rate_list = []
        attrition_rate_combined_list = []
        new_member_combined_list = []
        avg_balance_list = []
        avg_spending_list = []
        
        six_dates = response.get('meta', {}).get('six_dates', [])
        # Single connection for all 6 months (avoids 24 separate DB connections)
        with get_db_connection() as conn:
            cur = conn.cursor()
            for i, month_label in enumerate(months_label):
                try:
                    date_str = six_dates[i] if i < len(six_dates) else date_param
                    usage_rate_list.append(calculate_usage_rate_with_conn(cur, date_str))
                    attrition_rate_combined_list.append(calculate_attrition_rate_combined_with_conn(cur, date_str))
                    new_member_combined_list.append(
                        (new_consumer_data[i] if i < len(new_consumer_data) else 0) +
                        (new_corporate_data[i] if i < len(new_corporate_data) else 0)
                    )
                    avg_balance_list.append(calculate_avg_balance_per_member_with_conn(cur, date_str))
                    avg_spending_list.append(calculate_avg_spending_per_member_with_conn(cur, date_str, customer_type))
                except Exception as e:
                    print(f"[API ERROR] Error calculating metrics for month {i} ({month_label}): {e}")
                    usage_rate_list.append(0.0)
                    attrition_rate_combined_list.append(0.0)
                    new_member_combined_list.append(0)
                    avg_balance_list.append(0.0)
                    avg_spending_list.append(0.0)
        
        # Assign calculated metrics to response
        response['table_data']['usage_rate'] = usage_rate_list
        response['table_data']['attrition_rate_combined'] = attrition_rate_combined_list
        response['table_data']['new_member_combined'] = new_member_combined_list
        response['table_data']['avg_balance'] = avg_balance_list
        response['table_data']['avg_spending'] = avg_spending_list
        
        print(f"[API] Returning response with {len(response.get('charts', {}).get('member_growth', {}).get('eop_consumer', []))} data points")
        # Cache response for same (date, customer_type) to speed up repeat loads
        while len(_full_stats_cache) >= _full_stats_cache_max:
            oldest_key = min(_full_stats_cache.items(), key=lambda x: x[1][0])[0]
            del _full_stats_cache[oldest_key]
        _full_stats_cache[cache_key] = (time.time(), response)
        return jsonify(sanitize_for_json(response))
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"[API ERROR] full-stats error: {str(e)}")
        print(f"[API ERROR] Traceback: {error_trace}")
        return jsonify({'error': str(e), 'traceback': error_trace, 'context': 'full-stats'}), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat(), 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'timestamp': datetime.now().isoformat(), 'database': 'disconnected', 'error': str(e)}), 500

@app.route('/api/test-risk-data')
def test_risk_data():
    """Test endpoint to check risk_dpd table data"""
    try:
        date_param = request.args.get('date')
        if not date_param:
            date_param = get_latest_date()
        
        if not date_param:
            return jsonify({'error': 'No date available'}), 404
        
        with get_db_connection() as conn:
            cur = conn.cursor()
            table_name = get_table_name('risk_dpd')
            
            # Get all metrics for this date
            query = f'SELECT metric_code, metric_value FROM {table_name} WHERE trx_date = %s'
            cur.execute(query, (date_param,))
            rows = cur.fetchall()
            
            result = {
                'date': date_param,
                'table': table_name,
                'total_rows': len(rows),
                'metrics': {}
            }
            
            for row in rows:
                metric_code, metric_value = row
                result['metrics'][metric_code] = float(metric_value) if metric_value else 0.0
            
            # Check specific DPD rates
            result['dpd_rates'] = {
                'DPD_1P_Rate': result['metrics'].get('DPD_1P_Rate', 'NOT FOUND'),
                'DPD_10P_Rate': result['metrics'].get('DPD_10P_Rate', 'NOT FOUND'),
                'DPD_30P_Rate': result['metrics'].get('DPD_30P_Rate', 'NOT FOUND'),
                'NPL_90P_Rate': result['metrics'].get('NPL_90P_Rate', 'NOT FOUND')
            }
            
            return jsonify(sanitize_for_json(result))
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500

@app.route('/api/test-data')
def test_data():
    """Test endpoint to check if we can get data from tables"""
    try:
        test_date = request.args.get('date', '2026-01-12')
        result = {}
        
        # Test each table
        for logical_key, table_name in TABLE_MAP.items():
            try:
                data = get_table_data(table_name, test_date)
                result[logical_key] = {
                    'table': table_name,
                    'has_data': len(data) > 0,
                    'columns': list(data.keys()) if data else [],
                    'sample': dict(list(data.items())[:3]) if data else {}
                }
            except Exception as e:
                result[logical_key] = {
                    'table': table_name,
                    'error': str(e)
                }
        
        return jsonify(sanitize_for_json({
            'test_date': test_date,
            'tables': result,
            'latest_date': get_latest_date(),
            'available_dates': get_available_dates()[:10]
        }))
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500

if __name__ == '__main__':
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print("🌐 Starting Card Dashboard API Server...")
    print(f"🖥️  Local access: http://localhost:8700")
    print(f"🌍 Network access: http://{local_ip}:8700")
    print("📋 Registered routes:")
    for rule in app.url_map.iter_rules():
        print(f"  {rule.endpoint:30s} --> {rule}")
    app.run(
        debug=True,
        host='0.0.0.0',
        port=8700,
        threaded=True,
    )
