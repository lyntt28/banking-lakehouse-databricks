from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime, timedelta, timezone
import traceback
from psycopg2 import pool
from calendar import monthrange
from decimal import Decimal
import os

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,
    dbname="dashboard_db",
    user="postgres",
    password="<PG_PASSWORD>",
    host="localhost",
    port="5428"
)

DB_PARAMS = {
    'dbname': 'dashboard_db',
    'user': 'postgres',
    'password': '<PG_PASSWORD>',
    'host': 'localhost',
    'port': '5428'
}

# Cache flag for optional product_targets table
PRODUCT_TARGETS_TABLE_EXISTS = None


def product_targets_table_exists(conn):
    """
    Check if table product_targets exists.
    Result is cached in PRODUCT_TARGETS_TABLE_EXISTS to avoid repeated lookups.
    """
    global PRODUCT_TARGETS_TABLE_EXISTS
    if PRODUCT_TARGETS_TABLE_EXISTS is not None:
        return PRODUCT_TARGETS_TABLE_EXISTS

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'product_targets'
            LIMIT 1
            """
        )
        PRODUCT_TARGETS_TABLE_EXISTS = cur.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking product_targets table: {e}")
        PRODUCT_TARGETS_TABLE_EXISTS = False

    return PRODUCT_TARGETS_TABLE_EXISTS


def get_target_value(conn, stnd_dt, region, branch, product_scope):
    """
    Get dynamic target_value from product_targets table with priority:
    1) Exact match on (stnd_dt, region, manage_branch, product_scope)
    2) (stnd_dt, region, NULL manage_branch, product_scope)
    3) (stnd_dt, NULL region, NULL manage_branch, product_scope)

    If table doesn't exist or no matching row is found, return None so
    callers can fall back to existing hard-coded targets.
    """
    if not product_targets_table_exists(conn):
        # Optional table not available -> keep old behavior
        return None

    try:
        cur = conn.cursor()

        # 1) stnd_dt + region + branch
        if region and branch:
            cur.execute(
                """
                SELECT target_value
                FROM product_targets
                WHERE stnd_dt = %s
                  AND region = %s
                  AND manage_branch = %s
                  AND product_scope = %s
                ORDER BY stnd_dt DESC
                LIMIT 1
                """,
                [stnd_dt, region, branch, product_scope],
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0])

        # 2) stnd_dt + region + NULL branch
        if region:
            cur.execute(
                """
                SELECT target_value
                FROM product_targets
                WHERE stnd_dt = %s
                  AND region = %s
                  AND manage_branch IS NULL
                  AND product_scope = %s
                ORDER BY stnd_dt DESC
                LIMIT 1
                """,
                [stnd_dt, region, product_scope],
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0])

        # 3) stnd_dt + NULL region + NULL branch
        cur.execute(
            """
            SELECT target_value
            FROM product_targets
            WHERE stnd_dt = %s
              AND region IS NULL
              AND manage_branch IS NULL
              AND product_scope = %s
            ORDER BY stnd_dt DESC
            LIMIT 1
            """,
            [stnd_dt, product_scope],
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])

        return None
    except Exception as e:
        logger.error(f"Error getting target from product_targets: {e}")
        return None

# Old region format (before 2026-01-16)
REGIONS_OLD = ['South 1', 'South 2', 'North', 'Others']
# New region format (from 2026-01-16)
REGIONS_NEW = ['South 1', 'South 2', 'South 3', 'North 1', 'North 2', 'Others']
# Default to old format for backward compatibility
REGIONS = REGIONS_OLD

# Date threshold for new region format
REGION_CHANGE_DATE = datetime(2026, 1, 16).date()

def get_regions_for_date(date_str=None):
    """
    Get region list based on date
    - Before 2026-01-16: Old format (South 1, South 2, North, Others)
    - From 2026-01-16: New format (South 1, South 2, South 3, North 1, North 2, Others)
    """
    if not date_str:
        # Default to current date
        return REGIONS_NEW
    
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        if date_obj >= REGION_CHANGE_DATE:
            return REGIONS_NEW
        else:
            return REGIONS_OLD
    except:
        # If date parsing fails, return old format for safety
        return REGIONS_OLD

def is_valid_region(region, date_str=None):
    """
    Check if region is valid for the given date
    """
    if not region:
        return False
    valid_regions = get_regions_for_date(date_str)
    return region in valid_regions

# FX Rate conversion for year 2026
FX_RATE_OLD = 25641.026
FX_RATE_NEW = 26315.789
FX_RATE_RATIO = FX_RATE_NEW / FX_RATE_OLD  # 1.0263...

# THÊM: Development mode detection
IS_DEVELOPMENT = os.environ.get('FLASK_ENV') == 'development'

# UPDATED: Date parser for DATE type database
def parse_input_date(date_str):
    """Convert YYYY-MM-DD to YYYY-MM-DD (keep same format for DATE type)"""
    try:
        # Validate format and return as-is for DATE type
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str  # Return original YYYY-MM-DD format
    except Exception as e:
        logger.error(f"Date parsing error: {e}")
        return None

# Color schemes for charts
CHART_COLORS = {
    'group_class': {
        '1-CASA': ['#B3E5FC', '#22AFFF'],   # 2024: light, 2025: dark
        '2-TIME': ['#7986CB', '#1A237E'],
        '3-ISA': ['#FFD180', '#F47B20']
    },
    'region': {
        'South 1': '#223A5E',  # Dark blue - same as ending balance 30 days dark color
        'South 2': '#1A237E',  # Darker TDA color (darker than #009688)
        'South 3': '#80CBC4',  # Light teal like TDA 2025
        'North 1': '#FFE082',  # Light yellow like CASA 2025
        'North 2': '#F4A300',  # Dark yellow like CASA 2026
        'North': '#FFE082',  # Keep for old data compatibility (light yellow)
        'Others': '#F47B20'
    },
    'term': {
        'No Tennor': '#E57373',
        '< 1M': '#81C784',
        '1M-6M': '#64B5F6',
        '6M-12M': '#FFB74D',
        '12M-24M': '#BA68C8',
        '>24M': '#4DB6AC'
    }
}

def get_db_connection():
    try:
        return psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def release_db_connection(conn):
    connection_pool.putconn(conn)

def build_filters(date_yyyy_mm_dd, region, branch, branch_field, alias=None):
    prefix = f"{alias}." if alias else ""
    q = f" WHERE {prefix}stnd_dt = %s"
    p = [date_yyyy_mm_dd]  # Use DATE format directly
    # Use is_valid_region with date to support both old and new region formats
    if region and is_valid_region(region, date_yyyy_mm_dd):
        q += f" AND {prefix}region = %s"
        p.append(region)
    if branch:
        q += f" AND {prefix}{branch_field} = %s"
        p.append(branch)
    return q, p

def validate_date(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def is_empty(row):
    return not row or all(v is None for v in row)

def handle_error(e, endpoint):
    logger.error(f"Error in {endpoint}: {str(e)}")
    logger.error(traceback.format_exc())
    return jsonify({"error": f"Internal server error in {endpoint}"}), 500

def convert_decimal(obj):
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def safe_convert_decimal(value, default=0):
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def apply_fx_rate_conversion(value, year):
    """
    Apply FX rate conversion for all years
    Multiply by FX_RATE_RATIO (26315.789/25641.026) for all balance/financial data
    Note: Customer data endpoints should NOT use this function
    """
    return value * FX_RATE_RATIO

@app.before_request
def log_request_info():
    logger.info('Headers: %s', request.headers)
    logger.info('Body: %s', request.get_data())
    logger.info('Args: %s', request.args)

def add_cache_control_headers(response):
    """Add no-cache headers in development"""
    if 'localhost' in str(request.host):
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', "*")
        response.headers.add('Access-Control-Allow-Methods', "*")
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response
app.after_request(add_cache_control_headers)

@app.route('/api/regions')
def get_regions():
    """
    Get regions list based on date parameter
    If date is provided, return regions for that date
    Otherwise, return new format (default)
    """
    date_str = request.args.get('date')
    regions = get_regions_for_date(date_str)
    return jsonify(regions)

@app.route('/api/currencies')
def get_currencies():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT currency FROM bang1247 WHERE currency IS NOT NULL ORDER BY currency")
            rows = cur.fetchall()
            currencies = [row[0] for row in rows if row[0]]
            return jsonify(currencies)
    except Exception as e:
        return handle_error(e, 'currencies')

@app.route('/api/nationalities')
def get_nationalities():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT nation FROM bang1247 WHERE nation IS NOT NULL ORDER BY nation")
            rows = cur.fetchall()
            nationalities = [row[0] for row in rows if row[0]]
            return jsonify(nationalities)
    except Exception as e:
        return handle_error(e, 'nationalities')

# ===== FIXED GAUGE ENDPOINTS =====

@app.route('/api/gauge-all')
def gauge_all():
    try:
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')

        if not validate_date(d):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)
        if not db_date:
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Gauge-all request: date={db_date}, region={r}, branch={b}")

        q, p = build_filters(db_date, r, b, 'manage_branch')

        with get_db_connection() as conn:
            cur = conn.cursor()
            all_sql = f"""
                SELECT COALESCE(SUM(diff_t_y1), 0) as yoy_change,
                       COALESCE(SUM(bal), 0) as current_balance
                FROM bang1247 {q}
            """
            cur.execute(all_sql, p)
            result = cur.fetchone()
            
            if not result:
                logger.warning("No data found for gauge-all")
                return jsonify({
                    'all_balance': 0,
                    'target': 300000000,
                    'date': d
                })
            
            all_diff_yoy = result[0] if result[0] else 0
            # all_balance = result[1] if result[1] else 0

            logger.info(f"Gauge-all result: change={all_diff_yoy}")

            d_obj = datetime.strptime(d, '%Y-%m-%d')
            year = d_obj.year

            # Apply FX conversion (same as current logic)
            all_value = apply_fx_rate_conversion(float(all_diff_yoy), year)

            # Try dynamic target; fall back to old hard-coded target if not available
            dynamic_target = get_target_value(conn, db_date, r, b, 'ALL')
            target_value = dynamic_target if dynamic_target is not None else 300000000

            return jsonify({
                'all_balance': all_value,
                'target': target_value,
                'date': d
            })
    except Exception as e:
        return handle_error(e, 'gauge-all')

@app.route('/api/gauge-data')
def gauge_data():
    try:
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')

        if not validate_date(d):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)
        if not db_date:
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Gauge-data request: date={db_date}, region={r}, branch={b}")

        q, p = build_filters(db_date, r, b, 'manage_branch')

        with get_db_connection() as conn:
            cur = conn.cursor()
            casa_sql = f"""
                SELECT COALESCE(SUM(diff_t_y1), 0) as yoy_change,
                       COALESCE(SUM(bal), 0) as current_balance
                FROM bang1247 {q} AND group_class = '1-CASA'
            """
            cur.execute(casa_sql, p)
            result = cur.fetchone()
            
            if not result:
                logger.warning("No data found for gauge-data")
                return jsonify({
                    'casa_balance': 0,
                    'target': 100000000,
                    'date': d
                })
            
            casa_diff_yoy = result[0] if result[0] else 0
            # casa_balance = result[1] if result[1] else 0

            logger.info(f"Gauge-data result: change={casa_diff_yoy}")

            d_obj = datetime.strptime(d, '%Y-%m-%d')
            year = d_obj.year

            casa_value = apply_fx_rate_conversion(float(casa_diff_yoy), year)

            # Dynamic CASA target; fallback to old constant if not configured
            dynamic_target = get_target_value(conn, db_date, r, b, 'CASA')
            target_value = dynamic_target if dynamic_target is not None else 100000000

            return jsonify({
                'casa_balance': casa_value,
                'target': target_value,
                'date': d
            })
    except Exception as e:
        return handle_error(e, 'gauge-data')

@app.route('/api/gauge-metrics')
def gauge_metrics():
    try:
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')

        if not validate_date(d):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)
        if not db_date:
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Gauge-metrics request: date={db_date}, region={r}, branch={b}")

        q, p = build_filters(db_date, r, b, 'manage_branch')

        with get_db_connection() as conn:
            cur = conn.cursor()
            tda_sql = f"""
                SELECT COALESCE(SUM(diff_t_y1), 0) as yoy_change,
                       COALESCE(SUM(bal), 0) as current_balance
                FROM bang1247 {q} AND group_class in ('2-TIME', '3-ISA')
            """
            cur.execute(tda_sql, p)
            result = cur.fetchone()
            
            if not result:
                logger.warning("No data found for gauge-metrics")
                return jsonify({
                    'tda_growth': 0,
                    'target': 100000000,
                    'date': d
                })
            
            tda_diff_yoy = result[0] if result[0] else 0
            # tda_balance = result[1] if result[1] else 0

            logger.info(f"Gauge-metrics result: change={tda_diff_yoy}")

            d_obj = datetime.strptime(d, '%Y-%m-%d')
            year = d_obj.year

            tda_value = apply_fx_rate_conversion(float(tda_diff_yoy), year)

            # Dynamic TDA/ISA target; fallback to old constant if not configured
            dynamic_target = get_target_value(conn, db_date, r, b, 'TDA_ISA')
            target_value = dynamic_target if dynamic_target is not None else 100000000

            return jsonify({
                'tda_growth': tda_value,
                'target': target_value,
                'date': d
            })
    except Exception as e:
        return handle_error(e, 'gauge-metrics')

# ===== TICKER 3 - RETAIL CUSTOMERS LOGIC =====
# Dựa trên cấu trúc bảng bang3 thực tế

@app.route('/api/bang3-retail')
def bang3_retail():
    """
    API endpoint cho ticker 3 - Retail Customers
    Sử dụng bảng bang3 với cấu trúc:
    - id, stnd_dt, region, branch, retail_customer, casa_customer, active1_casa, dormant1_casa
    """
    try:
        # Lấy parameters từ request
        d = request.args.get('date')
        r = request.args.get('region')
        b = request.args.get('branch')

        if not validate_date(d):
            logger.error(f"Bang3-retail: Invalid date format received: {d}")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)
        if not db_date:
            logger.error(f"Bang3-retail: Failed to parse date: {d}")
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Bang3-retail request: date={db_date}, region={r}, branch={b}")

        # Parse input date để tính các ngày so sánh
        d_obj = datetime.strptime(d, '%Y-%m-%d')
        
        # Tính các ngày so sánh
        last_day = d_obj - timedelta(days=1)

        # Build base filters
        base_filters = []
        base_params = []
        
        if r and is_valid_region(r, d):
            base_filters.append("region = %s")
            base_params.append(r)
        if b:
            base_filters.append("branch = %s")
            base_params.append(b)
        
        where_base = ' AND ' + ' AND '.join(base_filters) if base_filters else ''

        with get_db_connection() as conn:
            cur = conn.cursor()

            # Function để lấy retail customers cho một ngày cụ thể
            def get_retail_data_for_date(target_date):
                """Lấy tổng retail customers cho một ngày"""
                sql = f"""
                    SELECT 
                        COALESCE(SUM(retail_customer), 0) as total_retail
                    FROM bang3 
                    WHERE stnd_dt = %s
                    {where_base}
                """
                params = [target_date] + base_params
                cur.execute(sql, params)
                result = cur.fetchone()
                if result:
                    return int(result[0]) if result[0] else 0
                return 0

            # Function để tìm ngày cuối cùng của tháng trước có dữ liệu
            def find_last_month_end_date():
                if d_obj.month == 1:
                    prev_month = 12
                    prev_year = d_obj.year - 1
                else:
                    prev_month = d_obj.month - 1
                    prev_year = d_obj.year
                sql = f"""
                    SELECT MAX(stnd_dt) as last_date
                    FROM bang3 
                    WHERE EXTRACT(YEAR FROM stnd_dt) = %s 
                    AND EXTRACT(MONTH FROM stnd_dt) = %s
                    {where_base}
                """
                params = [prev_year, prev_month] + base_params
                cur.execute(sql, params)
                result = cur.fetchone()
                if result and result[0]:
                    return result[0]
                return None

            # Function để tìm ngày cuối cùng của năm trước có dữ liệu  
            def find_last_year_end_date():
                prev_year = d_obj.year - 1
                sql = f"""
                    SELECT MAX(stnd_dt) as last_date
                    FROM bang3 
                    WHERE EXTRACT(YEAR FROM stnd_dt) = %s
                    {where_base}
                """
                params = [prev_year] + base_params
                cur.execute(sql, params)
                result = cur.fetchone()
                if result and result[0]:
                    return result[0]
                return None

            # Function để tìm ngày gần nhất có data (cho last_day)
            def find_nearest_date_with_data(target_date, max_days_back=7):
                for i in range(max_days_back):
                    test_date = target_date - timedelta(days=i)
                    data = get_retail_data_for_date(test_date.date())
                    if data > 0:
                        return data, test_date.date()
                return 0, None

            # Lấy dữ liệu cho ngày hiện tại
            current_data = get_retail_data_for_date(d_obj.date())
            
            # Lấy dữ liệu cho các ngày so sánh
            last_day_data, last_day_date = find_nearest_date_with_data(last_day)
            
            # Tìm ngày cuối tháng trước có dữ liệu
            last_month_end_date = find_last_month_end_date()
            if last_month_end_date:
                last_month_data = get_retail_data_for_date(last_month_end_date)
            else:
                last_month_data = 0
            
            # Tìm ngày cuối năm trước có dữ liệu
            last_year_end_date = find_last_year_end_date()
            if last_year_end_date:
                last_year_data = get_retail_data_for_date(last_year_end_date)
            else:
                last_year_data = 0

            # Tính toán các chỉ số thay đổi
            vs_last_day = current_data - last_day_data
            vs_last_month = current_data - last_month_data
            vs_last_year = current_data - last_year_data

            logger.info(f"Bang3-retail calculation results:")
            logger.info(f"  Current date: {d_obj.date()}")
            logger.info(f"  Current retail: {current_data}")
            logger.info(f"  Last day date: {last_day_date}")
            logger.info(f"  Last month end date: {last_month_end_date}")
            logger.info(f"  Last year end date: {last_year_end_date}")
            logger.info(f"  vs Last Day: {vs_last_day}")
            logger.info(f"  vs Last Month: {vs_last_month}")
            logger.info(f"  vs Last Year: {vs_last_year}")

            # Trả về kết quả theo format mà frontend expect
            result = {
                'retail_customer': current_data,
                'vs_last_day': vs_last_day,
                'vs_last_month': vs_last_month,
                'vs_last_year': vs_last_year,
                'comparison_dates': {
                    'current_date': str(d_obj.date()),
                    'last_day_date': str(last_day_date) if last_day_date else None,
                    'last_month_end_date': str(last_month_end_date) if last_month_end_date else None,
                    'last_year_end_date': str(last_year_end_date) if last_year_end_date else None
                },
                'date': d,
                'region': r or '',
                'branch': b or '',
                'last_updated': datetime.now().isoformat()
            }
            return jsonify(result)

    except Exception as e:
        logger.error(f"Error in bang3-retail: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": f"Internal server error in bang3-retail"}), 500

@app.route('/api/account-group-table')
def product_table():
    try:
        d = request.args.get('date')
        r = request.args.get('region')
        b = request.args.get('branch')
        currency = request.args.get('currency')
        nationality = request.args.get('nationality')

        if not validate_date(d):
            logger.error(f"Account-group-table: Invalid date format received: {d}")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)

        if not db_date:
            logger.error(f"Account-group-table: Failed to parse date: {d}")
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Account-group-table request: date={db_date}, region={r}, branch={b}, currency={currency}, nationality={nationality}")

        q, p = build_filters(db_date, r, b, 'manage_branch')

        if currency:
            q += " AND currency = %s"
            p.append(currency)
            
        if nationality:
            q += " AND nation = %s"
            p.append(nationality)

        q = q if q.strip() else "WHERE TRUE"

        query = f"""
            SELECT '1-CASA', COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 1
            FROM bang1247 {q} AND group_class = '1-CASA'
            UNION ALL
            SELECT account_group, COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 2
            FROM bang1247 {q} AND group_class = '1-CASA'
            GROUP BY account_group
            UNION ALL
            SELECT '2-TIME', COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 3
            FROM bang1247 {q} AND group_class = '2-TIME'
            UNION ALL
            SELECT account_group, COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 4
            FROM bang1247 {q} AND group_class = '2-TIME'
            GROUP BY account_group
            UNION ALL
            SELECT '3-ISA', COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 5
            FROM bang1247 {q} AND group_class = '3-ISA'
            UNION ALL
            SELECT account_group, COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 6
            FROM bang1247 {q} AND group_class = '3-ISA'
            GROUP BY account_group
            UNION ALL
            SELECT 'GRAND TOTAL', COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 9
            FROM bang1247 {q}
            ORDER BY 7, 1
        """

        with get_db_connection() as conn:
            cur = conn.cursor()
            # We repeat the parameters for each UNION part where the filter applies
            cur.execute(query, p * 7)
            rows = cur.fetchall()

            # Format the account group names
            d_obj = datetime.strptime(d, '%Y-%m-%d')
            year = d_obj.year
            
            formatted_rows = []
            for r in rows:
                account_group = r[0]
                if account_group == '1-CASA':
                    display_name = '1-CASA'
                elif account_group == '2-TIME':
                    display_name = '2-TIME'
                elif account_group == '3-ISA':
                    display_name = '3-ISA'
                elif account_group == 'GRAND TOTAL':
                    display_name = 'GRAND TOTAL'
                else:
                    display_name = account_group

                formatted_rows.append({
                    'Account Group': display_name,
                    'Ending Balance': apply_fx_rate_conversion(float(r[1] or 0), year),
                    'Portion': float((r[2] or 0) / 100),
                    '+- Last Day': apply_fx_rate_conversion(float(r[3] or 0), year),
                    '+- Last Month': apply_fx_rate_conversion(float(r[4] or 0), year),
                    '+- Last Year': apply_fx_rate_conversion(float(r[5] or 0), year)
                })

            return jsonify(formatted_rows)

    except Exception as e:
        return handle_error(e, 'account-group-table')

@app.route('/api/balance-by-region')
def region_table():
    try:
        # Use date from request
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')
        product = request.args.get('product', 'ALL')

        if not validate_date(d):
            logger.error(f"Balance-by-region: Invalid date format received: {d}")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d) # DATE format for database query

        if not db_date:
            logger.error(f"Balance-by-region: Failed to parse date: {d}")
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Balance-by-region request: date={db_date}, region={r}, branch={b}, product={product}")

        q, p = build_filters(db_date, r, b, 'manage_branch')
        
        # Add product filter if specified
        if product != 'ALL':
            q += " AND group_class = %s"
            p.append(product)
        
        sql = f"""
            SELECT region, COALESCE(SUM(bal), 0) as total_bal, COALESCE(SUM(port), 0) as total_port
            FROM bang1247 {q}
            GROUP BY region
            ORDER BY
            CASE region
            WHEN 'South 1' THEN 1
            WHEN 'South 2' THEN 2
            WHEN 'South 3' THEN 3
            WHEN 'North 1' THEN 4
            WHEN 'North 2' THEN 5
            WHEN 'North' THEN 6
            WHEN 'Others' THEN 7
            ELSE 8
            END
        """
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, p)
            rows = cur.fetchall()

            if not rows:
                logger.warning("Balance-by-region: No data found for the specified date/region/branch")
                return jsonify([])  # Return empty array instead of error

            d_obj = datetime.strptime(d, '%Y-%m-%d')
            year = d_obj.year

            return jsonify([{
                'region': r[0],
                'amount': apply_fx_rate_conversion(float(r[1] or 0), year),
                'portion': float((r[2] or 0)/100),
                'color': CHART_COLORS['region'].get(r[0], '#9E9E9E')
            } for r in rows])
    except Exception as e:
        return handle_error(e, 'balance-by-region')

@app.route('/api/balance-by-term')
def term_table():
    try:
        # Use date from request
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')

        if not validate_date(d):
            logger.error(f"Balance-by-term: Invalid date format received: {d}")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d) # DATE format for database query

        if not db_date:
            logger.error(f"Balance-by-term: Failed to parse date: {d}")
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Balance-by-term request: date={db_date}, region={r}, branch={b}")

        q, p = build_filters(db_date, r, b, 'branch')
        sql = f"""
            SELECT
                tennor,
                SUM(total_bal) as total_bal,
                ROUND(SUM(total_bal * avgintrt)/NULLIF(SUM(total_bal),0), 2) as avg_intr
            FROM bang5 {q}
            GROUP BY tennor
            ORDER BY
                CASE tennor
                    WHEN 'No Tennor' THEN 1
                    WHEN '<1M' THEN 2
                    WHEN '1M-6M' THEN 3
                    WHEN '6M-12M' THEN 4
                    WHEN '12M-24M' THEN 5
                    WHEN '>24M' THEN 6
                    ELSE 7
                END
        """
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, p)
            rows = cur.fetchall()

            if not rows:
                logger.warning("Balance-by-term: No data found for the specified date/region/branch")
                return jsonify([])  # Return empty array instead of error

            d_obj = datetime.strptime(d, '%Y-%m-%d')
            year = d_obj.year

            return jsonify([{
                'tennor': r[0],
                'balance': apply_fx_rate_conversion(float(r[1]) if r[1] else 0, year),
                'avg_intr': float(r[2]) if r[2] else 0,
                'color': CHART_COLORS['term'].get(r[0], '#9E9E9E')
            } for r in rows])
    except Exception as e:
        return handle_error(e, 'balance-by-term')

@app.route('/api/bang1247-metrics')
def bang1247():
    try:
        # Use date from request
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')

        if not validate_date(d):
            logger.error(f"Bang1247-metrics: Invalid date format received: {d}")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d) # DATE format for database query

        if not db_date:
            logger.error(f"Bang1247-metrics: Failed to parse date: {d}")
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Bang1247-metrics request: date={db_date}, region={r}, branch={b}")

        q, p = build_filters(db_date, r, b, 'manage_branch')
        sql = f"SELECT SUM(bal), SUM(diff_t_t1), SUM(diff_t_m1), SUM(diff_t_y1), SUM(mon_avg_bal_t), SUM(diff_mon_avg_t_t1), SUM(diff_mon_avg_t_m1), SUM(diff_mon_avg_t_y1) FROM bang1247 {q}"

        logger.info(f"Bang1247 SQL: {sql}")
        logger.info(f"Bang1247 Params: {p}")

        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, p)
            row = cur.fetchone()

            logger.info(f"Bang1247 query result: {row}")

            if is_empty(row):
                logger.warning("Bang1247: No data found for the specified date/region/branch")
                return jsonify({
                    'current_balance': 0,
                    'vs_last_day': 0,
                    'vs_last_month': 0,
                    'vs_last_year': 0,
                    'monthly_avg_balance': 0,
                    'monthly_vs_last_day': 0,
                    'monthly_vs_last_month': 0,
                    'monthly_vs_last_year': 0,
                    'message': 'No data available for the specified date/region/branch'
                })

            # Convert values to float and keep decimal places
            d_obj = datetime.strptime(d, '%Y-%m-%d')
            year = d_obj.year
            
            current_balance = apply_fx_rate_conversion(float(row[0] or 0), year)
            vs_last_day = apply_fx_rate_conversion(float(row[1] or 0), year)
            vs_last_month = apply_fx_rate_conversion(float(row[2] or 0), year)
            vs_last_year = apply_fx_rate_conversion(float(row[3] or 0), year)
            monthly_avg_balance = apply_fx_rate_conversion(float(row[4] or 0), year)
            monthly_vs_last_day = apply_fx_rate_conversion(float(row[5] or 0), year)
            monthly_vs_last_month = apply_fx_rate_conversion(float(row[6] or 0), year)
            monthly_vs_last_year = apply_fx_rate_conversion(float(row[7] or 0), year)

            return jsonify({
                'current_balance': current_balance,
                'vs_last_day': vs_last_day,
                'vs_last_month': vs_last_month,
                'vs_last_year': vs_last_year,
                'monthly_avg_balance': monthly_avg_balance,
                'monthly_vs_last_day': monthly_vs_last_day,
                'monthly_vs_last_month': monthly_vs_last_month,
                'monthly_vs_last_year': monthly_vs_last_year
            })
    except Exception as e:
        return handle_error(e, 'bang1247-metrics')

@app.route('/api/group-class-stacked-bar')
def group_class_stacked_bar():
    try:
        # Use date from request
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')

        if not validate_date(d):
            logger.error(f"Group-class-stacked-bar: Invalid date format received: {d}")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d) # DATE format for database query

        if not db_date:
            logger.error(f"Group-class-stacked-bar: Failed to parse date: {d}")
            return jsonify({"error": "Invalid date format"}), 400

        d_obj = datetime.strptime(d, '%Y-%m-%d')
        current_year = d_obj.year  # Năm hiện tại từ date được chọn
        prev_year = current_year - 1  # Năm trước đó
        month = d_obj.month

        logger.info(f"Processing group-class-stacked-bar for date: {d} (comparing {prev_year} vs {current_year}), region: {r}, branch: {b}")

        # Build base filters for region/branch (excluding date)
        base_filters = []
        base_params = []
        if r and is_valid_region(r, d):
            base_filters.append("region = %s")
            base_params.append(r)
        if b:
            base_filters.append("manage_branch = %s")
            base_params.append(b)
        
        where_base = ' AND ' + ' AND '.join(base_filters) if base_filters else ''

        with get_db_connection() as conn:
            cur = conn.cursor()

            # Get ONLY the last day of each month in prev_year (12 months max)
            dates_prev_year_sql = f"""
                SELECT EXTRACT(MONTH FROM stnd_dt) as month_num,
                       MAX(stnd_dt) as last_date
                FROM bang1247
                WHERE EXTRACT(YEAR FROM stnd_dt) = %s
                {where_base}
                GROUP BY EXTRACT(MONTH FROM stnd_dt)
                ORDER BY EXTRACT(MONTH FROM stnd_dt)
            """
            cur.execute(dates_prev_year_sql, [prev_year] + base_params)
            dates_prev_year_result = cur.fetchall()
            dates_prev_year = [row[1] for row in dates_prev_year_result if row[1] is not None]

            logger.info(f"Found {len(dates_prev_year)} month-end dates for {prev_year}: {dates_prev_year}")

            # 1. Lấy ngày cuối tháng cho các tháng trước tháng hiện tại của năm hiện tại
            dates_current_year_sql = f"""
                SELECT EXTRACT(MONTH FROM stnd_dt) as month_num,
                       MAX(stnd_dt) as last_date
                FROM bang1247
                WHERE EXTRACT(YEAR FROM stnd_dt) = %s
                  AND EXTRACT(MONTH FROM stnd_dt) < %s
                  {where_base}
                GROUP BY EXTRACT(MONTH FROM stnd_dt)
                ORDER BY EXTRACT(MONTH FROM stnd_dt)
            """
            cur.execute(dates_current_year_sql, [current_year, month] + base_params)
            dates_current_year_result = cur.fetchall()
            dates_current_year = [row[1] for row in dates_current_year_result if row[1] is not None]

            # 2. Lấy đúng ngày user chọn cho tháng hiện tại (nếu có dữ liệu)
            current_month_sql = f"""
                SELECT stnd_dt
                FROM bang1247
                WHERE stnd_dt = %s
                  {where_base}
                LIMIT 1
            """
            input_date_obj = d_obj.date()
            cur.execute(current_month_sql, [input_date_obj] + base_params)
            current_month_row = cur.fetchone()
            if current_month_row and current_month_row[0]:
                dates_current_year.append(current_month_row[0])

            logger.info(f"Found {len(dates_current_year)} dates for {current_year} (month-end + current): {dates_current_year}")

        # Validate we have data
        if not dates_prev_year and not dates_current_year:
            logger.error(f"No valid dates found for both {prev_year} and {current_year} after filtering")
            return jsonify({"error": "No data found for the specified criteria and date range"}), 404

        # Build the main query using only the month-end dates
        sql_parts = []
        all_params = []
        
        # prev_year data part
        if dates_prev_year:
            placeholders_prev_year = ','.join(['%s'] * len(dates_prev_year))
            sql_prev_year = f"""
                SELECT TO_CHAR(stnd_dt, 'MM') as month,
                       group_class,
                       SUM(bal) as total,
                       'Y{prev_year}' as year_label
                FROM bang1247
                WHERE stnd_dt IN ({placeholders_prev_year})
                {where_base}
                GROUP BY TO_CHAR(stnd_dt, 'MM'), group_class
            """
            sql_parts.append(f"({sql_prev_year})")
            all_params.extend(dates_prev_year)
            all_params.extend(base_params)
        
        # current_year data part  
        if dates_current_year:
            placeholders_current_year = ','.join(['%s'] * len(dates_current_year))
            sql_current_year = f"""
                SELECT TO_CHAR(stnd_dt, 'MM') as month,
                       group_class,
                       SUM(bal) as total,
                       'Y{current_year}' as year_label
                FROM bang1247
                WHERE stnd_dt IN ({placeholders_current_year})
                {where_base}
                GROUP BY TO_CHAR(stnd_dt, 'MM'), group_class
            """
            sql_parts.append(f"({sql_current_year})")
            all_params.extend(dates_current_year)
            all_params.extend(base_params)
        
        # Combine with UNION ALL
        final_sql = " UNION ALL ".join(sql_parts) + " ORDER BY month, group_class, year_label"

        logger.info(f"Executing query with {len(all_params)} parameters")
        logger.debug(f"Final SQL: {final_sql}")

        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(final_sql, all_params)
            rows = cur.fetchall()

        logger.info(f"Query returned {len(rows)} rows")

        # Process the results to separate prev_year and current_year data
        data_by_month_group = {}
        
        for row in rows:
            if not row or len(row) < 4:
                continue
                
            month_str, group_class, total, year_label = row
            if not month_str or not group_class:
                continue
                
            key = (month_str, group_class)
            if key not in data_by_month_group:
                data_by_month_group[key] = {f'Y{prev_year}': 0, f'Y{current_year}': 0}
            
            data_by_month_group[key][year_label] = safe_convert_decimal(total)

        # Extract unique months and group classes
        months = sorted(list(set(key[0] for key in data_by_month_group.keys())))
        group_classes = sorted(list(set(key[1] for key in data_by_month_group.keys())))

        logger.info(f"Processed months: {months}")
        logger.info(f"Processed group classes: {group_classes}")

        # Color scheme (swapped: CASA -> ISA, ISA -> TDA, TDA -> CASA)
        colors = {
            '1-CASA': ['#FFE082', '#F4A300'],   # CASA now uses old ISA color
            '2-TIME': ['#80CBC4', '#009688'],   # TDA now uses old CASA color
            '3-ISA':  ['#90A4AE', '#223A5E'],   # ISA now uses old TDA color
            '4-NAVY': ['#90A4AE', '#2B4C6F']    # unchanged
        }
        default_colors = ['#B0BEC5', '#607D8B']  # fallback: light/dark blue-grey

        # Build datasets
        datasets = []
        for gc in group_classes:
            if gc is None:
                continue
            if str(gc).strip().lower() in ['other', 'others']:
                continue

            data_prev_year = []
            data_current_year = []
            
            for month_str in months:
                key = (month_str, gc)
                if key in data_by_month_group:
                    prev_val = data_by_month_group[key].get(f'Y{prev_year}', 0)
                    current_val = data_by_month_group[key].get(f'Y{current_year}', 0)
                    # Apply FX rate conversion for current_year >= 2026
                    data_prev_year.append(apply_fx_rate_conversion(prev_val, prev_year))
                    data_current_year.append(apply_fx_rate_conversion(current_val, current_year))
                else:
                    data_prev_year.append(0)
                    data_current_year.append(0)

            gc_colors = colors.get(gc, default_colors)

            # Add datasets for this group class
            datasets.append({
                'label': f'{gc} {prev_year}',
                'data': data_prev_year,
                'stack': str(prev_year),
                'backgroundColor': gc_colors[0]  # Light color for prev_year
            })
            datasets.append({
                'label': f'{gc} {current_year}',
                'data': data_current_year,
                'stack': str(current_year),
                'backgroundColor': gc_colors[1]  # Dark color for current_year
            })

        return jsonify({
            'labels': months,
            'datasets': datasets,
            'dates_used': {
                str(prev_year): [str(d) for d in dates_prev_year],
                str(current_year): [str(d) for d in dates_current_year]
            },
            'years': {
                'prev_year': prev_year,
                'current_year': current_year
            }
        })

    except Exception as e:
        return handle_error(e, 'group-class-stacked-bar')

@app.route('/api/casa-donut')
def casa_donut():
    try:
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')

        if not validate_date(d):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)
        if not db_date:
            return jsonify({"error": "Invalid date format"}), 400

        q, p = build_filters(db_date, r, b, 'branch')
        with get_db_connection() as conn:
            cur = conn.cursor()
            sql = f"""
                SELECT 
                    COALESCE(SUM(retail_customer), 0), 
                    COALESCE(SUM(casa_customer), 0), 
                    COALESCE(SUM(active1_casa), 0), 
                    COALESCE(SUM(dormant1_casa), 0) 
                FROM bang3 {q}
            """
            cur.execute(sql, p)
            row = cur.fetchone() or (0, 0, 0, 0)
            return jsonify({
                'retail_customer': int(row[0] or 0),
                'casa_customer': int(row[1] or 0),
                'casa_active': int(row[2] or 0),
                'casa_dormant': int(row[3] or 0)
            })
    except Exception as e:
        return handle_error(e, 'casa-donut')

@app.route('/api/daily-balance-chart')
def daily_balance_chart():
    """Daily balance chart data for the last 30 days hoặc tháng hiện tại"""
    try:
        d = request.args.get('date')
        start_date = request.args.get('start_date')
        r, b = request.args.get('region'), request.args.get('branch')
        product = request.args.get('product', 'ALL')

        if not validate_date(d):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)
        if not db_date:
            return jsonify({"error": "Invalid date format"}), 400

        # Build base filters
        q_base = []
        p_base = []
        if r and is_valid_region(r, d):
            q_base.append("region = %s")
            p_base.append(r)
        if b:
            q_base.append("manage_branch = %s")
            p_base.append(b)
        if product != 'ALL':
            q_base.append("group_class = %s")
            p_base.append(product)
        where_base = ' AND ' + ' AND '.join(q_base) if q_base else ''

        with get_db_connection() as conn:
            cur = conn.cursor()

            if start_date and validate_date(start_date):
                # Lấy dữ liệu từ start_date đến d
                daily_sql = f"""
                    SELECT stnd_dt, SUM(bal) as daily_balance
                    FROM bang1247
                    WHERE stnd_dt >= %s AND stnd_dt <= %s
                      {where_base}
                    GROUP BY stnd_dt
                    ORDER BY stnd_dt
                """
                cur.execute(daily_sql, [start_date, d] + p_base)
            else:
                # Lấy dữ liệu trong tháng của ngày d (logic cũ)
                d_obj = datetime.strptime(d, '%Y-%m-%d')
                year = d_obj.year
                month = d_obj.month
                daily_sql = f"""
                    SELECT stnd_dt, SUM(bal) as daily_balance
                    FROM bang1247
                    WHERE EXTRACT(YEAR FROM stnd_dt) = %s
                      AND EXTRACT(MONTH FROM stnd_dt) = %s
                      {where_base}
                    GROUP BY stnd_dt
                    ORDER BY stnd_dt
                """
                cur.execute(daily_sql, [year, month] + p_base)

            daily_results = cur.fetchall()

            if not daily_results:
                return jsonify({
                    'labels': [],
                    'data': [],
                    'message': 'No data available for this period'
                })

            # FIX: Trả về full date information thay vì chỉ day
            labels = []
            data = []
            
            d_obj = datetime.strptime(d, '%Y-%m-%d')
            base_year = d_obj.year
            
            for row in daily_results:
                if row[0]:  # stnd_dt exists
                    date_obj = row[0]
                    # Format: "5 May" hoặc "15 Jun" 
                    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    formatted_label = f"{date_obj.day} {month_names[date_obj.month - 1]}"
                    labels.append(formatted_label)
                    # Apply FX rate conversion for year >= 2026
                    balance_value = float(row[1]) if row[1] else 0
                    data.append(apply_fx_rate_conversion(balance_value, date_obj.year))

            # Determine period info for chart title
            if daily_results:
                first_date = daily_results[0][0]
                last_date = daily_results[-1][0]
                if first_date.month == last_date.month and first_date.year == last_date.year:
                    period_info = f"{first_date.strftime('%B %Y')}"
                else:
                    period_info = f"{first_date.strftime('%b %Y')} - {last_date.strftime('%b %Y')}"
            else:
                d_obj = datetime.strptime(d, '%Y-%m-%d')
                period_info = f"{d_obj.strftime('%B %Y')}"

            return jsonify({
                'labels': labels,  # Now contains "5 May", "6 May", "1 Jun", etc.
                'data': data,
                'period': period_info,  # "May 2025" or "May 2025 - Jun 2025"
                'product': product
            })

    except Exception as e:
        return handle_error(e, 'daily-balance-chart')

@app.route('/api/balance-by-product')
def balance_by_product():
    try:
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')
        currency = request.args.get('currency')
        nation = request.args.get('nation')

        if not validate_date(d):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)
        if not db_date:
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Balance-by-product request: date={db_date}, region={r}, branch={b}, currency={currency}, nation={nation}")

        q, p = build_filters(db_date, r, b, 'manage_branch')

        if currency:
            q += " AND currency = %s"
            p.append(currency)
            
        if nation:
            q += " AND nation = %s"
            p.append(nation)

        q = q if q.strip() else "WHERE TRUE"

        query = f"""
            SELECT '1-CASA', COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 1
            FROM bang1247 {q} AND group_class = '1-CASA'
            UNION ALL
            SELECT account_group, COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 2
            FROM bang1247 {q} AND group_class = '1-CASA'
            GROUP BY account_group
            UNION ALL
            SELECT '2-TIME', COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 3
            FROM bang1247 {q} AND group_class = '2-TIME'
            UNION ALL
            SELECT account_group, COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 4
            FROM bang1247 {q} AND group_class = '2-TIME'
            GROUP BY account_group
            UNION ALL
            SELECT '3-ISA', COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 5
            FROM bang1247 {q} AND group_class = '3-ISA'
            UNION ALL
            SELECT account_group, COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 6
            FROM bang1247 {q} AND group_class = '3-ISA'
            GROUP BY account_group
            UNION ALL
            SELECT 'GRAND TOTAL', COALESCE(SUM(bal), 0), COALESCE(SUM(port), 0), COALESCE(SUM(diff_t_t1), 0), COALESCE(SUM(diff_t_m1), 0), COALESCE(SUM(diff_t_y1), 0), 9
            FROM bang1247 {q}
            ORDER BY 7, 1
        """

        with get_db_connection() as conn:
            cur = conn.cursor()
            # We repeat the parameters for each UNION part where the filter applies
            cur.execute(query, p * 7)
            rows = cur.fetchall()

            # Format the account group names
            d_obj = datetime.strptime(d, '%Y-%m-%d')
            year = d_obj.year
            
            formatted_rows = []
            for r in rows:
                account_group = r[0]
                if account_group == '1-CASA':
                    display_name = '1-CASA'
                elif account_group == '2-TIME':
                    display_name = '2-TIME'
                elif account_group == '3-ISA':
                    display_name = '3-ISA'
                elif account_group == 'GRAND TOTAL':
                    display_name = 'GRAND TOTAL'
                else:
                    display_name = account_group

                formatted_rows.append({
                    'Account Group': display_name,
                    'Ending Balance': apply_fx_rate_conversion(float(r[1] or 0), year),
                    'Portion': float((r[2] or 0) / 100),
                    '+- Last Day': apply_fx_rate_conversion(float(r[3] or 0), year),
                    '+- Last Month': apply_fx_rate_conversion(float(r[4] or 0), year),
                    '+- Last Year': apply_fx_rate_conversion(float(r[5] or 0), year)
                })

            return jsonify(formatted_rows)

    except Exception as e:
        return handle_error(e, 'balance-by-product')

# Enhanced error handling for existing endpoints
@app.errorhandler(500)
def handle_internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({
        'error': 'Internal server error',
        'message': 'An unexpected error occurred. Please try again later.'
    }), 500

@app.errorhandler(404)
def handle_not_found(error):
    return jsonify({
        'error': 'Endpoint not found',
        'message': 'The requested API endpoint was not found.'
    }), 404

@app.errorhandler(400)
def handle_bad_request(error):
    return jsonify({
        'error': 'Bad request',
        'message': 'Invalid request parameters.'
    }), 400

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add('Access-Control-Allow-Headers', "*")
        response.headers.add('Access-Control-Allow-Methods', "*")
        return response

@app.route('/api/health')
def health_check():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected'
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'disconnected',
            'error': str(e)
        }), 500

# ===== UPDATED API ENDPOINT FOR CASA CUSTOMER (Backend) =====
# Thêm vào api_server.py

@app.route('/api/casa-customer')
def casa_customer():
    """
    API endpoint for CASA Customer Distribution (2-layer donut)
    Returns: retail_customer, casa_customer, casa_active, casa_dormant
    """
    try:
        d = request.args.get('date')
        r = request.args.get('region')
        b = request.args.get('branch')

        if not validate_date(d):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        db_date = parse_input_date(d)
        if not db_date:
            return jsonify({"error": "Invalid date format"}), 400

        logger.info(f"Casa-customer request: date={db_date}, region={r}, branch={b}")

        q, p = build_filters(db_date, r, b, 'branch')
        
        with get_db_connection() as conn:
            cur = conn.cursor()
            sql = f"""
                SELECT 
                    COALESCE(SUM(retail_customer), 0) as retail_customer,
                    COALESCE(SUM(casa_customer), 0) as casa_customer, 
                    COALESCE(SUM(active1_casa), 0) as casa_active, 
                    COALESCE(SUM(dormant1_casa), 0) as casa_dormant
                FROM bang3 {q}
            """
            cur.execute(sql, p)
            row = cur.fetchone()

            if not row:
                logger.warning("No data found for casa-customer")
                return jsonify({
                    'retail_customer': 0,
                    'casa_customer': 0,
                    'casa_active': 0,
                    'casa_dormant': 0,
                    'message': 'No data available'
                })

            retail_customer = int(row[0] or 0)
            casa_customer = int(row[1] or 0)
            casa_active = int(row[2] or 0)
            casa_dormant = int(row[3] or 0)

            # Calculate derived values
            retail_non_casa = max(0, retail_customer - casa_customer)

            return jsonify({
                'retail_customer': retail_customer,
                'casa_customer': casa_customer,
                'casa_active': casa_active,
                'casa_dormant': casa_dormant,
                'retail_non_casa': retail_non_casa,
                'date': d,
                'region': r or '',
                'branch': b or ''
            })

    except Exception as e:
        return handle_error(e, 'casa-customer')

@app.route('/api/edusave-newopen')
def edusave_newopen():
    """Edu-Save New Open Accounts by Month"""
    try:
        d = request.args.get('date')
        r, b = request.args.get('region'), request.args.get('branch')

        if not validate_date(d):
            logger.error(f"Edusave-newopen: Invalid date format received: {d}")
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        d_obj = datetime.strptime(d, '%Y-%m-%d')
        current_year = d_obj.year  # Năm hiện tại từ date được chọn
        prev_year = current_year - 1  # Năm trước đó
        month = d_obj.month
        input_date_obj = d_obj.date()

        logger.info(f"Edusave-newopen request: date={d} (comparing {prev_year} vs {current_year}), region={r}, branch={b}")

        # Build base filters (excluding date)
        q_base = []
        p_base = []
        if r and is_valid_region(r, d):
            q_base.append("region = %s")
            p_base.append(r)
        if b:
            q_base.append("branch = %s")
            p_base.append(b)
        where_base = ' AND ' + ' AND '.join(q_base) if q_base else ''

        with get_db_connection() as conn:
            cur = conn.cursor()

            # Lấy các năm trước đó (nếu có dữ liệu) - lấy từ prev_year-2 trở về trước
            labels = []
            data_values = []
            
            # Kiểm tra và lấy năm prev_year-1 (nếu có)
            year_before_prev = prev_year - 1
            cur.execute(f"""
                SELECT MAX(stnd_dt) FROM bang6 WHERE EXTRACT(YEAR FROM stnd_dt) = %s {where_base}
            """, [year_before_prev] + p_base)
            last_date_year_before_prev = cur.fetchone()
            
            if last_date_year_before_prev and last_date_year_before_prev[0]:
                cur.execute(f"""
                    SELECT COALESCE(SUM(no_ac_edusave), 0) FROM bang6 WHERE stnd_dt = %s {where_base}
                """, [last_date_year_before_prev[0]] + p_base)
                total_year_before_prev = cur.fetchone()[0] or 0
                labels.append(f'Y{year_before_prev}')
                data_values.append(int(total_year_before_prev))
                logger.info(f"Year {year_before_prev}: {total_year_before_prev} accounts (date: {last_date_year_before_prev[0]})")

            # Năm prev_year: Chỉ có 1 cột - cộng dồn theo ngày cuối cùng các tháng có trong database
            cur.execute(f"""
                SELECT EXTRACT(MONTH FROM stnd_dt) as month_num,
                       MAX(stnd_dt) as last_date
                FROM bang6
                WHERE EXTRACT(YEAR FROM stnd_dt) = %s
                  {where_base}
                GROUP BY EXTRACT(MONTH FROM stnd_dt)
                ORDER BY EXTRACT(MONTH FROM stnd_dt)
            """, [prev_year] + p_base)
            dates_prev_year_result = cur.fetchall()
            
            # Tính tổng cộng dồn cho toàn bộ năm prev_year
            total_prev_year = 0
            for row in dates_prev_year_result:
                last_date = row[1]
                
                cur.execute(f"""
                    SELECT COALESCE(SUM(no_ac_edusave), 0)
                    FROM bang6
                    WHERE stnd_dt = %s {where_base}
                """, [last_date] + p_base)
                result = cur.fetchone()
                monthly_value = int(result[0]) if result and result[0] else 0
                total_prev_year += monthly_value
                logger.info(f"Month {row[0]}/{prev_year}: {monthly_value} accounts (date: {last_date})")
            
            # Thêm prev_year vào labels và data (chỉ 1 cột duy nhất)
            labels.append(f'Y{prev_year}')
            data_values.append(total_prev_year)
            logger.info(f"Total {prev_year}: {total_prev_year} accounts")

            # Năm current_year: Tháng sau = cộng dồn của tháng trước
            # Get end-of-month dates for current_year months before current month
            dates_current_year_sql = f"""
                SELECT EXTRACT(MONTH FROM stnd_dt) as month_num,
                       MAX(stnd_dt) as last_date
                FROM bang6
                WHERE EXTRACT(YEAR FROM stnd_dt) = %s
                  AND EXTRACT(MONTH FROM stnd_dt) < %s
                  {where_base}
                GROUP BY EXTRACT(MONTH FROM stnd_dt)
                ORDER BY EXTRACT(MONTH FROM stnd_dt)
            """
            cur.execute(dates_current_year_sql, [current_year, month] + p_base)
            dates_current_year_result = cur.fetchall()
            dates_current_year = [row[1] for row in dates_current_year_result if row[1] is not None]

            # Add current month date if data exists
            current_month_sql = f"""
                SELECT stnd_dt
                FROM bang6
                WHERE stnd_dt = %s
                  {where_base}
                LIMIT 1
            """
            cur.execute(current_month_sql, [input_date_obj] + p_base)
            current_month_row = cur.fetchone()
            if current_month_row and current_month_row[0]:
                dates_current_year.append(current_month_row[0])

            logger.info(f"Found {len(dates_current_year)} dates for {current_year}: {dates_current_year}")

            # Calculate cumulative monthly data for current_year
            cumulative_current_year = 0
            monthly_data_current_year = {}
            if dates_current_year:
                for date_val in dates_current_year:
                    month_key = date_val.month
                    cur.execute(f"""
                        SELECT COALESCE(SUM(no_ac_edusave), 0)
                        FROM bang6
                        WHERE stnd_dt = %s {where_base}
                    """, [date_val] + p_base)
                    result = cur.fetchone()
                    monthly_value = int(result[0]) if result and result[0] else 0
                    cumulative_current_year += monthly_value
                    monthly_data_current_year[month_key] = cumulative_current_year
                    logger.info(f"Month {month_key}/{current_year}: {monthly_value} accounts, cumulative: {cumulative_current_year} (date: {date_val})")

            # Add current_year monthly data (cumulative)
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            
            for month_num in sorted(monthly_data_current_year.keys()):
                labels.append(month_names[month_num - 1])
                data_values.append(monthly_data_current_year[month_num])

            # Validate we have data
            if not labels or not data_values:
                logger.warning("No Edu-Save data found")
                return jsonify({
                    'labels': [],
                    'data': [],
                    'dates_used': {str(current_year): []},
                    'message': 'No data available'
                })

            # FIXED: Return correct format that frontend expects
            result = {
                'labels': labels,  # ["Y2023", "Y2024", "Jan", "Feb", ...]
                'data': data_values,  # [1000, 2000, 500, 600, ...] <- Frontend expects this field name
                'dates_used': {str(current_year): [str(d) for d in dates_current_year]},
                'years': {
                    'prev_year': prev_year,
                    'current_year': current_year
                },
                'date': d,
                'region': r or '',
                'branch': b or ''
            }
            
            logger.info(f"Edu-save API response: labels={len(labels)}, data={len(data_values)}")
            logger.info(f"Labels: {labels}")
            logger.info(f"Data: {data_values}")
            
            return jsonify(result)

    except Exception as e:
        logger.error(f"Error in edusave-newopen: {str(e)}")
        logger.error(traceback.format_exc())
        return handle_error(e, 'edusave-newopen')

if __name__ == '__main__':
    print("🚀 Starting Dashboard API Server...")
    print("🛠️ Development mode: Cache disabled")
    print("📡 Available at: http://localhost:5000")
    app.run(
        debug=True,
        host='0.0.0.0',
        port=5000,
        threaded=True
        )                                                       
