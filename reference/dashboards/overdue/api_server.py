from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from datetime import datetime, timedelta
from contextlib import contextmanager
import calendar

app = Flask(__name__)
CORS(app)

DB_PARAMS = {
    'dbname': 'dashboard_db',
    'user': 'postgres',
    'password': '<PG_PASSWORD>',
    'host': 'localhost',
    'port': '5428'
}

FX_RATE = 25641.026

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

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        yield conn
    finally:
        conn.close()

def handle_error(e, context=''):
    return jsonify({'error': str(e), 'context': context}), 500

def get_latest_date(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(report_dt) FROM credit_card_overdue")
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def get_target_date(conn, date_param=None):
    """Get target date from parameter or latest date"""
    if date_param:
        try:
            return datetime.strptime(date_param, '%Y-%m-%d').date()
        except ValueError:
            return get_latest_date(conn)
    return get_latest_date(conn)

def get_comparison_dates(target_date):
    """Get comparison dates for last day, month, year - using actual dates available in database"""
    # last_day: previous day available in database
    last_day = target_date - timedelta(days=1)
    
    # last_month: find the latest date available in the previous month from database
    if target_date.month == 1:
        prev_month_year = target_date.year - 1
        prev_month = 12
    else:
        prev_month_year = target_date.year
        prev_month = target_date.month - 1
    
    # Create a target for end of previous month to search backwards from
    last_day_of_prev_month = calendar.monthrange(prev_month_year, prev_month)[1]
    last_month = target_date.replace(year=prev_month_year, month=prev_month, day=last_day_of_prev_month)
    
    # last_year: find the latest date available in December of previous year
    last_year = target_date.replace(year=target_date.year-1, month=12, day=31)
    
    return last_day, last_month, last_year

def get_closest_available_date(conn, target_date):
    """Get the closest available date in database that is <= target_date"""
    cur = conn.cursor()
    cur.execute("""
        SELECT report_dt FROM credit_card_overdue 
        WHERE report_dt <= %s 
        ORDER BY report_dt DESC 
        LIMIT 1
    """, (target_date,))
    row = cur.fetchone()
    return row[0] if row else None

# ===== ENHANCED TICKER ENDPOINTS WITH COMPARISONS =====

@app.route('/ticker1')
def ticker1():
    try:
        product_type = request.args.get('product_type', 'All')
        branch = request.args.get('branch', 'All')
        date_param = request.args.get('date')
        
        print(f"📄 ticker1 called with: product_type={product_type}, branch={branch}, date={date_param}")
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            actual_date = get_closest_available_date(conn, target_date)
            
            print(f"📅 Target date: {target_date}, Actual date: {actual_date}")
            
            if not actual_date:
                print("⚠ No data available")
                return jsonify({'error': 'No data available'}), 404
            
            cur = conn.cursor()
            
            # Build base query with proper null handling
            base_query = """
                SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) / NULLIF(COALESCE(SUM(COALESCE(port_bal, 0)), 0), 0) AS overdue_ratio 
                FROM credit_card_overdue 
                WHERE report_dt = %s
            """
            params_base = [actual_date]
            
            # Add filters
            if product_type != 'All':
                base_query += " AND product_type = %s"
                params_base.append(product_type)
            
            if branch != 'All':
                base_query += " AND mng_br = %s"
                params_base.append(branch)
            
            print(f"🔍 Executing query: {base_query}")
            print(f"🔍 With params: {params_base}")
            
            # Current value
            cur.execute(base_query, params_base)
            current_ratio = cur.fetchone()[0]
            # Handle null ratio - if null, set to 0
            current_value = float(current_ratio) * 100 if current_ratio is not None else 0
            
            # Get comparison dates
            last_day, last_month, last_year = get_comparison_dates(actual_date)
            
            # Get comparison values
            comparisons = {}
            comparison_dates = {
                'last_day': get_closest_available_date(conn, last_day),
                'last_month': get_closest_available_date(conn, last_month),
                'last_year': get_closest_available_date(conn, last_year)
            }
            
            for period, comp_date in comparison_dates.items():
                if comp_date:
                    params_comp = [comp_date] + params_base[1:]  # Replace date, keep other params
                    cur.execute(base_query, params_comp)
                    comp_ratio = cur.fetchone()[0]
                    # Handle null ratio - if null, set to 0
                    comp_value = float(comp_ratio) * 100 if comp_ratio is not None else 0
                    comparisons[period] = {
                        'value': comp_value,
                        'change': current_value - comp_value,
                        'date': str(comp_date)
                    }
                else:
                    comparisons[period] = {
                        'value': 0,
                        'change': 0,
                        'date': None
                    }
            
            print(f"✅ ticker1 result: {current_value}%")
            print(f"✅ comparisons: {comparisons}")
            
            return jsonify({
                'value': current_value, 
                'date': str(actual_date), 
                'unit': '%',
                'comparisons': comparisons
            })
    except Exception as e:
        print(f"❌ Error in ticker1: {str(e)}")
        return handle_error(e, 'ticker1')

# 5. /ticker2_filtered: OVD_BAL/FX_RATE (Overdue Balance USD) - Filtered by product type and branch
@app.route('/ticker2_filtered')
def ticker2_filtered():
    try:
        product_type = request.args.get('product_type', 'All')
        branch = request.args.get('branch', 'All')
        date_param = request.args.get('date')
        
        print(f"📄 ticker2_filtered called with: product_type={product_type}, branch={branch}, date={date_param}")
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            actual_date = get_closest_available_date(conn, target_date)
            print(f"📅 Using date: {actual_date}")
            
            cur = conn.cursor()
            
            # Add proper null handling with COALESCE - no product type filtering when product_type is 'All'
            if product_type == 'All':
                if branch == 'All':
                    query = "SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd FROM credit_card_overdue WHERE report_dt = %s"
                    params = (FX_RATE, actual_date)
                else:
                    query = "SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd FROM credit_card_overdue WHERE report_dt = %s AND mng_br = %s"
                    params = (FX_RATE, actual_date, branch)
            else:
                # Legacy support for specific product types (though we don't use this anymore)
                if branch == 'All':
                    query = "SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd FROM credit_card_overdue WHERE report_dt = %s AND product_type = %s"
                    params = (FX_RATE, actual_date, product_type)
                else:
                    query = "SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd FROM credit_card_overdue WHERE report_dt = %s AND product_type = %s AND mng_br = %s"
                    params = (FX_RATE, actual_date, product_type, branch)
            
            print(f"🔍 Executing query: {query}")
            print(f"🔍 With params: {params}")
            
            cur.execute(query, params)
            result = cur.fetchone()
            
            # Better null handling and ROUND to integer
            value = round(float(result[0])) if result and result[0] is not None else 0
            
            # Get comparison dates
            last_day, last_month, last_year = get_comparison_dates(actual_date)
            
            # Get comparison values
            comparisons = {}
            comparison_dates = {
                'last_day': get_closest_available_date(conn, last_day),
                'last_month': get_closest_available_date(conn, last_month),
                'last_year': get_closest_available_date(conn, last_year)
            }
            
            for period, comp_date in comparison_dates.items():
                if comp_date:
                    if branch == 'All':
                        if product_type == 'All':
                            comp_query = "SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd FROM credit_card_overdue WHERE report_dt = %s"
                            comp_params = (FX_RATE, comp_date)
                        else:
                            comp_query = "SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd FROM credit_card_overdue WHERE report_dt = %s AND product_type = %s"
                            comp_params = (FX_RATE, comp_date, product_type)
                    else:
                        if product_type == 'All':
                            comp_query = "SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd FROM credit_card_overdue WHERE report_dt = %s AND mng_br = %s"
                            comp_params = (FX_RATE, comp_date, branch)
                        else:
                            comp_query = "SELECT COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd FROM credit_card_overdue WHERE report_dt = %s AND product_type = %s AND mng_br = %s"
                            comp_params = (FX_RATE, comp_date, product_type, branch)
                    
                    cur.execute(comp_query, comp_params)
                    comp_result = cur.fetchone()
                    # Better null handling and ROUND to integer
                    comp_value = round(float(comp_result[0])) if comp_result and comp_result[0] is not None else 0
                    comparisons[period] = {
                        'value': comp_value,
                        'change': value - comp_value,
                        'date': str(comp_date)
                    }
                else:
                    comparisons[period] = {
                        'value': 0,
                        'change': 0,
                        'date': None
                    }
            
            print(f"✅ ticker2_filtered result: {value}")
            print(f"✅ comparisons: {comparisons}")
            
            return jsonify({
                'value': value, 
                'date': str(actual_date), 
                'unit': 'USD',
                'comparisons': comparisons
            })
    except Exception as e:
        print(f"❌ Error in ticker2_filtered: {str(e)}")
        return handle_error(e, 'ticker2_filtered')

# 6. /ticker3_filtered: PORT_BAL/FX_RATE (Ending Balance USD) - Filtered by product type and branch
@app.route('/ticker3_filtered')
def ticker3_filtered():
    try:
        product_type = request.args.get('product_type', 'All')
        branch = request.args.get('branch', 'All')
        date_param = request.args.get('date')
        
        print(f"📄 ticker3_filtered called with: product_type={product_type}, branch={branch}, date={date_param}")
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            actual_date = get_closest_available_date(conn, target_date)
            print(f"📅 Using date: {actual_date}")
            
            cur = conn.cursor()
            
            # Add proper null handling with COALESCE - no product type filtering when product_type is 'All'
            if product_type == 'All':
                if branch == 'All':
                    query = "SELECT COALESCE(SUM(COALESCE(port_bal, 0)), 0)/%s AS ending_balance_usd FROM credit_card_overdue WHERE report_dt = %s"
                    params = (FX_RATE, actual_date)
                else:
                    query = "SELECT COALESCE(SUM(COALESCE(port_bal, 0)), 0)/%s AS ending_balance_usd FROM credit_card_overdue WHERE report_dt = %s AND mng_br = %s"
                    params = (FX_RATE, actual_date, branch)
            else:
                # Legacy support for specific product types (though we don't use this anymore)
                if branch == 'All':
                    query = "SELECT COALESCE(SUM(COALESCE(port_bal, 0)), 0)/%s AS ending_balance_usd FROM credit_card_overdue WHERE report_dt = %s AND product_type = %s"
                    params = (FX_RATE, actual_date, product_type)
                else:
                    query = "SELECT COALESCE(SUM(COALESCE(port_bal, 0)), 0)/%s AS ending_balance_usd FROM credit_card_overdue WHERE report_dt = %s AND product_type = %s AND mng_br = %s"
                    params = (FX_RATE, actual_date, product_type, branch)
            
            print(f"🔍 Executing query: {query}")
            print(f"🔍 With params: {params}")
            
            cur.execute(query, params)
            result = cur.fetchone()
            
            # Better null handling and ROUND to integer
            value = round(float(result[0])) if result and result[0] is not None else 0
            
            # Get comparison dates
            last_day, last_month, last_year = get_comparison_dates(actual_date)
            
            # Get comparison values
            comparisons = {}
            comparison_dates = {
                'last_day': get_closest_available_date(conn, last_day),
                'last_month': get_closest_available_date(conn, last_month),
                'last_year': get_closest_available_date(conn, last_year)
            }
            
            for period, comp_date in comparison_dates.items():
                if comp_date:
                    if branch == 'All':
                        if product_type == 'All':
                            comp_query = "SELECT COALESCE(SUM(COALESCE(port_bal, 0)), 0)/%s AS ending_balance_usd FROM credit_card_overdue WHERE report_dt = %s"
                            comp_params = (FX_RATE, comp_date)
                        else:
                            comp_query = "SELECT COALESCE(SUM(COALESCE(port_bal, 0)), 0)/%s AS ending_balance_usd FROM credit_card_overdue WHERE report_dt = %s AND product_type = %s"
                            comp_params = (FX_RATE, comp_date, product_type)
                    else:
                        if product_type == 'All':
                            comp_query = "SELECT COALESCE(SUM(COALESCE(port_bal, 0)), 0)/%s AS ending_balance_usd FROM credit_card_overdue WHERE report_dt = %s AND mng_br = %s"
                            comp_params = (FX_RATE, comp_date, branch)
                        else:
                            comp_query = "SELECT COALESCE(SUM(COALESCE(port_bal, 0)), 0)/%s AS ending_balance_usd FROM credit_card_overdue WHERE report_dt = %s AND product_type = %s AND mng_br = %s"
                            comp_params = (FX_RATE, comp_date, product_type, branch)
                    
                    cur.execute(comp_query, comp_params)
                    comp_result = cur.fetchone()
                    # Better null handling and ROUND to integer
                    comp_value = round(float(comp_result[0])) if comp_result and comp_result[0] is not None else 0
                    comparisons[period] = {
                        'value': comp_value,
                        'change': value - comp_value,
                        'date': str(comp_date)
                    }
                else:
                    comparisons[period] = {
                        'value': 0,
                        'change': 0,
                        'date': None
                    }
            
            print(f"✅ ticker3_filtered result: {value}")
            print(f"✅ comparisons: {comparisons}")
            
            return jsonify({
                'value': value, 
                'date': str(actual_date), 
                'unit': 'USD',
                'comparisons': comparisons
            })
    except Exception as e:
        print(f"❌ Error in ticker3_filtered: {str(e)}")
        return handle_error(e, 'ticker3_filtered')

# ===== CHART ENDPOINTS =====

@app.route('/bar_line_chart')
def bar_line_chart():
    return jsonify({'error': 'Deprecated. Use /bar_line_chart_optimized'}), 410

# 8. /bar_line_5days: Overdue Trend - 5 Days Chart with Product Type Filter
@app.route('/bar_line_5days')
def bar_line_5days():
    return jsonify({'error': 'Deprecated. Use /bar_line_5days_optimized'}), 410

# ===== Optimized endpoints =====

@app.route('/bar_line_chart_optimized')
def bar_line_chart_optimized():
    try:
        branch = request.args.get('branch', 'All')
        date_param = request.args.get('date')
        product_type = request.args.get('product_type', 'All')

        print(f"[MONTHLY] branch={branch}, product_type={product_type}")

        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            cur = conn.cursor()

            # Step 1: Find the most recent date for each month
            # For current month: use the closest date to target_date
            # For previous months: use the last day of that month
            if product_type == 'All':
                date_query = """
                    WITH monthly_data AS (
                        SELECT DISTINCT 
                            DATE_TRUNC('month', report_dt) AS month_start,
                            CASE 
                                WHEN DATE_TRUNC('month', report_dt) = DATE_TRUNC('month', %s::date) THEN
                                    -- Current month: get closest date to target_date
                                    (SELECT report_dt FROM credit_card_overdue 
                                     WHERE report_dt <= %s 
                                       AND DATE_TRUNC('month', report_dt) = DATE_TRUNC('month', %s::date)
                                     ORDER BY report_dt DESC 
                                     LIMIT 1)
                                ELSE
                                    -- Previous months: get last day of month
                                    MAX(report_dt)
                            END AS selected_date
                        FROM credit_card_overdue 
                        WHERE report_dt <= %s 
                        GROUP BY DATE_TRUNC('month', report_dt)
                        HAVING DATE_TRUNC('month', report_dt) >= DATE_TRUNC('month', %s::date) - INTERVAL '5 months'
                           AND DATE_TRUNC('month', report_dt) <= DATE_TRUNC('month', %s::date)
                    )
                    SELECT month_start, selected_date
                    FROM monthly_data
                    WHERE selected_date IS NOT NULL
                    ORDER BY month_start
                """
                cur.execute(date_query, [target_date, target_date, target_date, target_date, target_date, target_date])
            else:
                date_query = """
                    WITH monthly_data AS (
                        SELECT DISTINCT 
                            DATE_TRUNC('month', report_dt) AS month_start,
                            CASE 
                                WHEN DATE_TRUNC('month', report_dt) = DATE_TRUNC('month', %s::date) THEN
                                    -- Current month: get closest date to target_date
                                    (SELECT report_dt FROM credit_card_overdue 
                                     WHERE report_dt <= %s 
                                       AND product_type = %s
                                       AND DATE_TRUNC('month', report_dt) = DATE_TRUNC('month', %s::date)
                                     ORDER BY report_dt DESC 
                                     LIMIT 1)
                                ELSE
                                    -- Previous months: get last day of month
                                    MAX(report_dt)
                            END AS selected_date
                        FROM credit_card_overdue 
                        WHERE report_dt <= %s 
                          AND product_type = %s
                        GROUP BY DATE_TRUNC('month', report_dt)
                        HAVING DATE_TRUNC('month', report_dt) >= DATE_TRUNC('month', %s::date) - INTERVAL '5 months'
                           AND DATE_TRUNC('month', report_dt) <= DATE_TRUNC('month', %s::date)
                    )
                    SELECT month_start, selected_date
                    FROM monthly_data
                    WHERE selected_date IS NOT NULL
                    ORDER BY month_start
                """
                cur.execute(date_query, [target_date, target_date, product_type, target_date, target_date, product_type, target_date, target_date])

            month_data = cur.fetchall()
            if not month_data:
                return jsonify({'labels': [], 'bar_data': [], 'line_data': []})

            # Step 2: Query values for those specific dates (apply branch filter)
            month_dates = [row[1] for row in month_data]  # selected_date
            
            # Use same query structure as 5-day chart for consistency
            if product_type == 'All':
                base_query = """
                    SELECT report_dt,
                           COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd,
                           CASE 
                               WHEN COALESCE(SUM(COALESCE(port_bal, 0)), 0) > 0 
                               THEN COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) / COALESCE(SUM(COALESCE(port_bal, 0)), 1) * 100
                               ELSE 0 
                           END AS overdue_ratio
                    FROM credit_card_overdue 
                    WHERE report_dt = ANY(%s)
                      {branch_filter}
                    GROUP BY report_dt
                    ORDER BY report_dt
                """
                params = [FX_RATE, month_dates]
            else:
                base_query = """
                    SELECT report_dt,
                           COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd,
                           CASE 
                               WHEN COALESCE(SUM(COALESCE(port_bal, 0)), 0) > 0 
                               THEN COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) / COALESCE(SUM(COALESCE(port_bal, 0)), 1) * 100
                               ELSE 0 
                           END AS overdue_ratio
                    FROM credit_card_overdue 
                    WHERE report_dt = ANY(%s)
                      AND product_type = %s
                      {branch_filter}
                    GROUP BY report_dt
                    ORDER BY report_dt
                """
                params = [FX_RATE, month_dates, product_type]

            branch_filter = ""
            if branch != 'All':
                branch_filter = "AND mng_br = %s"
                params.append(branch)

            query = base_query.format(branch_filter=branch_filter)

            print(f"[MONTHLY] Executing with {len(params)} params")
            cur.execute(query, params)
            rows = cur.fetchall()

            # Step 3: Fill missing dates with zeros and create result
            result_map = {row[0]: {'bar': float(row[1]) if row[1] else 0,
                                    'line': float(row[2]) if row[2] else 0} for row in rows}

            # month_data is already ordered from oldest to newest by the query ORDER BY month_start

            labels = []
            bar_data = []
            line_data = []
            for month_start, selected_date in month_data:
                labels.append(month_start.strftime('%b'))  # Remove -25 suffix
                if selected_date in result_map:
                    bar_data.append(result_map[selected_date]['bar'])
                    line_data.append(result_map[selected_date]['line'])
                else:
                    bar_data.append(0)
                    line_data.append(0)

            # DEBUG LOGGING
            print("[bar_line_chart_optimized] month_data:", [(m[0].strftime('%Y-%m'), m[1].strftime('%Y-%m-%d')) for m in month_data])
            print("[bar_line_chart_optimized] labels:", labels)
            print("[bar_line_chart_optimized] bar_data:", bar_data)
            print("[bar_line_chart_optimized] line_data:", line_data)

            return jsonify({
                'labels': labels,
                'bar_data': bar_data,
                'line_data': line_data
            })
    except Exception as e:
        print(f"[MONTHLY ERROR] {str(e)}")
        return handle_error(e, 'bar_line_chart_optimized')


@app.route('/bar_line_5days_optimized')
def bar_line_5days_optimized():
    try:
        branch = request.args.get('branch', 'All')
        date_param = request.args.get('date')
        product_type = request.args.get('product_type', 'All')
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            cur = conn.cursor()

            # Step 1: find 5 most recent dates with data (no branch filter)
            if product_type == 'All':
                date_query = """
                    SELECT DISTINCT report_dt 
                    FROM credit_card_overdue 
                    WHERE report_dt <= %s 
                    ORDER BY report_dt DESC 
                    LIMIT 5
                """
                cur.execute(date_query, [target_date])
            else:
                date_query = """
                    SELECT DISTINCT report_dt 
                    FROM credit_card_overdue 
                    WHERE report_dt <= %s AND product_type = %s
                    ORDER BY report_dt DESC 
                    LIMIT 5
                """
                cur.execute(date_query, [target_date, product_type])

            available_dates = [row[0] for row in cur.fetchall()]
            if not available_dates:
                return jsonify({'labels': [], 'bar_data': [], 'line_data': []})

            # Step 2: query values for those dates (apply branch filter)
            if product_type == 'All':
                base_query = """
                    SELECT report_dt,
                           COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd,
                           CASE 
                               WHEN COALESCE(SUM(COALESCE(port_bal, 0)), 0) > 0 
                               THEN COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) / COALESCE(SUM(COALESCE(port_bal, 0)), 1) * 100
                               ELSE 0 
                           END AS overdue_ratio
                    FROM credit_card_overdue 
                    WHERE report_dt = ANY(%s)
                      {branch_filter}
                    GROUP BY report_dt
                    ORDER BY report_dt
                """
                params = [FX_RATE, available_dates]
            else:
                base_query = """
                    SELECT report_dt,
                           COALESCE(SUM(COALESCE(ovd_bal, 0)), 0)/%s AS overdue_usd,
                           CASE 
                               WHEN COALESCE(SUM(COALESCE(port_bal, 0)), 0) > 0 
                               THEN COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) / COALESCE(SUM(COALESCE(port_bal, 0)), 1) * 100
                               ELSE 0 
                           END AS overdue_ratio
                    FROM credit_card_overdue 
                    WHERE report_dt = ANY(%s)
                      AND product_type = %s
                      {branch_filter}
                    GROUP BY report_dt
                    ORDER BY report_dt
                """
                params = [FX_RATE, available_dates, product_type]

            branch_filter = ""
            if branch != 'All':
                branch_filter = "AND mng_br = %s"
                params.append(branch)

            query = base_query.format(branch_filter=branch_filter)
            cur.execute(query, params)
            rows = cur.fetchall()

            # Step 3: fill missing dates with zeros
            result_map = {row[0]: {'bar': float(row[1]) if row[1] else 0,
                                    'line': float(row[2]) if row[2] else 0} for row in rows}

            # Reverse the order to show oldest to newest (ascending order)
            available_dates.reverse()

            labels = []
            bar_data = []
            line_data = []
            for d in available_dates:
                labels.append(d.strftime('%m-%d'))
                if d in result_map:
                    bar_data.append(result_map[d]['bar'])
                    line_data.append(result_map[d]['line'])
                else:
                    bar_data.append(0)
                    line_data.append(0)

            # DEBUG LOGGING: print result summary to terminal
            print("[bar_line_5days_optimized] rows:", len(rows))
            print("[bar_line_5days_optimized] available_dates:", [d.strftime('%Y-%m-%d') for d in available_dates])
            print("[bar_line_5days_optimized] labels:", labels)
            print("[bar_line_5days_optimized] bar_data:", bar_data)
            print("[bar_line_5days_optimized] line_data:", line_data)

            return jsonify({'labels': labels, 'bar_data': bar_data, 'line_data': line_data})
    except Exception as e:
        return handle_error(e, 'bar_line_5days_optimized')

# 11. /overdue_donut: Overdue Amount by Product Groups
@app.route('/overdue_donut')
def overdue_donut():
    try:
        date_param = request.args.get('date')
        branch_param = request.args.get('branch', 'All')
        product_type_param = request.args.get('product_type', 'All')
        
        print(f"🔍 overdue_donut called with: date={date_param}, branch={branch_param}, product_type={product_type_param}")
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            actual_date = get_closest_available_date(conn, target_date)
            
            cur = conn.cursor()
            
            # Build base query with proper null handling
            base_query = """
                SELECT product_group, COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) AS overdue_amount
                FROM credit_card_overdue
                WHERE report_dt = %s
            """
            
            params = [actual_date]
            
            if branch_param and branch_param != 'All':
                base_query += " AND mng_br = %s"
                params.append(branch_param)
            
            # No product type filtering - All means no filtering
            
            base_query += " GROUP BY product_group ORDER BY overdue_amount DESC"
            
            print(f"🔍 Executing donut query: {base_query}")
            print(f"🔍 With params: {params}")
            
            cur.execute(base_query, params)
            
            rows = cur.fetchall()
            
            labels = [row[0] for row in rows]
            # Ensure proper null handling and conversion
            data = [float(row[1]) / FX_RATE if row[1] is not None else 0 for row in rows]
            
            return jsonify({
                'labels': labels,
                'data': data
            })
    except Exception as e:
        return handle_error(e, 'overdue_donut')

# 12. /overdue_vertical_bar: Overdue Ratio by Product Groups
@app.route('/overdue_vertical_bar')
def overdue_vertical_bar():
    try:
        date_param = request.args.get('date')
        branch_param = request.args.get('branch', 'All')
        product_type_param = request.args.get('product_type', 'All')
        
        print(f"🔍 overdue_vertical_bar called with: date={date_param}, branch={branch_param}, product_type={product_type_param}")
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            actual_date = get_closest_available_date(conn, target_date)
            
            cur = conn.cursor()
            
            # Build base query to calculate overdue ratio by product groups
            # Formula: SUM(ovd_bal) / SUM(port_bal) * 100 for each product group
            base_query = """
                SELECT product_group, 
                       COALESCE(
                           COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) / NULLIF(COALESCE(SUM(COALESCE(port_bal, 0)), 0), 0),
                           0
                       ) AS overdue_ratio
                FROM credit_card_overdue
                WHERE report_dt = %s
            """
            
            params = [actual_date]
            
            if branch_param and branch_param != 'All':
                base_query += " AND mng_br = %s"
                params.append(branch_param)
            
            # No product type filtering - All means no filtering
            
            base_query += " GROUP BY product_group ORDER BY overdue_ratio DESC LIMIT 5"
            
            print(f"🔍 Executing vertical bar query: {base_query}")
            print(f"🔍 With params: {params}")
            
            cur.execute(base_query, params)
            
            rows = cur.fetchall()
            
            labels = [row[0] for row in rows]
            # Convert ratio to percentage (multiply by 100)
            data = [float(row[1]) * 100 if row[1] is not None else 0 for row in rows]
            
            return jsonify({
                'labels': labels,
                'data': data
            })
    except Exception as e:
        return handle_error(e, 'overdue_vertical_bar')

# 13. /overdue_treemap: Overdue Ratio by Branches - Top5
@app.route('/overdue_treemap')
def overdue_treemap():
    try:
        date_param = request.args.get('date')
        branch_param = request.args.get('branch', 'All')
        product_type_param = request.args.get('product_type', 'All')
        
        print(f"🔍 overdue_treemap called with: date={date_param}, branch={branch_param}, product_type={product_type_param}")
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            actual_date = get_closest_available_date(conn, target_date)
            
            cur = conn.cursor()
            
            # Build base query with overdue amount and ratio; then filter amount > 10,000 USD equivalent
            # We compute both SUM(ovd_bal) and SUM(port_bal) to allow filtering on absolute overdue amount
            base_query = """
                SELECT mng_br,
                       COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) AS overdue_amount,
                       COALESCE(SUM(COALESCE(port_bal, 0)), 0) AS total_balance,
                       COALESCE(
                           COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) / NULLIF(COALESCE(SUM(COALESCE(port_bal, 0)), 0), 0),
                           0
                       ) AS overdue_ratio
                FROM credit_card_overdue
                WHERE mng_br IS NOT NULL AND report_dt = %s AND mng_br NOT IN ('8125', '8125.0')
            """
            
            params = [actual_date]
            
            if branch_param and branch_param != 'All':
                base_query += " AND mng_br = %s"
                params.append(branch_param)
            
            # No product type filtering - All means no filtering
            
            base_query += " GROUP BY mng_br"
            
            print(f"🔍 Executing treemap query: {base_query}")
            print(f"🔍 With params: {params}")
            
            cur.execute(base_query, params)

            rows = cur.fetchall()

            # Filter branches where overdue_amount (converted to USD) > 10,000 before ranking
            # Note: FX_RATE assumed available globally as in other endpoints
            filtered = []
            for row in rows:
                mng_br = row[0]
                overdue_amount_vnd = float(row[1]) if row[1] is not None else 0.0
                total_balance_vnd = float(row[2]) if row[2] is not None else 0.0
                ratio = float(row[3]) if row[3] is not None else 0.0

                try:
                    overdue_amount_usd = overdue_amount_vnd / FX_RATE
                except Exception:
                    overdue_amount_usd = 0.0

                if overdue_amount_usd > 10000:  # Only include if overdue amount > 10,000 USD
                    filtered.append((mng_br, ratio))

            # Sort by ratio descending and take top 5 (or 6 to be safe if UI expects 5)
            filtered.sort(key=lambda x: x[1], reverse=True)
            top = filtered[:5]

            data = []
            for mng_br, ratio in top:
                data.append({
                    'key': mng_br,
                    'value': ratio * 100,  # Convert to percentage for frontend
                    'color': '#3498DB'
                })
            
            print(f"✅ Treemap data: {data}")
            
            return jsonify(data)
    except Exception as e:
        return handle_error(e, 'overdue_treemap')

# 13.5. /overdue_branches: Overdue Ratio by Branches - Top5 (NEW ENDPOINT)
@app.route('/overdue_branches')
def overdue_branches():
    try:
        date_param = request.args.get('date')
        branch_param = request.args.get('branch', 'All')
        product_type_param = request.args.get('product_type', 'All')
        
        print(f"🔍 overdue_branches called with: date={date_param}, branch={branch_param}, product_type={product_type_param}")
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            actual_date = get_closest_available_date(conn, target_date)
            
            cur = conn.cursor()
            
            # NEW: Calculate overdue ratio using sum(ovd_bal)/sum(port_bal) formula
            base_query = """
                SELECT sal_br, 
                       COALESCE(
                           COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) / NULLIF(COALESCE(SUM(COALESCE(port_bal, 0)), 0), 0),
                           0
                       ) AS overdue_ratio
                FROM credit_card_overdue
                WHERE sal_br IS NOT NULL AND report_dt = %s
            """
            
            params = [actual_date]
            
            if branch_param and branch_param != 'All':
                base_query += " AND sal_br = %s"
                params.append(branch_param)
            
            # No product type filtering - All means no filtering
            
            base_query += " GROUP BY sal_br ORDER BY overdue_ratio DESC LIMIT 5"
            
            print(f"🔍 Executing branches query: {base_query}")
            print(f"🔍 With params: {params}")
            
            cur.execute(base_query, params)
            
            rows = cur.fetchall()
            
            # Format data for the branches chart
            labels = [row[0] for row in rows]
            data_values = [float(row[1]) * 100 if row[1] is not None else 0 for row in rows]  # Convert to percentage
            
            result = {
                'labels': labels,
                'data': data_values
            }
            
            print(f"✅ Branches data: {result}")
            
            return jsonify(result)
    except Exception as e:
        return handle_error(e, 'overdue_branches')

# 14. /overdue_stackbar: Overdue Amount by Debt Groups
@app.route('/overdue_stackbar')
def overdue_stackbar():
    try:
        date_param = request.args.get('date')
        branch_param = request.args.get('branch', 'All')
        product_type_param = request.args.get('product_type', 'All')
        
        print(f"🔍 overdue_stackbar called with: date={date_param}, branch={branch_param}, product_type={product_type_param}")
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            
            cur = conn.cursor()
            
            # FIXED: Simplified stack bar query to avoid complex month-end logic
            base_query = """
                WITH monthly_data AS (
                    SELECT 
                        DATE_TRUNC('month', report_dt) AS month_start,
                        del_grp,
                        MAX(report_dt) as latest_date_in_month
                    FROM credit_card_overdue
                    WHERE DATE_TRUNC('month', report_dt) >= '2025-01-01'::date
                      AND DATE_TRUNC('month', report_dt) <= DATE_TRUNC('month', %s::date)
                      {branch_filter}
                      {product_filter}
                    GROUP BY DATE_TRUNC('month', report_dt), del_grp
                )
                SELECT 
                    md.month_start,
                    md.del_grp,
                    COALESCE(SUM(COALESCE(o.ovd_bal, 0)), 0) AS overdue_amount
                FROM monthly_data md
                JOIN credit_card_overdue o ON o.report_dt = md.latest_date_in_month 
                               AND o.del_grp = md.del_grp
                WHERE 1=1
                  {final_branch_filter}
                  {final_product_filter}
                GROUP BY md.month_start, md.del_grp
                ORDER BY md.month_start, md.del_grp
            """
            
            # Build filters
            branch_filter = ""
            product_filter = ""
            final_branch_filter = ""
            final_product_filter = ""
            params = [target_date]
            
            if branch_param and branch_param != 'All':
                branch_filter = "AND mng_br = %s"
                final_branch_filter = "AND o.mng_br = %s"
                params.extend([branch_param, branch_param])
            
            # No product type filtering - All means no filtering
            
            query = base_query.format(
                branch_filter=branch_filter,
                product_filter=product_filter,
                final_branch_filter=final_branch_filter,
                final_product_filter=final_product_filter
            )
            
            print(f"🔍 Executing stackbar query: {query}")
            print(f"🔍 With params: {params}")
            
            cur.execute(query, params)
            
            rows = cur.fetchall()

            # Organize data by del_grp
            data_by_group = {}
            labels = []
        
            for row in rows:
                month = row[0].strftime('%b')
                del_grp = row[1]
                # Ensure proper null handling
                amount = float(row[2]) / FX_RATE if row[2] is not None else 0
                
                if month not in labels:
                    labels.append(month)
                
                if del_grp not in data_by_group:
                    data_by_group[del_grp] = {}
                
                data_by_group[del_grp][month] = amount
            
            # Convert to format expected by frontend
            datasets = []
            colors = ['#00D4FF', '#FF6B35', '#9B59B6', '#F1C40F', '#E74C3C', '#00C851', '#FF8E53', '#5DADE2']
            
            # Define order for debt groups (excluding "0")
            debt_group_order = ['1-30', '31-90', '91-180', '181+']
            
            # Filter out "0" and sort by defined order
            ordered_groups = []
            for group in debt_group_order:
                if group in data_by_group:
                    ordered_groups.append(group)
            
            for i, group in enumerate(ordered_groups):
                data = data_by_group[group]
                dataset_data = [data.get(label, 0) for label in labels]
                datasets.append({
                    'label': group,
                    'data': dataset_data,
                    'backgroundColor': colors[i % len(colors)]
                })

            return jsonify({
                'labels': labels,
                'datasets': datasets
            })
    except Exception as e:
        return handle_error(e, 'overdue_stackbar')

# ===== UTILITY ENDPOINTS =====

@app.route('/available-dates')
def available_dates():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            # Include all dates including 31st
            cur.execute("SELECT DISTINCT report_dt FROM credit_card_overdue ORDER BY report_dt DESC")
            rows = cur.fetchall()
            dates = [row[0].strftime('%Y-%m-%d') for row in rows]
            print(f"📅 Available dates count: {len(dates)}")
            return jsonify(dates)
    except Exception as e:
        return handle_error(e, 'available_dates')

@app.route('/available-branches')
def available_branches():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT mng_br FROM credit_card_overdue WHERE mng_br IS NOT NULL ORDER BY mng_br")
            rows = cur.fetchall()
            # Handle both integer and decimal branch codes
            branches = []
            for row in rows:
                branch_code = row[0]
                # Convert to string, remove trailing .0 if present
                if isinstance(branch_code, float) and branch_code.is_integer():
                    branch_code = str(int(branch_code))
                else:
                    branch_code = str(branch_code)
                branches.append(branch_code)
            return jsonify(branches)
    except Exception as e:
        return handle_error(e, 'available_branches')

@app.route('/branch-mapping')
def branch_mapping():
    """Get branch code to branch name mapping"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT branch_code, branch_name FROM branch ORDER BY branch_code")
            rows = cur.fetchall()
            
            # Convert to dictionary format for easy lookup, handle decimal codes
            mapping = {}
            for row in rows:
                branch_code = row[0]
                branch_name = row[1]
                # Convert to string, remove trailing .0 if present
                if isinstance(branch_code, float) and branch_code.is_integer():
                    branch_code = str(int(branch_code))
                else:
                    branch_code = str(branch_code)
                mapping[branch_code] = branch_name
            
            print(f"✅ Branch mapping loaded: {len(mapping)} branches")
            return jsonify(mapping)
    except Exception as e:
        print(f"❌ Error loading branch mapping: {e}")
        return handle_error(e, 'branch_mapping')

@app.route('/health')
def health():
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat(), 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'timestamp': datetime.now().isoformat(), 'database': 'disconnected', 'error': str(e)}), 500

# ===== DEBUG ENDPOINT =====
@app.route('/debug/debt-groups')
def debug_debt_groups():
    """Debug endpoint to check debt groups in database"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT del_grp FROM credit_card_overdue WHERE del_grp IS NOT NULL ORDER BY del_grp")
            rows = cur.fetchall()
            debt_groups = [row[0] for row in rows]
            
            # Check sample data for each group
            sample_data = {}
            for del_grp in debt_groups:
                cur.execute("SELECT COUNT(*) FROM credit_card_overdue WHERE del_grp = %s AND report_dt = %s", (del_grp, '2025-10-13'))
                count = cur.fetchone()[0]
                sample_data[del_grp] = count
            
            return jsonify({
                'available_debt_groups': debt_groups,
                'sample_counts': sample_data,
                'expected_order': ['1-30', '31-90', '91-180', '181+']
            })
    except Exception as e:
        return handle_error(e, 'debug_debt_groups')

@app.route('/debug/date-filter')
def debug_date_filter():
    """Debug endpoint to test date filtering"""
    try:
        date_param = request.args.get('date')
        product_type = request.args.get('product_type', 'All')
        
        with get_db_connection() as conn:
            target_date = get_target_date(conn, date_param)
            cur = conn.cursor()
            
            # Test query with proper null handling
            cur.execute("""
                SELECT report_dt, product_type, COUNT(*) as record_count, 
                       COALESCE(SUM(COALESCE(ovd_bal, 0)), 0) as total_ovd_bal, 
                       COALESCE(SUM(COALESCE(port_bal, 0)), 0) as total_port_bal
                FROM credit_card_overdue 
                WHERE report_dt <= %s AND product_type = %s
                GROUP BY report_dt, product_type
                ORDER BY report_dt DESC
                LIMIT 10
            """, (target_date, product_type))
            
            rows = cur.fetchall()
            
            result = {
                'target_date': str(target_date),
                'product_type': product_type,
                'date_param': date_param,
                'data': []
            }
            
            for row in rows:
                result['data'].append({
                    'report_dt': str(row[0]),
                    'product_type': row[1],
                    'record_count': row[2],
                    'total_ovd_bal': float(row[3]) if row[3] is not None else 0,
                    'total_port_bal': float(row[4]) if row[4] is not None else 0
                })
            
            return jsonify(result)
    except Exception as e:
        return handle_error(e, 'debug_date_filter')

if __name__ == '__main__':
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print("🌐 Starting Dashboard API Server...")
    print("⚡ Development mode: Cache disabled")
    print(f"🖥️ Local access: http://localhost:8211")
    print(f"🌐 Network access: http://{local_ip}:8211")
    print("💡 To access from other machines, use the network IP address above")
    print("Register routes:")
    for rule in app.url_map.iter_rules():
        print(f"{rule.endpoint:30s} --> {rule}")
    app.run(
        debug=True,
        host='0.0.0.0',
        port=8211,
        threaded=True
    )
