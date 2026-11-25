import psycopg2
from psycopg2 import Error

# Database connection parameters
DB_PARAMS = {
    'dbname': 'dashboard_db',
    'user': 'postgres',
    'password': '<PG_PASSWORD>',
    'host': 'localhost',
    'port': '5428'
}

def create_branch_table():
    """Create branch table and populate with data from overdue table"""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("✅ Connected to PostgreSQL successfully")
        
        # Create branch table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS branch (
            id SERIAL PRIMARY KEY,
            branch_code VARCHAR(10) UNIQUE NOT NULL,
            branch_name VARCHAR(100) NOT NULL
        );
        """
        
        cur.execute(create_table_sql)
        print("✅ Branch table created successfully")
        
        # Get unique branch codes from overdue table
        cur.execute("""
            SELECT DISTINCT mng_br 
            FROM overdue 
            WHERE mng_br IS NOT NULL 
            ORDER BY mng_br
        """)
        
        branch_codes = cur.fetchall()
        print(f"📊 Found {len(branch_codes)} unique branch codes")
        
        # Clear existing data
        cur.execute("DELETE FROM branch")
        print("🗑️  Cleared existing branch data")
        
        # Insert branch data with mapped names
        for i, (branch_code,) in enumerate(branch_codes):
            if branch_code:
                # Map branch code to branch name (Branch A, Branch B, etc.)
                branch_name = f"Branch {chr(65 + i)}"  # A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z
                
                # If we run out of letters, use Branch AA, Branch BB, etc.
                if i >= 26:
                    branch_name = f"Branch {chr(65 + (i // 26) - 1)}{chr(65 + (i % 26))}"
                
                insert_sql = """
                INSERT INTO branch (branch_code, branch_name) 
                VALUES (%s, %s)
                """
                
                cur.execute(insert_sql, (branch_code, branch_name))
                print(f"➕ Inserted: {branch_code} -> {branch_name}")
        
        # Commit the changes
        conn.commit()
        print("💾 All changes committed successfully")
        
        # Verify the data
        cur.execute("SELECT * FROM branch ORDER BY branch_code")
        rows = cur.fetchall()
        
        print("\n📋 Branch table contents:")
        print("ID | Branch Code | Branch Name")
        print("-" * 40)
        for row in rows:
            print(f"{row[0]:2d} | {row[1]:11s} | {row[2]}")
        
        print(f"\n✅ Successfully created and populated branch table with {len(rows)} records")
        
    except (Exception, Error) as error:
        print(f"❌ Error: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("🔌 Database connection closed")

def test_branch_mapping():
    """Test the branch mapping by joining with overdue table"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        print("\n🧪 Testing branch mapping...")
        
        # Test query to see branch names in overdue data
        test_sql = """
        SELECT 
            o.mng_br as original_branch_code,
            b.branch_name as mapped_branch_name,
            COUNT(*) as record_count
        FROM overdue o
        LEFT JOIN branch b ON o.mng_br = b.branch_code
        WHERE o.mng_br IS NOT NULL
        GROUP BY o.mng_br, b.branch_name
        ORDER BY o.mng_br
        LIMIT 10
        """
        
        cur.execute(test_sql)
        rows = cur.fetchall()
        
        print("📊 Sample branch mapping test:")
        print("Original Code | Mapped Name | Record Count")
        print("-" * 50)
        for row in rows:
            print(f"{row[0]:13s} | {row[1]:12s} | {row[2]:12d}")
        
    except (Exception, Error) as error:
        print(f"❌ Test error: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    print("🚀 Starting branch table creation...")
    create_branch_table()
    test_branch_mapping()
    print("\n🎉 Branch table creation completed!")



