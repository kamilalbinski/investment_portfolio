import sqlite3

import pandas as pd

from utils.config import DATABASE_FILE


def create_tables():
    conn = sqlite3.connect('portfolio.db')
    cursor = conn.cursor()

    # Create Assets table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            name TEXT,
            type TEXT,
            UNIQUE(symbol)
        )
    ''')

    # Create Transactions table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER,
            quantity INTEGER,
            price REAL,
            transaction_type TEXT,
            date TEXT,
            FOREIGN KEY (asset_id) REFERENCES Assets(id)
        )
    ''')

    # Create Portfolios table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Portfolios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
    ''')

    conn.commit()
    conn.close()


def fetch_data_from_database(table_name, query=None):
    # Connect to the SQLite database
    conn = sqlite3.connect(DATABASE_FILE)

    # Query to fetch data from the specified table
    if not query:
        query = f'SELECT * FROM {table_name}'

    # Use Pandas to read the SQL query result into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return df


def get_asset_ids_from_database():
    query = 'SELECT NAME, ASSET_ID, MARKET, PRICE_DATE FROM Assets'
    df = fetch_data_from_database(table_name='Assets', query=query)
    df['PRICE_DATE'] = pd.to_datetime(df['PRICE_DATE'])
    return df


def get_latest_prices_from_database(table='Prices'):
    query = f"""
            WITH Latest_{table} AS (
                SELECT
                    ASSET_ID,
                    DATE,
                    PRICE,
                    ROW_NUMBER() OVER(PARTITION BY ASSET_ID ORDER BY DATE DESC) AS rn
                FROM
                    {table}
            )
            SELECT
                ASSET_ID,
                DATE,
                PRICE
            FROM
                Latest_{table}
            WHERE
                rn = 1;
            """
    df = fetch_data_from_database(table_name=table, query=query)
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['PRICE'] = df['PRICE'].round(4)

    return df



def get_price_data(price_table_name, asset_ids, start_date, end_date):
    # Convert asset_ids to a string of comma-separated values
    asset_ids_str = ', '.join([str(id) for id in asset_ids])

    # Ensure dates are in 'YYYY-MM-DD' format
    start_date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')

    # Construct the SQL query with inline parameterization
    query = f'''
    SELECT p.ASSET_ID, p.DATE, p.PRICE, s.NAME, s.CURRENCY
    FROM {price_table_name} p
    JOIN Assets s ON p.ASSET_ID = s.ASSET_ID
    WHERE p.ASSET_ID IN ({asset_ids_str}) AND p.DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
    '''
    # Fetch data using the existing unchanged function
    return fetch_data_from_database(price_table_name, query=query)


def get_all_currency_asset_ids():
    query = 'SELECT NAME, ASSET_ID, CATEGORY FROM Assets WHERE CATEGORY == "FX"'
    df = fetch_data_from_database(table_name='Assets', query=query)
    return df


def query_all_transactions(account_owner=None):
    # Connect to the SQLite database
    conn = sqlite3.connect(DATABASE_FILE)

    # Dynamically build the SELECT part of the query
    select_columns = """
            a.ACCOUNT_ID, 
            a.ACCOUNT_NAME,
            t.TIMESTAMP,
            t.ACCOUNT_ID,
            t.ASSET_ID,
            y.YFINANCE_ID,
            y.PRICE_MULTIPLIER,
            t.BUY_SELL,
            t.VOLUME,
            t.PRICE,
            t.TRANSACTION_FEE,
            t.ASSET_CURRENCY,
            t.BASE_CURRENCY,
            t.FX_RATE
    """

    # Include a.ACCOUNT_OWNER in the selection if account_owner is not provided
    if not account_owner:
        select_columns = "a.ACCOUNT_OWNER, " + select_columns

    # Construct the base part of the query using the dynamically built SELECT part
    query = f"""
        SELECT 
            {select_columns}
        FROM 
            Accounts a
        LEFT JOIN 
            Transactions t ON a.ACCOUNT_ID = t.ACCOUNT_ID
        LEFT JOIN 
            Assets s ON t.ASSET_ID = s.ASSET_ID
        LEFT JOIN 
            Mapping_yfinance y ON t.ASSET_ID = y.ASSET_ID
        WHERE 
            1=1
    """

    # Add condition for account_owner if provided
    if account_owner:
        query += f'AND a.ACCOUNT_OWNER = "{account_owner}"\n'

    # Add the final part of the WHERE clause
    query += f'AND s.MARKET != 0'

    # Use Pandas to read the SQL query result into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return df


def query_all_holdings(account_owner=None, listed=True):
    # Connect to the SQLite database
    conn = sqlite3.connect(DATABASE_FILE)

    if not listed:
        sign = '='
    else:
        sign = '!='

    # Dynamically build the SELECT part of the query
    select_columns = """
            a.ACCOUNT_ID, 
            a.ACCOUNT_NAME, 
            h.ASSET_ID, 
            h.VOLUME, 
            s.NAME, 
            s.MARKET, 
            s.CATEGORY,
            s.SUB_CATEGORY,
            s.PROFILE,
            p.PRICE AS CURRENT_PRICE,
            s.CURRENCY, 
            COALESCE(c.PRICE, 1) AS FX_RATE
    """

    # Include a.ACCOUNT_OWNER in the selection if account_owner is not provided
    if not account_owner:
        select_columns = "a.ACCOUNT_OWNER, " + select_columns

    # Construct the base part of the query using the dynamically built SELECT part
    query = f"""
        SELECT 
            {select_columns}
        FROM 
            Accounts a
        LEFT JOIN 
            Holdings h ON a.ACCOUNT_ID = h.ACCOUNT_ID
        LEFT JOIN 
            Assets s ON h.ASSET_ID = s.ASSET_ID
        LEFT JOIN 
            Latest_Prices p ON s.ASSET_ID = p.ASSET_ID
        LEFT JOIN (
            SELECT
                c.PRICE,
                s.CURRENCY
            FROM
                Latest_Currencies c
            JOIN
                Assets s ON c.ASSET_ID = s.ASSET_ID
        ) c on s.CURRENCY = c.CURRENCY
        WHERE 
            1=1
    """

    # Add condition for account_owner if provided
    if account_owner:
        query += f'AND a.ACCOUNT_OWNER = "{account_owner}"\n'

    # Add the final part of the WHERE clause
    query += f'AND s.MARKET {sign} 0'

    # Use Pandas to read the SQL query result into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return df


def get_temporary_owners_list(table='Accounts'):
    # Connect to your database

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Execute a query to fetch unique owner names
    cursor.execute(f"SELECT DISTINCT ACCOUNT_OWNER FROM {table}")
    owner_names = [row[0] for row in cursor.fetchall()]
    owner_names.append('All')

    conn.close()

    return owner_names
