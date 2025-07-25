import sqlite3

import pandas
import pandas as pd
import os
import shutil
import datetime
from glob import glob

from calculations.calculations_main import preprocess_transactions
from utils.config import DATABASE_FILE


def backup_database():
    """
    Creates a backup copy of the current database file in the same folder,
    with a filename pattern like: database_YYYYMMDD_HHMMSS.db
    """
    # Extract directory, base name, and extension from DATABASE_FILE
    directory = os.path.dirname(DATABASE_FILE)
    base_name = os.path.splitext(os.path.basename(DATABASE_FILE))[0]  # e.g., 'database'
    ext = os.path.splitext(os.path.basename(DATABASE_FILE))[1]  # e.g., '.db'

    # Build timestamp
    now_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    # Construct new filename with time suffix
    backup_filename = f"{base_name}_{now_str}{ext}"
    backup_path = os.path.join(directory, backup_filename)

    # Copy the file
    shutil.copy(DATABASE_FILE, backup_path)

    print(f"Database backup: {backup_path}")

def execute_ddl(ddl_statement):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(ddl_statement)
    conn.commit()
    conn.close()


def create_tables_from_schemas(schema_dir='ddl'):
    for file_name in os.listdir(schema_dir):
        if file_name.endswith('.sql'):
            table_name = file_name[7:-4]  # Remove the .sql extension to get the table name
            sql_file_path = os.path.join(schema_dir, file_name)
            with open(sql_file_path, 'r') as file:
                ddl_statement = file.read()
                execute_ddl(ddl_statement)
            print(f"Table {table_name} checked/created.")


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
#    query = 'SELECT NAME, ASSET_ID, MARKET FROM ASSETS'
    query = 'SELECT NAME, ASSET_ID, MARKET, INITIAL_DATE FROM ASSETS'
    df = fetch_data_from_database(table_name='ASSETS', query=query)
    df['INITIAL_DATE'] = pd.to_datetime(df['INITIAL_DATE'])
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
    JOIN ASSETS s ON p.ASSET_ID = s.ASSET_ID
    WHERE p.ASSET_ID IN ({asset_ids_str}) AND p.DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
    '''
    # Fetch data using the existing unchanged function
    return fetch_data_from_database(price_table_name, query=query)


def get_all_currency_asset_ids():
    query = 'SELECT NAME, ASSET_ID, CATEGORY FROM ASSETS WHERE CATEGORY == "FX"'
    df = fetch_data_from_database(table_name='ASSETS', query=query)
    return df

def get_current_portfolio_data(table_name, account_owner=None):
    query = f'''
    SELECT *
    FROM {table_name} 
    '''
    if account_owner:
        query += f'WHERE ACCOUNT_OWNER = "{account_owner}"\n'

    return fetch_data_from_database(table_name, query=query)

def get_daily_portfolio_data(table_name, account_owner=None):
    query = f'''
    SELECT *
    FROM {table_name} 
    '''
    if account_owner:
        query += f'WHERE ACCOUNT_OWNER = "{account_owner}"\n'
    else:
        query += f'WHERE ACCOUNT_OWNER = "None"\n'

    return fetch_data_from_database(table_name, query=query)

def query_all_transactions(account_owner=None):
    # Connect to the SQLite database
    conn = sqlite3.connect(DATABASE_FILE)

    query = '''SELECT * FROM TRANSACTIONS_ALL'''

    if account_owner:
        query += f'WHERE a.ACCOUNT_OWNER = "{account_owner}"\n'

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
            ACCOUNTS a
        LEFT JOIN 
            HOLDINGS h ON a.ACCOUNT_ID = h.ACCOUNT_ID
        LEFT JOIN 
            ASSETS s ON h.ASSET_ID = s.ASSET_ID
        LEFT JOIN 
            LATEST_PRICES p ON s.ASSET_ID = p.ASSET_ID
        LEFT JOIN (
            SELECT
                c.PRICE,
                s.CURRENCY
            FROM
                LATEST_CURRENCIES c
            JOIN
                ASSETS s ON c.ASSET_ID = s.ASSET_ID
        ) c on s.CURRENCY = c.CURRENCY
        WHERE 
            1=1
    """

    # Add condition for account_owner if provided
    if account_owner:
        query += f'AND a.ACCOUNT_OWNER = "{account_owner}"\n'

    # # Add the final part of the WHERE clause
    # query += f'AND s.MARKET {sign} 0'

    # Use Pandas to read the SQL query result into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return df


def get_temporary_owners_list(table='ACCOUNTS'):
    # Connect to your database

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Execute a query to fetch unique owner names
    cursor.execute(f"SELECT DISTINCT ACCOUNT_OWNER FROM {table}")
    owner_names = [row[0] for row in cursor.fetchall()]
    owner_names.append('All')

    conn.close()

    return owner_names


def get_portfolio_over_time(owner=None):

    # transactions_df = query_all_transactions(owner)
    transactions_df = get_current_portfolio_data(table_name='TRANSACTIONS_ALL', account_owner=owner)
    transactions_df = preprocess_transactions(transactions_df)

    portfolio_df = get_daily_portfolio_data('AGGREGATED_PORTFOLIO_VALUES', account_owner=owner)
    portfolio_df['TIMESTAMP'] = pd.to_datetime(portfolio_df['TIMESTAMP']).dt.date
    portfolio_df['AGGREGATED_VALUE'] = portfolio_df['AGGREGATED_VALUE'].astype('float64')

    return portfolio_df, transactions_df


def setup_database(db_path, ddl_tables_path='ddl/tables', ddl_views_path='ddl/view'):
    """
    Create a blank SQLite DB and run all table & view DDL scripts.
    """
    # Create DB file if it doesn't exist
    if not os.path.exists(db_path):
        print(f"Creating new database at: {db_path}")
    else:
        print(f"Database already exists at: {db_path}, continuing to apply DDLs.")

    # Connect to DB
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Execute table DDLs
    table_files = sorted(glob(os.path.join(ddl_tables_path, '*.sql')))
    for ddl_file in table_files:
        with open(ddl_file, 'r') as f:
            sql_script = f.read()
            print(f"Executing table DDL: {ddl_file}")
            cursor.executescript(sql_script)

    # Execute view DDLs
    view_files = sorted(glob(os.path.join(ddl_views_path, '*.sql')))
    for ddl_file in view_files:
        with open(ddl_file, 'r') as f:
            sql_script = f.read()
            print(f"Executing view DDL: {ddl_file}")
            cursor.executescript(sql_script)

    # Commit & close
    conn.commit()
    conn.close()
    print("Database setup complete.")

