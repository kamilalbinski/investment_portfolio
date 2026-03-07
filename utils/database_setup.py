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

def get_current_portfolio_data(table_name, portfolio_id=None):
    query = f"""
    SELECT *
    FROM {table_name}
    """
    if portfolio_id is not None:
        query += f'WHERE PORTFOLIO_ID = {int(portfolio_id)}\n'

    return fetch_data_from_database(table_name, query=query)


def get_portfolio_transactions(portfolio_id=None):
    query = """
    SELECT *
    FROM V_PORTFOLIO_TRANSACTIONS
    """
    if portfolio_id is not None:
        query += f'WHERE PORTFOLIO_ID = {int(portfolio_id)}\n'
    return fetch_data_from_database('V_PORTFOLIO_TRANSACTIONS', query=query)


def get_portfolio_holdings_values(portfolio_id=None):
    query = """
    SELECT *
    FROM V_PORTFOLIO_CURRENT_VALUES
    """
    if portfolio_id is not None:
        query += f'WHERE PORTFOLIO_ID = {int(portfolio_id)}\n'
    return fetch_data_from_database('V_PORTFOLIO_CURRENT_VALUES', query=query)


def get_portfolio_aggregated_values(portfolio_id=None):
    query = """
    SELECT *
    FROM AGGREGATED_PORTFOLIO_VALUES
    """
    if portfolio_id is not None:
        query += f'WHERE PORTFOLIO_ID = {int(portfolio_id)}\n'
    return fetch_data_from_database('AGGREGATED_PORTFOLIO_VALUES', query=query)


def query_all_transactions(portfolio_id=None):
    return get_portfolio_transactions(portfolio_id=portfolio_id)


def query_all_holdings(portfolio_id=None, listed=True):
    _ = listed
    return get_portfolio_holdings_values(portfolio_id=portfolio_id)


def get_temporary_owners_list(table='ACCOUNTS'):
    """Deprecated: kept for backward compatibility."""
    _ = table
    df = get_portfolios_list(include_inactive=False)
    return df['PORTFOLIO_NAME'].tolist()


def get_portfolios_list(owner_id=None, include_inactive=False):
    """
    Returns available portfolios for the selector layer.

    Parameters
    ----------
    owner_id : int | None
        If provided, returns portfolios for this owner and global owner (OWNER_ID = 0).
    include_inactive : bool
        Include inactive portfolios when True.
    """
    query = """
    SELECT
        PORTFOLIO_ID,
        OWNER_ID,
        PORTFOLIO_CODE,
        PORTFOLIO_NAME,
        BASE_CURRENCY,
        IS_DEFAULT,
        IS_ACTIVE
    FROM PORTFOLIOS_V2
    WHERE 1=1
    """

    if not include_inactive:
        query += " AND IS_ACTIVE = 1\n"

    if owner_id is not None:
        query += f" AND OWNER_ID IN (0, {int(owner_id)})\n"

    query += " ORDER BY OWNER_ID, IS_DEFAULT DESC, PORTFOLIO_NAME"

    return fetch_data_from_database('PORTFOLIOS_V2', query=query)


def get_portfolio_over_time(portfolio_id=None):
    transactions_df = get_portfolio_transactions(portfolio_id=portfolio_id)

    if not transactions_df.empty:
        transactions_df = preprocess_transactions(transactions_df)

    portfolio_df = get_portfolio_aggregated_values(portfolio_id=portfolio_id)
    if not portfolio_df.empty:
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