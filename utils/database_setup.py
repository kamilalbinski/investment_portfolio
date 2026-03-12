import sqlite3

import pandas as pd
import os
import shutil
import datetime
from glob import glob

from calculations.calculations_main import preprocess_transactions
from utils.config import DATABASE_FILE


def backup_database():
    directory = os.path.dirname(DATABASE_FILE)
    base_name = os.path.splitext(os.path.basename(DATABASE_FILE))[0]
    ext = os.path.splitext(os.path.basename(DATABASE_FILE))[1]
    now_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f"{base_name}_{now_str}{ext}"
    backup_path = os.path.join(directory, backup_filename)
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
            table_name = file_name[7:-4]
            sql_file_path = os.path.join(schema_dir, file_name)
            with open(sql_file_path, 'r') as file:
                ddl_statement = file.read()
                execute_ddl(ddl_statement)
            print(f"Table {table_name} checked/created.")


def fetch_data_from_database(table_name, query=None):
    conn = sqlite3.connect(DATABASE_FILE)
    if not query:
        query = f'SELECT * FROM {table_name}'
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def get_asset_ids_from_database():
    query = 'SELECT NAME, ASSET_ID, MARKET, INITIAL_DATE FROM ASSETS'
    df = fetch_data_from_database(table_name='ASSETS', query=query)
    df['INITIAL_DATE'] = pd.to_datetime(df['INITIAL_DATE'])
    return df


def get_price_data(price_table_name, asset_ids, start_date, end_date):
    asset_ids_str = ', '.join([str(id) for id in asset_ids])
    start_date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
    query = f'''
    SELECT p.ASSET_ID, p.DATE, p.PRICE, s.NAME, s.CURRENCY
    FROM {price_table_name} p
    JOIN ASSETS s ON p.ASSET_ID = s.ASSET_ID
    WHERE p.ASSET_ID IN ({asset_ids_str}) AND p.DATE BETWEEN '{start_date_str}' AND '{end_date_str}'
    '''
    return fetch_data_from_database(price_table_name, query=query)


def get_all_currency_asset_ids():
    query = 'SELECT NAME, ASSET_ID, CATEGORY FROM ASSETS WHERE CATEGORY == "FX"'
    return fetch_data_from_database(table_name='ASSETS', query=query)


def get_current_portfolio_data(table_name, portfolio_id=None):
    query = f'SELECT * FROM {table_name}\n'
    if portfolio_id is not None:
        query += f'WHERE PORTFOLIO_ID = {int(portfolio_id)}\n'
    return fetch_data_from_database(table_name, query=query)


def get_daily_portfolio_data(table_name, portfolio_id=None):
    return get_current_portfolio_data(table_name=table_name, portfolio_id=portfolio_id)


def get_temporary_portfolios_list():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT PORTFOLIO_ID FROM PORTFOLIOS ORDER BY IS_ALL_HOLDINGS DESC, PORTFOLIO_ID')
    portfolio_ids = [str(row[0]) for row in cursor.fetchall()]
    conn.close()
    return portfolio_ids


def get_portfolio_selector_items():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT PORTFOLIO_ID, NAME FROM PORTFOLIOS ORDER BY IS_ALL_HOLDINGS DESC, NAME, PORTFOLIO_ID'
    )
    rows = cursor.fetchall()
    conn.close()

    labels = []
    label_to_id = {}
    seen_names = {}

    for portfolio_id, portfolio_name in rows:
        seen_names[portfolio_name] = seen_names.get(portfolio_name, 0) + 1
        if seen_names[portfolio_name] > 1:
            label = f"{portfolio_name} [{portfolio_id}]"
        else:
            label = portfolio_name

        labels.append(label)
        label_to_id[label] = str(portfolio_id)

    return labels, label_to_id


def get_portfolio_over_time(portfolio_id=None):
    transactions_df = get_current_portfolio_data(table_name='TRANSACTIONS_ALL', portfolio_id=portfolio_id)
    transactions_df = preprocess_transactions(transactions_df)

    portfolio_df = get_daily_portfolio_data('AGGREGATED_PORTFOLIO_VALUES', portfolio_id=portfolio_id)
    portfolio_df['TIMESTAMP'] = pd.to_datetime(portfolio_df['TIMESTAMP']).dt.date
    portfolio_df['AGGREGATED_VALUE'] = portfolio_df['AGGREGATED_VALUE'].astype('float64')
    return portfolio_df, transactions_df


def setup_database(db_path, ddl_tables_path='ddl/tables', ddl_views_path='ddl/view'):
    if not os.path.exists(db_path):
        print(f"Creating new database at: {db_path}")
    else:
        print(f"Database already exists at: {db_path}, continuing to apply DDLs.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    table_files = sorted(glob(os.path.join(ddl_tables_path, '*.sql')))
    for ddl_file in table_files:
        with open(ddl_file, 'r') as f:
            sql_script = f.read()
            print(f"Executing table DDL: {ddl_file}")
            cursor.executescript(sql_script)

    view_files = sorted(glob(os.path.join(ddl_views_path, '*.sql')))
    for ddl_file in view_files:
        with open(ddl_file, 'r') as f:
            sql_script = f.read()
            print(f"Executing view DDL: {ddl_file}")
            cursor.executescript(sql_script)

    conn.commit()
    conn.close()
    print("Database setup complete.")
