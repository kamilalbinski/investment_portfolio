# loaders.py

from utils.config import DATABASE_FILE
from etl_pipeline.etl_utils import *


def add_default_values(data, is_edo=False):
    """ default value for a new record"""
    df = data.copy()
    if not is_edo:
        df['PRICE_DATE'] = pd.to_datetime('2021-06-12 00:00:00')  # Dummy date to allow refreshing
    else:
        # Default values for EDO instrument
        df['MARKET'] = 0
        df['CATEGORY'] = "BOND"
        df['SUB_CATEGORY'] = "BONDS"
        df['CURRENT_PRICE'] = 100.00
        df['CURRENCY'] = "PLN"
        # df['PRICE_DATE'] = df['PRICE_DATE'].astype('str')

    return df


def add_new_asset(first_key, second_key, is_edo=False):
    # Connect to the SQLite database
    conn = sqlite3.connect(DATABASE_FILE)

    if not is_edo:
        column_to_lookup = 'MARKET'
    else:
        column_to_lookup = 'PRICE_DATE'

    # Check if the asset already exists in the ASSETS table
    query = f"SELECT ASSET_ID FROM ASSETS WHERE NAME = '{first_key}' AND {column_to_lookup} = '{second_key}'"
    existing_asset_id = pd.read_sql_query(query, conn)

    if existing_asset_id.empty:
        # Asset not found, add a new row to the ASSETS table
        new_asset_data = {'NAME': [first_key], column_to_lookup: [second_key]}
        new_asset_df = pd.DataFrame(new_asset_data)

        new_asset_df = add_default_values(new_asset_df, is_edo)

        new_asset_df.to_sql('ASSETS', conn, if_exists='append', index=False)
        print(f'{first_key} for {str(second_key).split()[0]} not found. Adding to table: "ASSETS"')
        # Retrieve the newly generated ASSET_ID
        query = f"SELECT ASSET_ID FROM ASSETS WHERE NAME = '{first_key}' AND {column_to_lookup} = '{second_key}'"
        new_asset_id = pd.read_sql_query(query, conn)['ASSET_ID'].values[0]

        # Close the database connection
        conn.close()

        return new_asset_id

    # Asset already exists, retrieve the existing ASSET_ID
    existing_asset_id = existing_asset_id['ASSET_ID'].values[0]

    # Close the database connection
    conn.close()

    return existing_asset_id


def load_holdings(new_df):
    # Ensure REFRESH_DATE is datetime for comparison

    conn = sqlite3.connect(DATABASE_FILE)

    # Read existing data from the HOLDINGS table
    query = 'SELECT ASSET_ID, VOLUME, ACCOUNT_ID, REFRESH_DATE FROM HOLDINGS'

    existing_df = pd.read_sql_query(query, conn)

    existing_df['REFRESH_DATE'] = pd.to_datetime(existing_df['REFRESH_DATE'])
    new_df['REFRESH_DATE'] = pd.to_datetime(new_df['REFRESH_DATE'])

    # Find new or updated records in new_df
    updated_records = pd.merge(new_df, existing_df, on="ACCOUNT_ID", how="left", suffixes=('_new', '_old'))

    # Filter out new or more recent records
    updated_records = updated_records[
        (updated_records['REFRESH_DATE_new'] >= updated_records['REFRESH_DATE_old']) | updated_records[
            'REFRESH_DATE_old'].isnull()]

    # Drop old records from existing_df that need to be updated
    old_df_filtered = existing_df[~existing_df['ACCOUNT_ID'].isin(updated_records['ACCOUNT_ID'])]

    # Select the new or updated records to add to the old DataFrame
    new_records_to_add = new_df[new_df['ACCOUNT_ID'].isin(updated_records['ACCOUNT_ID'])]

    # Combine the old filtered DataFrame with the new or updated records
    updated_df = pd.concat([old_df_filtered, new_records_to_add], ignore_index=True)

    if not existing_df.equals(updated_df):
        # Clear existing records in the HOLDINGS table
        delta = abs(updated_df.shape[0] - existing_df.shape[0])
        conn.execute("DELETE FROM HOLDINGS;")
        # Append new data to the HOLDINGS table
        updated_df.to_sql('HOLDINGS', conn, if_exists='append', index=False, method='multi')

        # Commit the transaction\
        print(f'Upload completed: {delta} new record(s) added to Database')

    else:
        # existing_holdings_df.to_sql('HOLDINGS', conn, if_exists='append', index=False, method='multi')
        print('No changes required')

    conn.close()


def load_transactions(new_df):
    # Create a cursor object using the connection

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    appended_count = 0

    # Iterate over DataFrame rows as tuples
    for row in new_df.itertuples(index=False):
        # Check if record exists
        query = """SELECT COUNT(*) FROM TRANSACTIONS 
                   WHERE TIMESTAMP = ? AND ACCOUNT_ID = ? AND ASSET_ID = ?"""
        cursor.execute(query, (row.TIMESTAMP, row.ACCOUNT_ID, row.ASSET_ID))
        result = cursor.fetchone()

        if result[0] == 0:
            # Insert query with all columns specified
            insert_query = """INSERT INTO TRANSACTIONS 
                              (TIMESTAMP, ACCOUNT_ID, ASSET_ID, BUY_SELL, VOLUME, PRICE, TRANSACTION_FEE,\
                               ASSET_CURRENCY, BASE_CURRENCY, FX_RATE) 
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

            cursor.execute(insert_query, row)
            appended_count += 1

    if not appended_count == 0:
        print(f'Upload completed: {appended_count} new record(s) added to Database')
    else:
        print('No changes required')

    conn.commit()


def load(new_data, file_type):
    """Function to manage load, depending on file type"""

    if file_type == 'holdings':
        load_holdings(new_data)
    elif file_type == 'transactions':
        load_transactions(new_data)


def upload_to_table(new_data, table, action='append'):
    # Validate the action parameter
    if action not in ['fail', 'replace', 'append']:
        raise ValueError("action parameter must be one of 'fail', 'replace', 'append'")

    # Connect to the SQLite database
    conn = sqlite3.connect(DATABASE_FILE)
    new_data_df = pd.DataFrame(new_data)

    # Depending on the action, append or replace data in the table
    if action == 'replace':
        conn.execute(f"DELETE FROM {table};")  # Clear the table before inserting new data

    # Use pandas to_sql function to insert data
    new_data_df.to_sql(table, conn, if_exists=action, index=False, method='multi')

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()
