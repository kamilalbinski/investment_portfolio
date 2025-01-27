from etl_pipeline.loaders import add_new_asset
from utils.database_setup import fetch_data_from_database, get_asset_ids_from_database
from etl_pipeline.parsers_yfinance import download_adjusted_prices_from_yfinance
from etl_pipeline.parsers_biznesradar import download_adjusted_prices_from_biznesradar
from etl_pipeline.etl_utils import *
from calculations.calculations_edo import calculate_bulk_edo_values


def transform_holdings(new_data, is_edo=False):
    """Gets asset_id from Asset table based on ticker and market. If not found, calls function to add new asset_id"""

    new_data_df = pd.DataFrame(new_data)
    assets_df = get_asset_ids_from_database()

    #  Merge new data with existing data based on columns 'ticker' and 'market'

    if not is_edo:
        second_key = 'MARKET'
        assets_df.drop(columns=['INITIAL_DATE'], inplace=True)
    else:
        second_key = 'INITIAL_DATE'
        assets_df.drop(columns=['MARKET'], inplace=True)

    merged_df = pd.merge(new_data_df, assets_df, on=['NAME', second_key], how='left')
    # If there is any missing Asset_ID, add to database, return new asset_id and use in transformation

    if merged_df['ASSET_ID'].isna().values.any():
        for index, row in merged_df.iterrows():
            asset_id = add_new_asset(row['NAME'], row[second_key], is_edo)
            merged_df.at[index, 'ASSET_ID'] = asset_id

    merged_df = merged_df[['ASSET_ID', 'VOLUME', 'ACCOUNT_ID', 'REFRESH_DATE']]

    merged_df = transform_holdings_dtypes(merged_df)

    return merged_df


def transform_holdings_dtypes(data):
    df = data.copy()
    df['ASSET_ID'] = df['ASSET_ID'].astype('int64')
    df['VOLUME'] = df['VOLUME'].astype('int64')
    df['ACCOUNT_ID'] = df['ACCOUNT_ID'].astype('str')
    df['REFRESH_DATE'] = df['REFRESH_DATE'].astype('str')
    return df


def transform_decimal_separators(df, column_list):
    df = df.copy()
    for column in column_list:
        df[column] = df[column].str.replace(' ', '').str.replace(',', '.').astype('float64')
    return df


def combine_transactions(df):
    """Combines transactions which share the same key"""
    column_order = df.columns.to_list()

    grouped_df = df.groupby(['TIMESTAMP',
                             'ACCOUNT_ID',
                             'ASSET_ID'], as_index=False).agg({'VOLUME': 'sum',
                                                               'TRANSACTION_FEE': 'sum'}).reset_index()
    df_dropped_duplicates = df.drop_duplicates(subset=['TIMESTAMP',
                                                       'ACCOUNT_ID',
                                                       'ASSET_ID']).drop(columns=['VOLUME',
                                                                                  'TRANSACTION_FEE'])
    merged_df = pd.merge(df_dropped_duplicates, grouped_df, on=['TIMESTAMP',
                                                                'ACCOUNT_ID',
                                                                'ASSET_ID'])
    merged_df = merged_df[column_order]

    return merged_df


def transform_mbank_columns(df):
    df = df.copy()
    # Change decimal separators
    df = transform_decimal_separators(df, ['FX_RATE', 'PRICE', 'VALUE', 'TRANSACTION_FEE'])

    # Translate K(upno) to B(uy)
    df['BUY_SELL'] = np.where(df['BUY_SELL'] == 'K', 'B', df['BUY_SELL']).astype('str')

    df['VOLUME'] = df['VOLUME'].astype('int64')

    # Calculate FX rate based on transaction value
    df['FX_RATE'] = (df['VALUE'] / (df['VOLUME'] * df['PRICE'])).round(4)
    # Convert Transaction fee to transaction currency
    df['TRANSACTION_FEE'] = (df['TRANSACTION_FEE'] / df['FX_RATE']).round(2)

    df = combine_transactions(df)

    # Transform remaining columns
    df.drop(columns=['NAME', 'MARKET', 'VALUE'], inplace=True)
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%d.%m.%Y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['ACCOUNT_ID'] = df['ACCOUNT_ID'].astype('str')
    df['ASSET_ID'] = df['ASSET_ID'].astype('int64')
    df['ASSET_CURRENCY'] = df['ASSET_CURRENCY'].astype('str')
    df['BASE_CURRENCY'] = df['BASE_CURRENCY'].astype('str')

    return df


def transform_transactions(new_data):
    """Gets asset_id from Asset table based on ticker and market. If not found, calls function to add new asset_id"""

    new_data_df = pd.DataFrame(new_data)
    assets_df = get_asset_ids_from_database()

    column_order = new_data_df.columns.to_list()

    new_data_df.drop(columns='ASSET_ID', inplace=True)

    merged_df = pd.merge(new_data_df, assets_df, on=['NAME', 'MARKET'], how='left')
    # If there is any missing Asset_ID, add to database, return new asset_id and use in transformation

    merged_df = merged_df[column_order]

    if merged_df['ASSET_ID'].isna().values.any():
        for index, row in merged_df.iterrows():
            asset_id = add_new_asset(row['NAME'], row['MARKET'], is_edo=False)
            merged_df.at[index, 'ASSET_ID'] = asset_id

    merged_df = merged_df[column_order]
    merged_df = transform_mbank_columns(merged_df)

    return merged_df


def transform(new_data, source, file_type):
    """handle transform at high-level"""

    transformed_data = None

    if source == 'mbank':
        if file_type == 'holdings':
            transformed_data = transform_holdings(new_data, is_edo=False)
        elif file_type == 'transactions':
            transformed_data = transform_transactions(new_data)
    elif source == 'pkotb':
        transformed_data = transform_holdings(new_data, is_edo=True)

    return transformed_data


def transform_assets_for_refresh():
    assets_df = fetch_data_from_database('ASSETS')
    mappings_yfinance_df = fetch_data_from_database('MAPPING_YFINANCE')

    merged_df = pd.merge(assets_df, mappings_yfinance_df, on='ASSET_ID', how='left')

    return merged_df


def get_new_assets(assets_df, latest_prices_df, table_type='PRICES'):
    latest_prices_list = latest_prices_df['ASSET_ID'].to_list()

    if table_type == 'PRICES':
        new_assets = assets_df[(assets_df['MARKET'] != str(0))].copy()
    elif table_type == 'CURRENCIES':
        new_assets = assets_df[(assets_df['MARKET'] == str(0)) & (assets_df['CATEGORY'] == 'FX')].copy()
    else:
        print('Unknown table type')
        new_assets = assets_df[(assets_df['MARKET'] != str(0))].copy()

    new_assets = new_assets[(~new_assets['ASSET_ID'].isin(latest_prices_list))][['ASSET_ID', 'INITIAL_DATE']]
    new_assets.rename(columns={'INITIAL_DATE': 'DATE'}, inplace=True)
    new_assets['DATE'] = pd.to_datetime(new_assets['DATE']) + pd.Timedelta(days=-1)
    new_assets['DATE'] = new_assets['DATE'].dt.strftime('%Y-%m-%d 00:00:00')
    return new_assets


def transform_prices_for_refresh(table_type='PRICES'):
    # get assets
    assets_df = fetch_data_from_database('ASSETS')

    # get Latest PRICES & Mapping from Database, merge both
    latest_prices_df = fetch_data_from_database(f'LATEST_{table_type}')

    # Handle assets not included in table
    new_assets_df = get_new_assets(assets_df, latest_prices_df, table_type=table_type)

    if not new_assets_df.empty:
        latest_prices_df = pd.concat([latest_prices_df, new_assets_df], axis=0)
        print('New asset(s) found. Added to database')

    prices_to_refresh_df = pd.merge(latest_prices_df, assets_df[['ASSET_ID', 'NAME', 'PRICE_SOURCE','INITIAL_DATE']],
                                    on='ASSET_ID', how='inner')

    prices_from_yfinance_df = prices_to_refresh_df[(prices_to_refresh_df['PRICE_SOURCE'] == 'YFINANCE')].drop(
        columns=['NAME', 'PRICE_SOURCE']).copy()

    prices_from_tbpl_df = prices_to_refresh_df[(prices_to_refresh_df['PRICE_SOURCE'] == 'PLGOV')].drop(
        columns=['PRICE_SOURCE']).copy()

    prices_from_biznesradar_df = prices_to_refresh_df[(prices_to_refresh_df['PRICE_SOURCE'] == 'BIZNESRADAR')].drop(
        columns=['PRICE_SOURCE']).copy()

    all_prices = []

    # Merge latest prices and yfinance mapping

    if not prices_from_yfinance_df.empty:
        mappings_yfinance_df = fetch_data_from_database('MAPPING_YFINANCE')
        merged_yfinance_df = pd.merge(prices_from_yfinance_df, mappings_yfinance_df, on='ASSET_ID', how='inner')

        # transform into yfinance extractor-viable format
        merged_yfinance_df['DATE'] = pd.to_datetime(merged_yfinance_df['DATE'])
        merged_yfinance_df.drop(columns='PRICE', inplace=True)
        new_prices_yfinance_df = download_adjusted_prices_from_yfinance(merged_yfinance_df)

        if new_prices_yfinance_df:
            merged = pd.merge(new_prices_yfinance_df, prices_from_yfinance_df, on=['ASSET_ID', 'DATE'], how='left', indicator=True)

            final_prices_yfinance_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge','PRICE_y', 'INITIAL_DATE'])
            final_prices_yfinance_df = final_prices_yfinance_df.rename(columns={'PRICE_x': 'PRICE'})
            all_prices.append(final_prices_yfinance_df)

    if not prices_from_tbpl_df.empty:
        final_prices_tbpl_df = calculate_bulk_edo_values(prices_from_tbpl_df)
        all_prices.append(final_prices_tbpl_df)

    if not prices_from_biznesradar_df.empty:
        final_prices_biznesradar_df = download_adjusted_prices_from_biznesradar(prices_from_biznesradar_df)
        all_prices.append(final_prices_biznesradar_df)

    if len(all_prices) >= 1:
        final_prices_df = pd.concat(all_prices, axis=0)

        return final_prices_df

    else:
        print(f'{table_type.title()} are up to date')



