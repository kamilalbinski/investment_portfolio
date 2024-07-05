import csv
import os
import re
from pandas.tseries.offsets import DateOffset
from etl_utils import *

def convert_date(item):
    item = item.split('.')
    return f'{item[2]}-{item[1]}-{item[0]} 00:00:00'


def convert_date_2(date_str):
    """
    Converts a date string from one format to another.

    :param date_str: Date string in the format 'DD.MM.YYYY'
    :return: Date string in the format 'YYYY-MM-DD'
    """
    return datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')

def parse_date_from_filename(file_name):
    # Example regex pattern to extract date (adjust as needed)
    date_pattern = r'(\d{4}-\d{2}-\d{2})'

    match = re.search(date_pattern, file_name)
    if match:
        return match.group(1)
    else:
        return None


def read_csv_header_mbank(file_path, skip_rows, delimiter=';'):
    """
    Reads the initial part of the CSV to determine the file type.

    :param file_path: Path to the CSV file
    :param skip_rows: Number of rows to skip before reading the file type
    :param delimiter: Delimiter used in the CSV file
    :return: The file type as a string
    """
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file, delimiter=delimiter)
        for _ in range(skip_rows):
            next(reader)
        file_type = next(reader)[0]
    return file_type


def parse_file_type_1_mbank(reader):
    """
    Parses CSV data for file type 1.

    :param reader: CSV reader object
    :return: Parsed data specific to file type 1
    """
    account_prefix = 'MB_'

    # Skipping rows to reach the account owner information
    for _ in range(10):
        next(reader)
    account_owner = next(reader)[0].split()[0]

    # Skipping rows to reach the account ID
    for _ in range(2):
        next(reader)
    account_id = next(reader)[0]

    # Skipping rows to reach the date
    for _ in range(5):
        next(reader)
    date = next(reader)[0]
    date = convert_date(date)

    # Skipping header and separator rows to reach the tabular data
    for _ in range(4):
        next(reader)

    # Collecting the tabular data (removing summary rows)
    file_data = [row for row in reader][:-2]

    # Creating the parsed data structure
    parsed_data = {
        'account_owner': account_owner,
        'account_id': account_prefix + account_id,
        'date': date,
        'file_data': file_data
    }
    return parsed_data


def parse_file_type_2_mbank(reader):
    """
    Placeholder function for parsing file type 2.

    :param reader: CSV reader object
    :return: Parsed data specific to file type 2
    """
    # Placeholder for file type 2 parsing logic
    account_prefix = 'MB_'

    # Skipping rows to reach the account owner information
    for _ in range(10):
        next(reader)
    account_owner = next(reader)[0].split()[0]

    # Skipping rows to reach the account ID
    for _ in range(2):
        next(reader)
    account_id = next(reader)[0]

    # Skipping rows to reach the date
    for _ in range(5):
        next(reader)
    date = next(reader)[0]
    date = convert_date(date)

    # Skipping header and separator rows to reach the tabular data
    for _ in range(15):
        next(reader)

    # Collecting the tabular data (removing summary rows)
    file_data = [row for row in reader]

    # Creating the parsed data structure
    parsed_data = {
        'account_owner': account_owner,
        'account_id': account_prefix + account_id,
        'date': date,
        'file_data': file_data
    }

    return parsed_data


def create_results_table_mbank(parsed_data, file_type):
    """
    Builds the results table from parsed data.

    :param parsed_data: Data parsed from the CSV
    :param file_type: Type of the file to handle specific formatting
    :return: Results table built from the parsed data
    """
    results_table = []

    if file_type == 'holdings':
        for file_row in parsed_data['file_data']:
            # Assuming file_row structure is known and consistent
            ticker_name, market, volume = file_row[:3]
            result_row = {
                'OWNER': parsed_data['account_owner'],
                'ACCOUNT_ID': parsed_data['account_id'],
                'REFRESH_DATE': parsed_data['date'],
                'NAME': ticker_name,
                'MARKET': market,
                'VOLUME': volume
            }
            results_table.append(result_row)
    # Placeholder for handling file type 2 data structure
    elif file_type == 'transactions':
        for file_row in parsed_data['file_data']:
            timestamp, ticker_name, market, buy_sell, volume, price, asset_ccy, trans_fee, base_ccy, value = file_row[:10]

            result_row = {
                'TIMESTAMP': timestamp,
                'ACCOUNT_ID': parsed_data['account_id'],
                'ASSET_ID': None,
                'BUY_SELL': buy_sell,
                'VOLUME': volume,
                'PRICE': price,
                'TRANSACTION_FEE': trans_fee,
                'ASSET_CURRENCY': asset_ccy,
                'BASE_CURRENCY': base_ccy,
                'VALUE': value,
                'NAME': ticker_name,
                'MARKET': market,
                'FX_RATE': None
            }
            results_table.append(result_row)
    return results_table


def parse_mbank(file_path):
    """
    Orchestrates the parsing of mBank CSV files based on their type.

    :param file_path: Path to the CSV file
    :return: Results table and a flag indicating whether it is an EDO file
    """

    source = 'mbank'

    skip_rows = 7  # Number of rows to skip might be adjusted based on actual requirements
    file_type = read_csv_header_mbank(file_path, skip_rows)
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        if file_type == 'eMAKLER - Portfel':
            parsed_data = parse_file_type_1_mbank(reader)
            file_type = 'holdings'
        elif file_type == 'eMAKLER - Historia transakcji':
            parsed_data = parse_file_type_2_mbank(reader)
            file_type = 'transactions'
        else:
            raise ValueError("Unsupported file type")
        results_table = create_results_table_mbank(parsed_data, file_type)
    return results_table, source, file_type  # The second part of the return value might be adjusted based on actual requirements


def parse_pkotb(file_path):

    source = 'pkotb'

    df = pd.read_excel(file_path)

    date_string = parse_date_from_filename(os.path.basename(file_path))

    df.columns.values[0] = "NAME"
    df.columns.values[1] = "VOLUME"
    df.columns.values[2] = "BLOCKED"
    df.columns.values[3] = "VALUE"
    df.columns.values[4] = "CURRENT_VALUE"
    df.columns.values[5] = "MATURITY_DATE"

    df['PRICE_DATE'] = pd.to_datetime(df['MATURITY_DATE']) - DateOffset(years=10)
    df['MARKET'] = '0'
    df['ACCOUNT_ID'] = os.path.basename(os.path.dirname(file_path))
    df['REFRESH_DATE'] = f'{date_string} 00:00:00'

    # Create a table combining information from rows
    results_table = df.copy()
    is_edo = True
    file_type = 'holdings'

    return results_table, source, file_type
