import csv
import os
import pdfplumber
import re

from etl_pipeline.etl_utils import *


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

def parse_mbank_csv_file(reader):
    """
    Parses mBank CSV transaction file and returns a formatted DataFrame.

    :param reader: Path to the CSV file
    :return: Tuple of (results_table, source, file_type)
    """
    account_prefix = 'MB_'

    # Skip to account owner
    for _ in range(10):
        next(reader)
    account_owner = next(reader)[0].split()[0]

    # Skip to account ID
    for _ in range(2):
        next(reader)
    account_id = account_prefix + next(reader)[0]

    # Skip to date
    for _ in range(5):
        next(reader)
    date_str = next(reader)[0]
    date = convert_date(date_str)

    # Skip to tabular data
    for _ in range(15):
        next(reader)

    # Collect data into DataFrame
    rows = [row[:10] for row in reader]
    df = pd.DataFrame(rows, columns=[
        'TIMESTAMP', 'NAME', 'MARKET', 'BUY_SELL', 'VOLUME', 'PRICE',
        'ASSET_CURRENCY', 'TRANSACTION_FEE', 'BASE_CURRENCY', 'VALUE'
    ])

    df['NAME'] = df['NAME'].str.replace(' ','')
    df['ACCOUNT_ID'] = account_id
    df['ASSET_ID'] = None
    df['FX_RATE'] = None
    df['INITIAL_DATE'] = date  # Optional: keep for compatibility

    results_table = df[['TIMESTAMP', 'ACCOUNT_ID', 'ASSET_ID', 'BUY_SELL', 'VOLUME', 'PRICE',
                        'TRANSACTION_FEE', 'ASSET_CURRENCY', 'BASE_CURRENCY', 'VALUE',
                        'NAME', 'MARKET', 'INITIAL_DATE', 'FX_RATE']]

    return results_table


def parse_mbank_pdf_file(file_path):

    records = []
    account_number = None
    account_owner = None
    # account_pattern = re.compile(r"Potwierdzenie wykonania zleceń na rachunku numer (\d+)")


    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.splitlines()

            # Extract account number (only once)
            if account_number is None:
                account_number = lines[4].split()[1]
                account_owner = lines[3].split()[1].capitalize()

            # Extract tables
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if 'PROWIZJA' in table[0] and row[0] not in ['RYNEK', 'RAZEM']:
                        # Attach account number to each row
                        row.append(account_number)
                        row.append(account_owner)
                        records.append(row)

        #print("Incorrect password or unable to open PDF.")

    # Convert to DataFrame and name columns (assuming consistent structure)
    columns = ["RYNEK", "WALOR", "OFERTA", "LICZBA", "CENA", "WARTOŚĆ",
               "PROWIZJA", "KURS WALUTY", "CZAS TRANSAKCJI", "DATA ROZLICZENIA", "RACHUNEK", "WŁAŚCICIEL"]

    return pd.DataFrame(records, columns=columns)

def parse_mbank(file_path):
    """
    Orchestrates the parsing of mBank CSV or PDF files for transactions only.

    :param file_path: Path to the input file
    :return: Tuple of (results_table, source, file_type)
    """
    source = 'mbank'


    if file_path.lower().endswith('.pdf'):
        file_type = 'pdf'

        results_table = parse_mbank_pdf_file(file_path)  # Placeholder function

    elif file_path.lower().endswith('.csv'):
        file_type = 'csv'

        with open(file_path, 'r') as csv_file:
            reader = csv.reader(csv_file, delimiter=';')
            results_table = parse_mbank_csv_file(reader)
    else:
        raise ValueError("Unsupported file format. Only .csv and .pdf are supported.")

    return results_table, source, file_type


def parse_pkotb(file_path):

    source = 'pkotb'

    df = pd.read_excel(file_path)

    df.columns.values[0] = "DATE"
    df.columns.values[1] = "TYPE"
    df.columns.values[2] = "NAME"
    df.columns.values[5] = "VOLUME"
    df.columns.values[6] = "VALUE"

    df['ACCOUNT_ID'] = os.path.basename(os.path.dirname(file_path))

    add_types = ['zakup papierów']
    deduct_types = ['przedterminowy wykup']
    relevant_types = add_types + deduct_types
    initial_dates_str = add_types[0]

    df = df[df['TYPE'].isin(relevant_types)]

    initial_dates = df[df['TYPE'] == initial_dates_str].groupby(['NAME','DATE'])['DATE'].min()
    initial_dates_alt = df[df['TYPE'] == initial_dates_str].groupby(['NAME'])['DATE'].min() ## workaround to handle missing details

    df['INITIAL_DATE'] = df.set_index(['NAME', 'DATE']).index.map(initial_dates)
    df.loc[df['INITIAL_DATE'].isna(), 'INITIAL_DATE'] = df.loc[df['INITIAL_DATE'].isna(), 'NAME'].map(
        initial_dates_alt)

    df['INITIAL_DATE'] = pd.to_datetime(df['INITIAL_DATE'])

    df['BUY_SELL'] = np.where(df['TYPE'].isin(add_types), 'B', 'S').astype('str')
    df['TIMESTAMP'] = pd.to_datetime(df['DATE']).dt.strftime('%Y-%m-%d %H:%M:%S')

    df['ASSET_ID'] = None
    df['VOLUME'] = df["VALUE"] / 100
    df['PRICE'] = 100.0
    df['TRANSACTION_FEE'] = 0.0
    df['ASSET_CURRENCY'] = 'PLN'
    df['BASE_CURRENCY'] = 'PLN'
    df['MARKET'] = 0
    df['FX_RATE'] = 1.0

    results_table = df[['TIMESTAMP','ACCOUNT_ID','ASSET_ID','BUY_SELL', 'VOLUME', 'PRICE','TRANSACTION_FEE',
                        'ASSET_CURRENCY', 'BASE_CURRENCY', 'VALUE','NAME', 'MARKET','INITIAL_DATE', 'FX_RATE']]

    file_type = 'transactions'

    return results_table, source, file_type
