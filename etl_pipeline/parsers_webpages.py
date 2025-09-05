from bs4 import BeautifulSoup
import requests
from etl_pipeline.etl_utils import *
import os
from utils.config import BIZNESRADAR_URL, CPI_URL


# Function to fetch and parse the webpage content

def fetch_and_parse_table(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch the webpage: {url}")

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table
    table = soup.find('table', {'class': 'qTableFull'})
    if not table:
        raise Exception("Table not found in the provided URL.")

    # Extract the table headers
    headers = [header.text for header in table.find_all('th')]

    # Extract the rows
    rows = []
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        row_data = [cell.text for cell in cells]
        rows.append(row_data)

    # Create a DataFrame
    df = pd.DataFrame(rows, columns=headers)

    # Select the "Data" and "Zamknięcie" columns
    df_selected = df[['Data', 'Zamknięcie']]
    df_selected.loc[:, 'Data'] = pd.to_datetime(df_selected['Data'], format='%d.%m.%Y')

    return df_selected


def adjust_price_bizradar_df(df, asset_id, min_date):
    df['ASSET_ID'] = asset_id
    adjusted_df = df.rename(columns={'Data': 'DATE',
                                     'Zamknięcie': 'PRICE'})

    adjusted_df['PRICE'] = adjusted_df['PRICE'].astype('float64')

    adjusted_df['PRICE'] *= 10
    adjusted_df['DATE'] = pd.to_datetime(adjusted_df['DATE'])

    adjusted_df = adjusted_df[adjusted_df['DATE'] > min_date]

    return adjusted_df


def download_adjusted_prices_from_biznesradar(df):
    adjusted_prices = []

    df['DATE'] = pd.to_datetime(df['DATE'])

    for index, row in df.iterrows():
        asset_id = row['ASSET_ID']
        ticker = row['NAME']
        date = row['DATE']
        url = f'{BIZNESRADAR_URL}{ticker}'
        raw_table_df = fetch_and_parse_table(url)
        adjusted_prices.append(adjust_price_bizradar_df(df=raw_table_df, asset_id=asset_id, min_date=date))

    adjusted_prices_df = pd.concat(adjusted_prices)

    adjusted_prices_df = adjusted_prices_df[['ASSET_ID', 'DATE', 'PRICE']]
    adjusted_prices_df['DATE'] = pd.to_datetime(adjusted_prices_df['DATE'])
    adjusted_prices_df.sort_values(by='DATE',ascending=True, inplace=True)
    adjusted_prices_df['DATE'] = adjusted_prices_df['DATE'].dt.strftime('%Y-%m-%d 00:00:00')

    return adjusted_prices_df


def parse_cpi_pl() -> tuple[pd.DataFrame, str, str]:
    """
    Downloads and parses CPI data from Polish statistical office.
    Returns cleaned DataFrame, source tag, and file_type.
    """

    temp_file = 'cpi_temp.csv'

    # Download file

    response = requests.get(CPI_URL)
    with open(temp_file, 'wb') as f:
        f.write(response.content)  # Raw bytes written

    # Read CSV
    df = pd.read_csv(temp_file, encoding="ISO-8859-2", delimiter=';') # TODO fix encoding

    df.columns.values[0] = "VARIABLE"
    df.columns.values[1] = "REGION"
    df.columns.values[2] = "TYPE"
    df.columns.values[3] = "YEAR"
    df.columns.values[4] = "MONTH"
    df.columns.values[5] = "VALUE"
    df.columns.values[6] = "FLAG"

    type_value = 'Analogiczny miesišc poprzedniego roku = 100'
    filtered_df = df[df['TYPE']==type_value].reset_index()

    os.remove(temp_file)

    return filtered_df
