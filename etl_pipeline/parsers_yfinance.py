import yfinance as yf
from etl_pipeline.etl_utils import *
from pandas.tseries.offsets import BDay

def get_tickers_from_assets_df(df, tickers_only=True):
    if not tickers_only:
        tickers = df['YFINANCE_ID', 'ASSET_ID', 'PRICE_MULTIPLIER'].dropna(subset=['YFINANCE_ID'])
    else:
        tickers = df['YFINANCE_ID'].dropna()
    return tickers


def get_tickers_from_fxes_df(df):
    return (df['FROM_CURRENCY'] + df['TO_CURRENCY'] + '=X').to_list()


def get_prices_from_yfinance(tickers, rounding=2):
    results = []
    for ticker in tickers:
        s = yf.download(ticker, period="1d", progress=False)['Close']
        date = str(s.index[0])
        price = round(s.values[0], rounding)
        result = {'YFINANCE_ID': ticker, 'CURRENT_PRICE': price, 'PRICE_DATE': date}
        results.append(result)
    df = pd.DataFrame(results)
    return df


def download_adjusted_prices_from_yfinance(df):
    data_rows = []
    today = pd.Timestamp.now().normalize()

    for index, row in df.iterrows():
        ticker = row['YFINANCE_ID']
        date = row['DATE']
        multiplier = row['PRICE_MULTIPLIER']

        start_date = pd.to_datetime(date) + pd.Timedelta(days=1)
        end_date = today - BDay(1)

        if start_date > end_date:
            continue

        yfinance_record = yf.download(
            ticker,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            progress=False,
            auto_adjust=False  # So 'Adj Close' remains available
        )

        if not yfinance_record.empty and 'Adj Close' in yfinance_record.columns:

            yfinance_record.index = pd.to_datetime(yfinance_record.index)
            # Apply your multiplier
            yfinance_record['Adj Close'] = yfinance_record['Adj Close'] / multiplier
            for stock_date, adj_close in yfinance_record['Adj Close'][ticker].items():
                data_rows.append({
                    'ASSET_ID': row['ASSET_ID'],
                    'DATE': stock_date.strftime('%Y-%m-%d 00:00:00'),
                    'PRICE': round(adj_close, 4)
                })

    # Create a DataFrame if data_rows is not empty
    if data_rows:
        return pd.DataFrame(data_rows)
    else:
        return None


def get_prices_from_yfinance_2(tickers, rounding=2):
    results = []
    for ticker in tickers:
        s = yf.download(ticker, start=11, end=11, progress=False)
        date = str(s.index[0])
        price = round(s.values[0], rounding)
        result = {'YFINANCE_ID': ticker, 'CURRENT_PRICE': price, 'PRICE_DATE': date}
        results.append(result)
    df = pd.DataFrame(results)
    return df
