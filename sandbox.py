import yfinance as yf

# Specify the stock ticker
ticker = 'AAPL'  # Replace with your desired stock ticker
stock = yf.Ticker(ticker)

# Get the historical data including dividends
dividends = stock.history(period="max", actions=True)['Dividends']
#
# # Display dividend information
# if not dividends.empty:
#     print(dividends)
# else:
#     print("No dividend data available for this ticker.")

dividend_df = dividends.reset_index()
dividend_df.columns = ['Date', 'Dividend']
print(dividend_df[dividend_df['Dividend']>0])