from manage_calculations import calculate_current_values, calculate_return_rate_per_asset
from views.custom_views import default_pivot, aggregated_values_pivoted, default_table
from visualization.dynamic_plots import plot_portfolio_percentage, plot_portfolio_over_time, plot_asset_value_by_account, plot_return_values
from manage_database_functions import *
from manage_pipeline_functions import run_etl_processes
from utils.database_setup import get_temporary_owners_list, get_portfolio_over_time
import yfinance as yf
from matplotlib import pyplot as plt

# Example usage of the new function
# input_df = pd.DataFrame({
#     'ASSET_ID': [1, 2],
#     'LATEST_PRICE_DATE': ['2023-01-01', '2023-06-01'],
#     'INITIAL_DATE': ['2022-01-01', '2022-06-01']
# })
# result = calculate_values_for_range(input_df)
# print(result)
#
#print(default_table(owner='Kamil')[['NAME', 'PROFILE','RETURN_RATE','RETURN_RATE_BASE']])
# print(calculate_current_values(owner='Kamil', return_totals=True))
# print(calculate_current_values(owner='Kamil').columns)




