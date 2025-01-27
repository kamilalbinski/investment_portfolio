from manage_calculations import calculate_current_values, calculate_portfolio_over_time
from views.custom_views import default_pivot, default_table
from visualization.dynamic_plots import plot_portfolio_percentage, plot_portfolio_over_time, plot_asset_value_by_account
from manage_database_functions import refresh_market, refresh_fx
from manage_pipeline_functions import run_etl_processes
from utils.database_setup import get_temporary_owners_list


# Example usage of the new function
# input_df = pd.DataFrame({
#     'ASSET_ID': [1, 2],
#     'LATEST_PRICE_DATE': ['2023-01-01', '2023-06-01'],
#     'INITIAL_DATE': ['2022-01-01', '2022-06-01']
# })
# result = calculate_values_for_range(input_df)
# print(result)
#
# refresh_fx()