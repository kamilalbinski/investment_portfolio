from manage_calculations import calculate_current_values, calculate_return_rate_per_asset
from views.custom_views import default_pivot, aggregated_values_pivoted, default_table
from visualization.dynamic_plots import plot_portfolio_percentage, plot_portfolio_over_time, plot_asset_value_by_account, plot_return_values
from manage_database_functions import *
from etl_pipeline.parsers_files import *
from manage_pipeline_functions import run_etl_processes
from utils.database_setup import get_temporary_owners_list, get_portfolio_over_time
import yfinance as yf
from utils.config import ROOT_PATH
import os



run_etl_processes()