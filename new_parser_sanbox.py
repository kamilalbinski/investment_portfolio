from manage_calculations import calculate_current_values, calculate_return_rate_per_asset
from views.custom_views import default_pivot, aggregated_values_pivoted, default_table
from visualization.dynamic_plots import plot_portfolio_percentage, plot_portfolio_over_time, plot_asset_value_by_account, plot_return_values
from manage_database_functions import *
from manage_pipeline_functions import run_etl_processes
from utils.database_setup import get_temporary_owners_list, get_portfolio_over_time
import yfinance as yf
from utils.config import ROOT_PATH
import os

#La6$t6b6RY
import fitz  # PyMuPDF
import getpass

# Step 1: Open the PDF (ask for password)
file_path =  os.path.join(ROOT_PATH, r'data\TwojeDokumenty.pdf')
# password = getpass.getpass("Enter PDF password: ")

doc = fitz.open(file_path)
if doc.is_encrypted:
    if not doc.authenticate(password):
        print("Wrong password!")
        exit()

# Total number of embedded files
count = doc.embfile_count()

for i in range(count):
    info = doc.embfile_info(i)  # Get metadata
    actual_name = info["filename"]
    print(f"Found attachment: {actual_name}")

    # Extract by index, not name
    file_data = doc.embfile_get(i)

    with open(actual_name, "wb") as f:
        f.write(file_data)
    print(f"Saved: {actual_name}")



import pdfplumber
import pandas as pd
import re

pdf_path = "20241223_91316625_TRANS_14588007-848a-48d6-b5de-fe0d94b10b12.pdf"

records = []
account_number = None
account_owner = None
account_pattern = re.compile(r"Potwierdzenie wykonania zleceń na rachunku numer (\d+)")

with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        text = page.extract_text()
        lines = text.splitlines()

        # Extract account number (only once)
        if account_number is None:
            account_number = lines[4].split()[1]
            account_owner = lines[3].split()[1].capitalize()

            # match = account_pattern.search(text)
            # if match:
            #     account_number = match.group(1)

        # Extract tables
        tables = page.extract_tables()
        for table in tables:
            for row in table:
                if 'PROWIZJA' in table[0] and row[0] not in ['RYNEK', 'RAZEM']:
                    # Attach account number to each row
                    row.append(account_number)
                    row.append(account_owner)
                    records.append(row)

# Convert to DataFrame and name columns (assuming consistent structure)
columns = ["RYNEK", "WALOR", "OFERTA", "LICZBA", "CENA", "WARTOŚĆ",
           "PROWIZJA","KURS WALUTY", "CZAS TRANSAKCJI", "DATA ROZLICZENIA", "RACHUNEK", "WŁAŚCICIEL"]
df = pd.DataFrame(records, columns=columns)

# Optionally: save to CSV
# df.to_csv("transactions_with_account.csv", index=False)

print(df.head())

# import pdfplumber
# import pandas as pd
# import re
#
# pdf_path = "20241223_91316625_TRANS_14588007-848a-48d6-b5de-fe0d94b10b12.pdf"
#
#
#
# account_number = None
# account_pattern = re.compile(r"Potwierdzenie wykonania zleceń na rachunku numer (\d+)")
#
# with pdfplumber.open(pdf_path) as pdf:
#     for page in pdf.pages:
#         text = page.extract_text()
#         match = account_pattern.search(text)
#         if match:
#             account_number = match.group(1)
#             break  # Assuming account number appears only once
#
# records = []
#
# with pdfplumber.open(pdf_path) as pdf:
#     for page in pdf.pages:
#         # Use layout-based extraction
#         tables = page.extract_tables()
#         for table in tables:
#             for row in table:
#                 if 'PROWIZJA' in table[0] and row[0] not in ['RYNEK', 'RAZEM']:
#                     records.append(row)
#
# # Convert to DataFrame (the table structure isn't guaranteed, so show raw rows first)
# df = pd.DataFrame(records)
# df.to_csv('transactions.csv')
# #import ace_tools as tools; tools.display_dataframe_to_user(name="Raw Transaction Rows (Layout-Based)", dataframe=df)
