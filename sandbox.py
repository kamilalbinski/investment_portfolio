from manage_calculations import *
from views.custom_views import *
from visualization.dynamic_plots import *
from manage_database_functions import *
from manage_pipeline_functions import *
from utils.database_setup import get_temporary_owners_list

import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import datetime
import utils.config

import sqlite3
import os

import sqlite3
import os


def get_table_names(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    return [table[0] for table in tables]


def get_ddl_for_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    ddl = cursor.fetchone()
    if ddl:
        # Add "IF NOT EXISTS" clause to the CREATE TABLE statement
        modified_ddl = ddl[0].replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS', 1)
        return modified_ddl
    return None


def save_ddl_to_file(ddl_directory, table_name, ddl):
    file_path = os.path.join(ddl_directory, f"create_{table_name}.sql")
    with open(file_path, 'w') as file:
        file.write(ddl)
        print(f"Created {file_path}")


def generate_ddl_files(database_path, ddl_directory):
    conn = sqlite3.connect(database_path)

    # Ensure the DDL directory exists
    if not os.path.exists(ddl_directory):
        os.makedirs(ddl_directory)

    table_names = get_table_names(conn)
    for table_name in table_names:
        ddl = get_ddl_for_table(conn, table_name)
        if ddl:
            save_ddl_to_file(ddl_directory, table_name, ddl)

    conn.close()


# Example usage
if __name__ == "__main__":
    database_path = DATABASE_FILE
    ddl_directory = 'ddl'
    generate_ddl_files(database_path, ddl_directory)
