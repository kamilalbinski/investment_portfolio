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


def execute_ddl_scripts(database_path, ddl_directory):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    for filename in os.listdir(ddl_directory):
        if filename.endswith(".sql"):
            with open(os.path.join(ddl_directory, filename), 'r') as file:
                ddl_script = file.read()
                cursor.executescript(ddl_script)
                print(f"Executed {filename}")

    conn.commit()
    conn.close()


# Example usage
if __name__ == "__main__":
    database_path = DATABASE_FILE
    ddl_directory = 'ddl'
    execute_ddl_scripts(database_path, ddl_directory)