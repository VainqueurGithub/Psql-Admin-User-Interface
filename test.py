import enum
import subprocess
import statistics
import csv
from pathlib import Path
import importlib
import tkinter as tk
from tkinter import ttk
from turtle import bgcolor
from unittest import TestResult
from tkinter import messagebox, filedialog
from tkinter import PhotoImage
import time
import threading
import random
import geopandas as gpd
import os
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use("TkAgg") # for backend
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from shapely.geometry import Point
from pyproj import CRS, Transformer
import seaborn as sns
sns.set_theme(style="darkgrid")

import psycopg2
import tkinter as tk
from tkinter import messagebox

# Database connection details

def connect_db():
    DB_PARAMS = {
    "dbname": database_entry.get(),
    "user": user_entry.get(),
    "password": password_entry.get(),
    "host": host_entry.get(),
    "port": "5432"
    }

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        populate_table(conn)
        return conn
    except Exception as e:
        messagebox.showerror("Database Connection Fail", str(e))


def fetch_schemas(conn):
    """Fetch all schema names from the PostgreSQL database."""
    try:
       
        cur = conn.cursor()
        
        # Query to get schemas
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schemas = cur.fetchall()
        
        # Close connection
        #cur.close()
        #conn.close()
        
        return [schema[0] for schema in schemas]

    except Exception as e:
        print("Error:", e)
        return []

def fetch_tables(schema_name):
    """Fetch table names for a selected schema."""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = %s;
        """, (schema_name,))
        
        tables = cursor.fetchall()
        
        #cursor.close()
        #conn.close()
        return [table[0] for table in tables]
    
    except Exception as e:
        print("Error:", e)
        return []

def fetch_records(schema_name, table_name):
    """Fetch first 1000 records from a selected table."""
    try:
        conn  = connect_db()
        cursor = conn.cursor()
        
        # Fetch first 1000 records
        cursor.execute(f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT 1000;')
        records = cursor.fetchall()
        
        # Get column names
        col_names = [desc[0] for desc in cursor.description]
        
        cursor.close()
        conn.close()
        return col_names, records
    
    except Exception as e:
        print("Error:", e)
        return [], []


def populate_table(conn):
    """Populate Tkinter Treeview with schema names."""
    schemas = fetch_schemas(conn)
    
    # Clear existing data
    for row in tree_schemas.get_children():
        tree_schemas.delete(row)

    # Insert new schema names
    for schema in schemas:
        tree_schemas.insert("", "end", values=(schema,))

def on_schema_click(event):
    """Fetch and display tables when a schema is clicked."""
    selected_item = tree_schemas.selection()
    if selected_item:
        schema_name = tree_schemas.item(selected_item, "values")[0]
        
        tables = fetch_tables(schema_name)
        
        # Clear old data
        for row in tree_tables.get_children():
            tree_tables.delete(row)
        
        # Insert new tables
        for table in tables:
            tree_tables.insert("", "end", values=(table,schema_name))

def on_table_click(event):
    """Fetch and display the first 1000 records when a table is clicked."""
    selected_item = tree_tables.selection()
    if selected_item:
        values = tree_tables.item(selected_item, "values")

        if len(values) == 2:
            schema_name, table_name = values
            col_names, records = fetch_records(table_name,schema_name)
        
            # Clear old records
            for row in tree_records.get_children():
                tree_records.delete(row)

            # Update column headings
            tree_records["columns"] = col_names
            for col in col_names:
                tree_records.heading(col, text=col)
                tree_records.column(col, anchor="center")

            # Insert records
            for record in records:
                tree_records.insert("", "end", values=record)

# root window
root = tk.Tk()
root.geometry("1600x820")
root.title('Database administration interface')
#root.resizable(0, 0)

# configure the grid
root.columnconfigure(0, weight=1)
root.columnconfigure(1, weight=3)
root.columnconfigure(2, weight=1)


# Label to display the status

database_label = ttk.Label(root, text="Database:")
database_label.place(relx=0.03, rely=0.03)

database_entry = ttk.Entry(root, width=15)
database_entry.place(relx=0.085, rely=0.03)
database_entry.insert(0, 'dianfossey')

user_label = ttk.Label(root, text="User:")
user_label.place(relx=0.18, rely=0.03)

user_entry = ttk.Entry(root, width=15)
user_entry.place(relx=0.21, rely=0.03)
user_entry.insert(0, 'user')


password_label = ttk.Label(root, text="Password:")
password_label.place(relx=0.31, rely=0.03)

password_entry = ttk.Entry(root, width=15)
password_entry.place(relx=0.37, rely=0.03)
password_entry.insert(0, 'fgfgfhdhfhjdjd')

host_label = ttk.Label(root, text="Host:")
host_label.place(relx=0.47, rely=0.03)

host_entry = ttk.Entry(root, width=15)
host_entry.place(relx=0.5, rely=0.03)
host_entry.insert(0, 'localhost')


mergin_button = ttk.Button(root, text="Connect to server",width=20, command=connect_db)
mergin_button.place(relx=0.6, rely=0.03)


# Separator object
separator = ttk.Separator(root, orient='horizontal')
separator.place(relx=0, rely=0.1, relwidth=1, relheight=1)


# Frame for schemas
frame_schemas = ttk.LabelFrame(root, text="Schemas")
frame_schemas.pack(fill="both", expand=True, padx=10, pady=5)

# Scrollbar for schemas
scrollbar_schemas = ttk.Scrollbar(frame_schemas, orient="vertical")
tree_schemas = ttk.Treeview(frame_schemas, columns=("Schema Name",), show="headings", yscrollcommand=scrollbar_schemas.set)
scrollbar_schemas.config(command=tree_schemas.yview)

tree_schemas.heading("Schema Name", text="Schema Name")
tree_schemas.pack(side="left", fill="both", expand=True)
scrollbar_schemas.pack(side="right", fill="y")

tree_schemas.bind("<<TreeviewSelect>>", on_schema_click)

# Frame for tables
frame_tables = ttk.LabelFrame(root, text="Tables")
frame_tables.pack(fill="both", expand=True, padx=10, pady=5)

# Scrollbar for tables
scrollbar_tables = ttk.Scrollbar(frame_tables, orient="vertical")
tree_tables = ttk.Treeview(frame_tables, columns=("Schema", "Table Name"), show="headings", yscrollcommand=scrollbar_tables.set)
scrollbar_tables.config(command=tree_tables.yview)

tree_tables.heading("Schema", text="Schema")
tree_tables.heading("Table Name", text="Table Name")
tree_tables.pack(side="left", fill="both", expand=True)
scrollbar_tables.pack(side="right", fill="y")

tree_tables.bind("<<TreeviewSelect>>", on_table_click)

# Frame for records
frame_records = ttk.LabelFrame(root, text="First 1000 Records")
frame_records.pack(fill="both", expand=True, padx=10, pady=5)

# Scrollbar for records
scrollbar_records = ttk.Scrollbar(frame_records, orient="vertical")
tree_records = ttk.Treeview(frame_records, show="headings", yscrollcommand=scrollbar_records.set)
scrollbar_records.config(command=tree_records.yview)

tree_records.pack(side="left", fill="both", expand=True)
scrollbar_records.pack(side="right", fill="y")

root.mainloop()





