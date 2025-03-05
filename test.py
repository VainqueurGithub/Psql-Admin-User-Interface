import psycopg2
import pandas as pd

# Database connection details
DB_CONFIG = {
    "dbname": "dianfossey",
    "user": "vainqueur",
    "password": "123",
    "host": "localhost",
    "port": "5432",
}

# Connect to PostgreSQL and fetch data
def fetch_data(query):
    """Fetch data from a PostgreSQL table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql(query, conn)  # Read into Pandas DataFrame
        conn.close()
        return df
    except Exception as e:
        print("Error:", e)
        return None

# Example Query (Modify based on your table structure)
query = "SELECT observation, signe, easting FROM prog_gorille.surveillance"
df = fetch_data(query)

if df is not None:
    # Pivot the table: Rows = 'category', Columns = 'sub_category', Values = 'sales'
    pivot_table = df.pivot_table(index="observation", columns="signe", values="easting", aggfunc="sum")

    # Show pivot table
    print(pivot_table)

    # Save as CSV
    pivot_table.to_csv("C:/Users/vainq/PG_GUI/pivot_table.csv")
    print("Pivot table saved as 'pivot_table.csv'.")
