import csv
import psycopg2
from psycopg2 import OperationalError
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
from tkinter import messagebox

def mon_create_engine(database,user,password,host,port):
    try:
        # Attempt to connect to the database
        engine = create_engine('postgresql+psycopg2://'+user+':'+password+'@'+host+':'+port+'/'+database)
        return engine
    except OperationalError as e:
        # Handle connection failure
        if "password authentication failed" in str(e):
            messagebox.showerror("OperationalError:", f"Password authentication failed for user '{user}'.Please check your username and password.")
        else:
            messagebox.showerror("OperationalError:", e)

        # Optional: retry logic or prompt for a new password here
        return None

def mon_connect_to_db(database,user,password,host,port):
    try:
        # Attempt to connect to the database
        connection = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,  # Replace this with the actual password
            host=host,
            port=port
        )
        messagebox.showinfo("Confirmation", "Connection successful!")
        return connection
    except OperationalError as e:
        # Handle connection failure
        if "password authentication failed" in str(e):
            messagebox.showerror("OperationalError:" f"Password authentication failed for user '{user}'. Please check your username and password.")
        else:
            messagebox.showerror("OperationalError:", e)
        # Optional: retry logic or prompt for a new password here
        return None


def mon_read_csv(source_raw):
    # Open the file and read the first line to detect the delimiter
    with open(source_raw, "r") as csvfile:
        # Read the file's content
        sample = csvfile.readline()
        
        # Use Sniffer to detect the delimiter
        dialect = csv.Sniffer().sniff(sample)
        delimiter = dialect.delimiter
    
    # Read the data to pandas dataframe by assigning the correct delimiter

    if delimiter==',':
        data = pd.read_csv(source_raw, sep = ',', encoding = 'latin1')
    elif delimiter==';':
        data = pd.read_csv(source_raw, sep = ';', encoding = 'latin1')
    return data

def raw_monitoring_data_checking(data):
    
    ''' This function check the validity of data from csv file. Most importantly the date format and nombre column'''

    # Formating date_surveillance column to '%d/%m/%Y' format
    try:
        # Try parsing with the first format
        data['date_surveillance'] = pd.to_datetime(data.date_surveillance)
        messagebox.showinfo("Confirmation","date parsed succeffully")
    except ValueError:
        # Provide a helpful message and suggest the correct format
        messagebox.showerror("Error Message",f"Make sure date_surveillance '{data['date_surveillance']}' column is parsed to this format '%d/%m/%Y'.")
    
        
    #Cleaning nombre column     
        
    try:
        # Try to convert the value directly to an integer
        data['nombre'] = data['nombre'].fillna(0) # Assign 0 to all nan value in this column
        data['nombre'] = data['nombre'].astype(int) # Convert this column to int 
        data['nombre'] = data['nombre'].astype(str) # Convert this column to string
        messagebox.showinfo("nombre Converted succeffully")
    except ValueError:
        messagebox.showerror("Error Message", f"Make sure '{data['nombre']}'is numeric characters.")
    
    return data
        
def mon_retrieve_data_psql(database,user,password,host,port):
    
    '''This function connect python to psql and retrieve data from psql'''
    #establishing the connection
    try:
        conn = mon_connect_to_db(database=database, user=user, password=password, host=host, port=port)
        #Setting auto commit false
        conn.autocommit = True
        #Creating a cursor object using the cursor() method
        cursor_espece = conn.cursor()
        cursor_signe = conn.cursor()
        cursor_equipe = conn.cursor()
        cursor_nombre = conn.cursor()
        cursor_age = conn.cursor()
        cursor_chef_equipe = conn.cursor()
    
        #Retrieving data
        cursor_espece.execute('''SELECT nom_espece from prog_gorille.espece''')
        cursor_signe.execute('''SELECT valeur from prog_gorille.signes''')
        cursor_equipe.execute('''SELECT nom_equipe from prog_gorille.equipe_surveillance''')
        cursor_nombre.execute('''SELECT valeur from prog_gorille.nombre''')
        cursor_age.execute('''SELECT valeur from prog_gorille.age''')
        cursor_chef_equipe.execute('''SELECT num_pisteur from prog_gorille.pisteur''')
    
        #Fetching rows from the table
        especes = cursor_espece.fetchall();
        signes = cursor_signe.fetchall();
        equipes = cursor_equipe.fetchall();
        nombres = cursor_nombre.fetchall();
        ages = cursor_age.fetchall();
        chef_equipes = cursor_chef_equipe.fetchall();
    
        #Commit your changes in the database
        conn.commit()
        #Closing the connection
        conn.close()
    
        #Creating single dataframe for each cursor, and add nan value if neccessary
        df_espece = pd.DataFrame(especes, columns=['espece'])
        df_signe = pd.DataFrame(signes, columns=['signe'])
        df_signe2 = pd.DataFrame([[np.nan]], columns=['signe'])
        df_signe = pd.concat([df_signe,df_signe2], ignore_index=True)

        df_nombre = pd.DataFrame(nombres, columns=['nombre'])
        df_nombre2 = pd.DataFrame([[np.nan]], columns=['nombre'])
        df_nombre = pd.concat([df_nombre,df_nombre2], ignore_index=True)

        df_equipe = pd.DataFrame(equipes, columns=['equipe'])
        df_age = pd.DataFrame(ages, columns=['age'])
        df_age2 = pd.DataFrame([[np.nan]], columns=['age'])
        df_age = pd.concat([df_age,df_age2], ignore_index=True)

        df_chef_equipe = pd.DataFrame(chef_equipes, columns=['chef_equipe'])
    
        return df_espece, df_signe, df_nombre, df_equipe, df_age, df_chef_equipe
    except AttributeError:
        df_espece = pd.DataFrame(columns=['espece'])
        df_signe = pd.DataFrame(columns=['signe'])
        df_nombre = pd.DataFrame(columns=['nombre'])
        df_equipe = pd.DataFrame(columns=['equipe'])
        df_age = pd.DataFrame(columns=['age'])
        df_chef_equipe = pd.DataFrame(columns=['chef_equipe'])
        return df_espece, df_signe, df_nombre, df_equipe, df_age, df_chef_equipe


def mon_checking_data_integrity(source_raw,source_checked,df_espece, df_signe, df_nombre, df_equipe, df_age, df_chef_equipe,data):
    ''' This function check data integrity before downloading the data into psql.'''
    
    data_success = data.loc[(data['observation'].isin(df_espece['espece'])) & (data['signe'].isin(df_signe['signe'])) &
        (data['equipe'].isin(df_equipe['equipe'])) & (data['age_jours'].isin(df_age['age'])) & 
         (data['chef_equipe'].isin(df_chef_equipe['chef_equipe'])) &
         (data['nombre'].isin(df_nombre['nombre']))]
    data_success
    
    
    
    data_fail = data.loc[(~data['observation'].isin(df_espece['espece'])) | (~data['signe'].isin(df_signe['signe'])) |
        (~data['equipe'].isin(df_equipe['equipe'])) | (~data['age_jours'].isin(df_age['age'])) | 
         (~data['chef_equipe'].isin(df_chef_equipe['chef_equipe'])) | (~data['nombre'].isin(df_nombre['nombre']))]

    
    # Check if the data_success_surveillance CSV file exists
    if os.path.exists(source_checked+'/data_success_surveillance.csv'):
                      # Read the existing CSV file
                      existing_df = pd.read_csv(source_checked+'/data_success_surveillance.csv')
                      # Merge the new DataFrame with the existing DataFrame
                      combined_df = pd.concat([existing_df, data_success], ignore_index=True)
    else:
        # If the file does not exist, use the new DataFrame as the combined DataFrame
        combined_df = data_success

    
    try:
        data_fail.to_csv(source_raw, index=False)
        # Write the combined DataFrame back to the CSV file
        combined_df.to_csv(source_checked+'/data_success_surveillance.csv', index=False)
        
        if len(data_fail)==0:
            messagebox.showinfo("Success Massage",'ALL YOUR DATA IS VALIDATED, READY TO BE INTEGRETED INTO PSQL')
        
        else:
            messagebox.showwarning("Warning Message",f"YOU STILL HAVE SOME DATA TO VALIDATE, '{len(data_fail)}' raw seem to have issues check your data_fail_surveillance.csv file.")
        
    except PermissionError:
        messagebox.showerror("Error Message","The script fail to write data on the file, make sure both data_fail_surveillance and data_success_surveillance csv files are not opened")
    
            
    #return message

def clear_csv_but_keep_headers(file_path):
    #Deletes all data lines but keeps the header in a CSV file.
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader, None) # Read the header

    if headers:
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers) # Write only the header back    

def mon_data_downloading_psql(source_checked, user,password,host,port,database):
    
    engine = mon_create_engine(database,user,password,host,port)

    try:
        data_success = pd.read_csv(source_checked, encoding = 'latin1')
        data_success.to_sql('surveillance', engine, schema='prog_gorille',if_exists='append', index=False)
        clear_csv_but_keep_headers(source_checked)
        messagebox.showinfo("Confirmation",f"{len(data_success)} data, successfully loaded into psql server")
    except Exception as e:
        messagebox.showerror("Error Importing data into psql server",str(e))


    
