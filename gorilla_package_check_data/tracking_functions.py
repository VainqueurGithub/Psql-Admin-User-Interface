
import csv
import psycopg2
from psycopg2 import OperationalError
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
from tkinter import messagebox
from urllib.parse import quote_plus


def tr_create_engine(database,user,password,host,port):
    try:
        # Attempt to connect to the database
        encoded_password = quote_plus(password)
        encoded_user = quote_plus(user)
        engine = create_engine(f'postgresql+psycopg2://{encoded_user}:{encoded_password}@{host}:{port}/{database}')
        return engine
    except OperationalError as e:
        # Handle connection failure
        if "password authentication failed" in str(e):
            messagebox.showerror("OperationalError:", f"Password authentication failed for user '{user}'.Please check your username and password.")
        else:
            messagebox.showerror("OperationalError:", e)

        # Optional: retry logic or prompt for a new password here
        return None

def tr_connect_to_db(database,user,password,host,port):
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


def tr_read_csv(source_raw):
    # Open the file and read the first line to detect the delimiter
    with open(source_raw+'/data_fail_pistage.csv', "r") as csvfile:
        # Read the file's content
        sample = csvfile.readline()
        
        # Use Sniffer to detect the delimiter
        dialect = csv.Sniffer().sniff(sample)
        delimiter = dialect.delimiter
    
    # Read the data to pandas dataframe by assigning the correct delimiter

    if delimiter==',':
        data = pd.read_csv(source_raw+'/data_fail_pistage.csv', sep = ',', encoding = 'latin1')
    elif delimiter==';':
        data = pd.read_csv(source_raw+'/data_fail_pistage.csv', sep = ';', encoding = 'latin1')
    return data

def raw_tracking_data_checking(data):
    
    ''' This function check the validity of data from csv file. Most importantly the date format and nombre column'''

    # Formating date_date_carnet column to '%d/%m/%Y' format
    try:
        # Try parsing with the first format
        data['date_carnet'] = pd.to_datetime(data.date_carnet)
        messagebox.showinfo("Confirmation","date parsed succeffully")
    except ValueError:
        # Provide a helpful message and suggest the correct format
        messagebox.showerror("Error Message",f"Make sure date_carnet '{data['date_carnet']}' column is parsed to this format '%d/%m/%Y'.")
    
        
    #Cleaning nid_arboricole column     
    data['nid_arboricoles']=data['nid_arboricoles'].fillna(0)
    data['nid_terrestres']=data['nid_terrestres'].fillna(0)  
    data['famille_gorille']=data['famille_gorille'].str.rstrip()
    # Try to convert the value directly to an integer
    try:
        data['nid_arboricoles']=data['nid_arboricoles'].astype(int)
        try:
            data['nid_terrestres']=data['nid_terrestres'].astype(int)
            try:
                data['nombre'] = data['nombre'].fillna(0)
                #data['nombre'] = data['nombre'].astype(int)
                data['nombre'] = data['nombre'].astype(str)
                try:
                    data['age_jours'] = data['age_jours'].fillna(0)
                    #data['age_jours'] = data['age_jours'].astype(int)
                    data['age_jours'] = data['age_jours'].astype(str)
                    messagebox.showinfo("nombre Converted succeffully")
                except ValueError:
                    messagebox.showerror("Error Message",f"Make sure '{data['age_jours']}'in age_jours column is numeric characters.")
            except ValueError:
                messagebox.showerror("Error Message",f"Make sure '{data['nombre']}'in nombre column is numeric characters.")
        except ValueError:
            messagebox.showerror("Error Message",f"Make sure '{data['nid_terrestres']}'in nid_terrestres column is numeric characters.")
    except ValueError:
        messagebox.showerror("Error Message",f"Make sure '{data['nid_arboricoles']}'in nid_arboricoles column is numeric characters.")
    return data
        
def tr_retrieve_data_psql(database,user,password,host,port):
    
    '''This function connect python to psql and retrieve data from psql'''
    #establishing the connection
    try:
        conn = tr_connect_to_db(database=database, user=user, password=password, host=host, port=port)
        #Setting auto commit false
        conn.autocommit = True
        #Creating a cursor object using the cursor() method
        cursor_espece = conn.cursor()
        cursor_signe = conn.cursor()
        cursor_type = conn.cursor()
        cursor_plante_consommee = conn.cursor()
        cursor_partie_consomme = conn.cursor()
        cursor_nombre = conn.cursor()
        cursor_foret = conn.cursor()
        cursor_age = conn.cursor()
        cursor_chef_equipe = conn.cursor()
        cursor_famille_gorille = conn.cursor()
    
        #Retrieving data
        cursor_espece.execute('''SELECT nom_espece from prog_gorille.observation''')
        cursor_signe.execute('''SELECT valeur from prog_gorille.signes''')
        cursor_type.execute('''SELECT valeur from prog_gorille.type''')
        cursor_partie_consomme.execute('''SELECT valeur from prog_gorille.partie_consommee''')
        cursor_nombre.execute('''SELECT valeur from prog_gorille.nombre''')
        cursor_foret.execute('''SELECT foret from prog_gorille.foret''')
        cursor_age.execute('''SELECT valeur from prog_gorille.age''')
        cursor_chef_equipe.execute('''SELECT num_pisteur from prog_gorille.pisteur''')
        cursor_famille_gorille.execute('''SELECT nom_famille from prog_gorille.famille_gorille''')
        cursor_plante_consommee.execute('''SELECT nom_espece from prog_biodiversite.espece_arbre''')
    
        #Fetching rows from the table
        especes = cursor_espece.fetchall();
        signes = cursor_signe.fetchall();
        types = cursor_type.fetchall();
        partie_consommes = cursor_partie_consomme.fetchall();
        nombres = cursor_nombre.fetchall();
        forets = cursor_foret.fetchall();
        ages = cursor_age.fetchall();
        chef_equipes = cursor_chef_equipe.fetchall();
        famille_gorilles = cursor_famille_gorille.fetchall();
        plante_consommees = cursor_plante_consommee.fetchall(); 
    
        #Commit your changes in the database
        conn.commit()
        #Closing the connection
        conn.close()
    
        #Creating single dataframe for each cursor, and add nan value if neccessary
        df_espece = pd.DataFrame(especes, columns=['espece'])
        df_signe = pd.DataFrame(signes, columns=['signe'])
        df_signe2 = pd.DataFrame([[np.nan]], columns=['signe'])
        df_signe = pd.concat([df_signe,df_signe2], ignore_index=True)

        df_type = pd.DataFrame(types, columns=['type'])
        df_type2 = pd.DataFrame([[np.nan]], columns=['type'])
        df_type = pd.concat([df_type,df_type2], ignore_index=True)


        df_partie_consomme = pd.DataFrame(partie_consommes, columns=['partie_consomme'])
        df_partie_consomme2 = pd.DataFrame([[np.nan]], columns=['partie_consomme'])
        df_partie_consomme = pd.concat([df_partie_consomme,df_partie_consomme2], ignore_index=True)

        df_nombre = pd.DataFrame(nombres, columns=['nombre'])
        df_nombre2 = pd.DataFrame([[np.nan]], columns=['nombre'])
        df_nombre = pd.concat([df_nombre,df_nombre2], ignore_index=True)

        df_foret = pd.DataFrame(forets, columns=['foret'])
        df_age = pd.DataFrame(ages, columns=['age'])
        df_age2 = pd.DataFrame([[np.nan]], columns=['age'])
        df_age = pd.concat([df_age,df_age2], ignore_index=True)

        df_chef_equipe = pd.DataFrame(chef_equipes, columns=['chef_equipe'])
        df_famille_gorille = pd.DataFrame(famille_gorilles, columns=['famille_gorille'])

        df_plante_consommee = pd.DataFrame(plante_consommees, columns=['nom_espece'])
        df_plante_consommee2 = pd.DataFrame([[np.nan]], columns=['nom_espece'])
        df_plante_consommee = pd.concat([df_plante_consommee,df_plante_consommee2], ignore_index=True)
    
        return df_espece, df_signe, df_nombre, df_foret, df_age, df_chef_equipe, df_type, df_partie_consomme, df_famille_gorille, df_plante_consommee
    
    except AttributeError:
        df_espece = pd.DataFrame(columns=['espece'])
        df_signe = pd.DataFrame(columns=['signe'])
        df_nombre = pd.DataFrame(columns=['nombre'])
        df_foret = pd.DataFrame(columns=['foret'])
        df_age = pd.DataFrame(columns=['age'])
        df_chef_equipe = pd.DataFrame(columns=['chef_equipe'])
        df_type = pd.DataFrame(columns=['type'])
        df_partie_consomme = pd.DataFrame(columns=['partie_consomme']) 
        df_famille_gorille = pd.DataFrame(columns=['famille_gorille'])
        df_plante_consommee = pd.DataFrame(columns=['plante_consomee'])
        
        return df_espece, df_signe, df_nombre, df_foret, df_age, df_chef_equipe, df_type, df_partie_consomme, df_famille_gorille, df_plante_consommee


def tr_checking_data_integrity(source_raw,source_checked,df_espece,df_signe,df_nombre,df_foret,df_age,df_chef_equipe,df_type,df_partie_consomme,df_famille_gorille,df_plante_consommee,data):
    ''' This function check data integrity before downloading the data into psql.'''
    
    data_success = data.loc[(data['espece'].isin(df_espece['espece'])) & (data['signe'].isin(df_signe['signe'])) &
        (data['type'].isin(df_type['type'])) & (data['partie_consommee'].isin(df_partie_consomme['partie_consomme'])) & 
         (data['foret'].isin(df_foret['foret'])) &(data['age_jours'].isin(df_age['age'])) & 
         (data['chef_equipe'].isin(df_chef_equipe['chef_equipe'])) &
         (data['famille_gorille'].isin(df_famille_gorille['famille_gorille'])) & 
                        (data['nombre'].isin(df_nombre['nombre'])) & (data['plante_consomee'].isin(df_plante_consommee['nom_espece']))]
    data_success
    
    
    
    data_fail = data.loc[(~data['espece'].isin(df_espece['espece'])) | (~data['signe'].isin(df_signe['signe'])) |
        (~data['type'].isin(df_type['type'])) | (~data['partie_consommee'].isin(df_partie_consomme['partie_consomme']))  | 
         (~data['foret'].isin(df_foret['foret'])) | (~data['age_jours'].isin(df_age['age'])) | 
         (~data['chef_equipe'].isin(df_chef_equipe['chef_equipe'])) |
         (~data['famille_gorille'].isin(df_famille_gorille['famille_gorille'])) |
                    (~data['nombre'].isin(df_nombre['nombre'])) | (~data['plante_consomee'].isin(df_plante_consommee['nom_espece']))]
    data_fail

    
    # Check if the data_success_surveillance CSV file exists
    if os.path.exists(source_checked+'/data_success_pistage.csv'):
                      # Read the existing CSV file
                      existing_df = pd.read_csv(source_checked+'/data_success_pistage.csv')
                      # Merge the new DataFrame with the existing DataFrame
                      combined_df = pd.concat([existing_df, data_success], ignore_index=True)
    else:
        # If the file does not exist, use the new DataFrame as the combined DataFrame
        combined_df = data_success

    
    try:
        data_fail.to_csv(source_raw, index=False)
        # Write the combined DataFrame back to the CSV file
        combined_df.to_csv(source_checked+'/data_success_pistage.csv', index=False)
        
        if len(data_fail)==0:
            messagebox.showinfo("Confirmation",'ALL YOUR DATA IS VALIDATED, READY TO BE INTEGRETED INTO PSQL')
        
        else:
            messagebox.showwarning("Warning Message", f"YOU STILL HAVE SOME DATA TO VALIDATE, '{len(data_fail)}' raw seem to have issues check your data_fail_pistage.csv file.")
        
    except PermissionError:
        messagebox.showerror("Error Message","The script fail to write data on the file, make sure both data_fail_pistage and data_success_pistage csv files are not opened")
    
            
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

def tr_data_downloading_psql(source_checked, user,password,host,port,database):
    
    engine = tr_create_engine(database,user,password,host,port)

    try:
        data_success = pd.read_csv(source_checked, encoding = 'latin1')
        data_success.to_sql('pistage', engine, schema='prog_gorille',if_exists='append', index=False)
        clear_csv_but_keep_headers(source_checked)
        messagebox.showinfo("Confirmation",f"{len(data_success)} data, successfully loaded into psql server")
    except Exception as e:
        messagebox.showerror("Error Importing data into psql server",str(e))
