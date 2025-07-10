# 1. Importando bibliotecas

import sys
import os
user = os.getlogin()

sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib")
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib\\atmlib")

import MongoDB
import mongo
import pandas as pd
import requests
import datetime
from bs4 import BeautifulSoup as bs
import json
import numpy as np
import unicodedata
import calendar
import xlwings as xw
import calendar
import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

print('Rodando ETL de capacidade de paineis solares de Portugal.')

# 2. Extraction (webscraping)


url = 'https://www.dgeg.gov.pt/pt/estatistica/energia/publicacoes/estatisticas-rapidas-das-renovaveis/'
url_raiz = 'https://www.dgeg.gov.pt'
response = requests.get(url)


soup = bs(response.text ,features = 'lxml')


soup.find_all('a' ,href = True , title = True)
folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_portugal_capac'
pre_existing_files = os.listdir(folder_path)


for row_a in soup.find_all('a' ,href = True , title = True):
    if '.xlsx' in row_a['href']:
        title = row_a['title'].replace('-',' ')
        month = title.split()[-1]
        year = title.split()[-2]
        date_str = str(year) + str(month)
        file_name = 'portugal_dgeg_renew_energy' + date_str + '.xlsx'
        save_path = folder_path + '\\' + file_name
        url_file = row_a['href']
        response = requests.get(url_raiz + url_file)
        if file_name in pre_existing_files:
            #print('Já existe um arquivo salvo com esse nome.')
            None
        else:
            with open(save_path,'wb') as file:
                print(f'Baixando o arquivo {file_name}')
                file.write(response.content)
    


# 3. Transform (limpando e manipulando dados)

folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_portugal_capac'
list_files = os.listdir(folder_path)
df_concat = pd.DataFrame()

for file in list_files:
    file_path = folder_path + '\\' + file
    df = pd.read_excel(file_path, sheet_name='Quadros')
    first_row = df.index[df['Unnamed: 2'].astype(str).str.contains('Quadro 4')][0]
    df = df.loc[first_row:]
    last_row = df.index[df['Unnamed: 2'].isna()][0] - 1
    df = df.loc[:last_row]
    df = df.reset_index(drop = True)
    df = df.dropna(axis=1, how='all')
    df.columns = df.iloc[1]
    df = df.iloc[2:]
    df = df.rename(columns= {df.columns[0]:'energy_type'})
    df['energy_type'] = df['energy_type'].str.lower()
    solar_row = df.index[df['energy_type'] == 'fotovoltaica'][0]
    df_solar = df.loc[solar_row+1:].reset_index(drop=True)
    df_melt = df_solar.melt(id_vars='energy_type', var_name='date', value_name='capacity_mw')
    month = int(file.split('.')[0][-2:])
    year = int(file.split('.')[0][-6:-2])
    df_melt['file_date'] = datetime.datetime(year,month,1)
    df_melt['capacity_mw'] = df_melt['capacity_mw'].replace('-',0).astype(float).copy()
    df_concat = pd.concat([df_concat,df_melt])
    
    

def check_date_filedate(row):
        return row['date'] == row['file_date'].year



df_concat['file_date'].dt.year

df_concat['is_month'] = df_concat.apply(lambda row: check_date_filedate(row), axis = 1)

df_concat['last_file_date'] = df_concat['file_date'].max()


def find_date(row):
    if row['is_month']:
        return row['file_date']
    else:
        return datetime.datetime(row['date']+1,1,1)


df_concat['real_date'] = df_concat.apply(lambda row: find_date(row), axis =1 )

df_concat = df_concat.sort_values(by=['real_date','is_month'], ascending= False).drop_duplicates(subset = ['real_date','energy_type'])

df_concat = df_concat[['real_date','energy_type','capacity_mw']].rename(columns = {'real_date':'date'})

def last_day_of_month(date):
    # Get the year and month from the provided date
    year = date.year
    month = date.month
    
    # Find the last day of the month
    _, last_day = calendar.monthrange(year, month)
    
    # Return the last day of the month as a datetime object
    return datetime.datetime(year, month, last_day)

df_concat['date'] = df_concat['date'].apply(lambda x: last_day_of_month(x))


# 4. Load (upload para o banco de dados) 


list_dict_upload = []
for index, row in df_concat.iterrows():
    dict_i = {
        '_id':{
            'date':row['date'],
            'tech':row['energy_type'],
        },
        'capacity_mw':row['capacity_mw']
    }
    list_dict_upload.append(dict_i)


tipo_bd = 'PROD'
mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
portugal_solar_capac = mdb.client['gestao']['energy.portugal_solar_capac']
mongo.bulk_update(portugal_solar_capac,list_dict_upload)
mdb.client.close()

print('Script rodou com sucesso!')