import sys
import os
user = os.getlogin()
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib")
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib\\atmlib")

import pymongo
import mongo
import MongoDB
import pandas as pd
import datetime
import numpy as np
import time
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import calendar

print('Rodando ETL de capacidade de paineis de energia solar nos EUA (large-scale) ')

# 2. Baixando arquivos

user = os.getlogin()

url = "https://www.eia.gov/electricity/data/eia860m"
request = requests.get(url)

soup = BeautifulSoup(request.text, 'html.parser')


base_url = "https://www.eia.gov"
list_url_download = []
for item in soup.find_all("a"):
    if item.get('href') and item.get('title'):
        if 'EIA 860' in item.get('title'):
            list_url_download.append((base_url+item['href']))


folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\elt_eia_utility_scale'
folder_archives = os.listdir(folder_path)
folder_archives2 = folder_archives[1:]
max_date = datetime.datetime(1,1,1)
for archive in folder_archives2:
    ano = archive.split('.')[0].split('-')[0][-4:]
    ano = int(ano)
    mes = archive.split('.')[0].split('-')[1]
    mes = int(mes)
    date_archive = datetime.datetime(ano,mes,1)
    if date_archive > max_date:
        max_date = date_archive
    

for link in list_url_download:
    file_name = link.split('/')[-1]
    year = file_name.split('.')[0][-4:]
    month = file_name.split('_')[0]
    if month != 'energy':
        month_to_number = {'january': '01', 'february': '02', 'march': '03', 'april': '04',
        'may': '05', 'june': '06', 'july': '07', 'august': '08',
        'september': '09', 'october': '10', 'november': '11', 'december': '12'}
        month_n = month_to_number[month]
        file_name = file_name.replace(month,'')
        file_name = file_name.replace("_","")
        file_name = file_name.replace(year,str(year+'-'+month_n))
    else:
        month = 1
        year = 1
    save_path = f'{folder_path}\\{file_name}'
    response_i = requests.get(link)
    if file_name in folder_archives and datetime.datetime(int(year),int(month_n),1)!= max_date:
        #print('Já existe um arquivo salvo com esse nome.')
        None
    else:
        with open(save_path, 'wb') as file:
            print(f'Salvando o arquivo {file_name}.')
            file.write(response_i.content)
    time.sleep(0.1)

# 3.Importando arquivos

folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\elt_eia_utility_scale'

files_paths = []
for file_name_i in os.listdir(folder_path):
    if 'energy_source_codes' not in file_name_i:
        files_paths.append(os.path.join(folder_path,file_name_i))


dict_df = {}
i = 0
for file_i in tqdm(files_paths):
    name_file_i = file_i.split("\\")[-1]
    #print(f'Reading file: {name_file_i}')
    try:
        excel_file_i = pd.ExcelFile(file_i)
    except:
        try:
            excel_file_i = pd.ExcelFile(file_i, engine='openpyxl')
        except:
            excel_file_i = pd.ExcelFile(file_i, engine='xlrd')
    sheet_names_i = excel_file_i.sheet_names
    for sheet_name_j in sheet_names_i:
        if "operating" in sheet_name_j.lower():
            df_i = pd.read_excel(excel_file_i , sheet_name= sheet_name_j)
            break
    #df_i = df_i.melt(id_vars=[list(df_i.columns)[x] for x in range (0,6)])
    year_month = name_file_i.split('.')[0][-7:]
    dict_df[f'{year_month}'] = df_i


i = 0 
list_keys = list(dict_df.keys())
while i < len(list_keys):
    if 'Entity ID' in dict_df[list_keys[i]].columns:
        i = i + 1
        continue
    new_header_i = dict_df[list_keys[i]].iloc[0]
    dict_df[list_keys[i]] = dict_df[list_keys[i]][1:]
    dict_df[list_keys[i]].columns = list(new_header_i)
    if not pd.isna(dict_df[list_keys[i]].columns[0]):
        i = i + 1


#for key in list_keys:
#    print(key)
#    print(dict_df[key].columns)

rename_columns = {
    'sector_name':'sector'
}


for key in dict_df.keys():
    #print(key)
    new_columns = []
    for col in dict_df[key].columns:
        new_columns.append(col.lower().strip().replace(" ","_"))
    dict_df[key].columns = new_columns
    dict_df[key].rename(columns = rename_columns , inplace = True)
    dict_df[key]['month'] = key.split('-')[1]
    dict_df[key]['year'] = key.split('-')[0]
    dict_df[key] = dict_df[key][['entity_id','entity_name','year','month','plant_id','plant_name','sector','plant_state','generator_id','energy_source_code','technology','status','net_summer_capacity_(mw)']]

    
df = pd.concat([dict_df[key] for key in dict_df.keys()])

df['entity_id'] = pd.to_numeric(df['entity_id'], errors = 'coerce')

df['net_summer_capacity_(mw)'] = pd.to_numeric(df['net_summer_capacity_(mw)'], errors = 'coerce')

df = df.dropna(subset='entity_id')

df['date'] = df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str) + '-1'
df['date'] = pd.to_datetime(df['date'], format = '%Y-%m-%d')

def last_day_of_month(date):
    # Get the year and month from the provided date
    year = date.year
    month = date.month
    
    # Find the last day of the month
    _, last_day = calendar.monthrange(year, month)
    
    # Return the last day of the month as a datetime object
    return datetime.datetime(year, month, last_day)

df['date'] = df['date'].apply(lambda x: last_day_of_month(x))

df = df.dropna(subset = 'net_summer_capacity_(mw)')
df = df[df['net_summer_capacity_(mw)'] != 0 ]
df = df[df['energy_source_code']=='SUN']
df = df[df['status']=='(OP) Operating']

# 4. Importando para o banco

dict_list = []
for index,row in df.iterrows():
    dict_i = {
        '_id':{
            'id_date':row['date'] ,
            'entity_id':row['entity_id'] ,
            'entity_name':row['entity_name'] ,
            'plant_id':row['plant_id'] ,
            'plant_name':row['plant_name'] ,
            'sector':row['sector'] ,
            'plant_state':row['plant_state'],
            'generator_id':row['generator_id']
        },
        'energy_source_code':row['energy_source_code'],
        'technology':row['technology'],
        'status':row['status'],
        'net_summer_capacity_(mw)':row['net_summer_capacity_(mw)']
    }
    dict_list.append(dict_i)

def divide_list(big_list,n_divisions):
    """Divide a list in n parts"""
    n_itens = len(big_list)//n_divisions
    list_smaller_lists = []
    for i in range(n_divisions-1):
        smaller_list = big_list[i*n_itens:(i+1)*n_itens]
        list_smaller_lists.append(smaller_list)
    smaller_list = big_list[i*n_itens:]
    list_smaller_lists.append(smaller_list)

    return list_smaller_lists

lists_upload = divide_list(dict_list,50)

tipo_bd = 'PROD'
mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
eia_power_plant_solar_collection = mdb.client['gestao']['energy.eia_pwrplant_sol_capac']
try:
    for upload in lists_upload:
        mongo.bulk_update(eia_power_plant_solar_collection,upload)
except pymongo.errors.AutoReconnect as e:
    print(e)
    mdb.client.close()
mdb.client.close()


