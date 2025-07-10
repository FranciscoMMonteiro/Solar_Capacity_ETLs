import sys
import os
user = os.getlogin()
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib")
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib\\atmlib")

import mongo
import MongoDB
import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import calendar

print('Rodando ETL de capacidade de paineis de energia solar nos EUA (small-scale)')

# 2. Baixando arquivos

user = os.getlogin()

url = "https://www.eia.gov/electricity/data/eia861m/"

request = requests.get(url)

soup = BeautifulSoup(request.text, 'html.parser')

list_url_download = []
for item in soup.find_all("a"):
    if item.get('href') and item.get('title'):
        if 'Small scale' in item.get('title'):
            list_url_download.append((url+item['href']).replace('./',''))

folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_eia_small_scalar_solar'
folder_archives = os.listdir(folder_path)
ultimo_ano_disp = 0
for file_name in folder_archives:
    ano = int(file_name.split(".")[0][-4:])
    if ano >= ultimo_ano_disp:
        ultimo_ano_disp = ano
        file_name_ult_ano = file_name


for link in list_url_download:
    file_name = link.split('/')[-1]
    file_name = file_name.replace("_","")
    if file_name[:11] == "distributed":
        file_name = 'smallscale'+file_name[11:]
    save_path = f'{folder_path}\\{file_name}'
    response_i = requests.get(link)
    if file_name in folder_archives and file_name != file_name_ult_ano:
        #print(f'Já existe um arquivo {file_name} salvo com esse nome.')
        None
    else:
        with open(save_path, 'wb') as file:
            print(f'Baixando arquivo {file_name}.')
            file.write(response_i.content)

# 3.Importando arquivos

folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_eia_small_scalar_solar'

files_paths = []
for file_name_i in os.listdir(folder_path):
    files_paths.append(os.path.join(folder_path,file_name_i))

list_df = []
i = 0
for file_i in files_paths:
    #print(i)
    name_file_i = file_i.split("\\")[-1]
    print(f'Reading file: {name_file_i}')
    excel_file_i = pd.ExcelFile(file_i)
    sheet_names_i = excel_file_i.sheet_names
    for sheet_name_j in sheet_names_i:
        if "states" in sheet_name_j.lower():
            df_i = pd.read_excel(excel_file_i , sheet_name= sheet_name_j, header = [0,1], skiprows=1)
            break
    header_concat_i = []
    for header0,header1 in df_i.columns:
        if "unnamed:" in str(header0).lower():
            header_concat_j = "id_"+str(header1)
        else:
            header_concat_j = str(header0) + "_" + str(header1)
        header_concat_j = header_concat_j.lower()
        header_concat_j = header_concat_j.replace(" ","")
        header_concat_i.append(header_concat_j)
    df_i.columns = header_concat_i
    df_i = df_i.melt(id_vars=[list(df_i.columns)[x] for x in range (0,4)])
    list_df.append(df_i)
    i = i + 1

df = pd.concat(list_df)

df['id_year'] = pd.to_numeric(df['id_year'], errors = 'coerce')

df['value'] = pd.to_numeric(df['value'], errors = 'coerce')

df = df.dropna(subset='id_year')

df = df.reset_index(drop = True)

dict_padronize ={
    'charateristics':'characteristics',
    'installations':'customers',
    'customers/customers':'customers'
    }
for key in dict_padronize.keys():
    df['variable'] = df['variable'].str.replace(key,dict_padronize[key])

df = df[~df['variable'].str.contains('alltech')]
df = df[~df['variable'].str.contains('total')]

df['date'] = df['id_year'].astype(int).astype(str) + '-' + df['id_month'].astype(int).astype(str) + '-1'
df['date'] = pd.to_datetime(df['date'], format = '%Y-%m-%d')

def last_day_of_month(date):
    # Get the year and month from the provided date
    year = date.year
    month = date.month
    
    # Find the last day of the month
    _, last_day = calendar.monthrange(year, month)
    
    # Return the last day of the month as a datetime object
    return datetime.datetime(year, month, last_day)

df['date'] = df['date'].apply(lambda x:last_day_of_month(x)) 

df['variable'].unique()

dict_replace = {
    'combustionturbine_commercial':'combustionturbine(mw)_commercial',
    'combustionturbine_directconnected':'combustionturbine(mw)_directconnected',
    'combustionturbine_industrial':'batteryenergy(mwh)_industrial',
    'combustionturbine_residential':'batteryenergy(mwh)_residential',
    'combustionturbine_transportation':'batteryenergy(mwh)_transportation'
    }

df['variable'].unique()

df = df[df['variable'].isin(['capacitymw_residential','capacitymw_commercial','capacitymw_industrial'])]

df = df.dropna(subset = 'value')
df = df[df['value'] != 0 ]

# 4. Importando para o banco

dict_list = []
for index,row in df.iterrows():
    dict_i = {
        '_id':{
            'id_date':row['date'] ,
            'id_state':row['id_state'] ,
            'value_category':row['variable']
        },
        'value':row['value']
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

lists_upload = divide_list(dict_list,2)

tipo_bd = 'PROD'
mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
eia_small_scale_solar_collection = mdb.client['gestao']['energy.eia_smallscale_sol_capac']
for upload in lists_upload:
    mongo.bulk_update(eia_small_scale_solar_collection,upload)
mdb.client.close()


