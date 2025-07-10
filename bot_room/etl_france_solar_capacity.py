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

print('Rodando elt de capacidade de paineis solares da França.')

# 2. Extracting (webscraping)

raiz_url = 'https://analysesetdonnees.rte-france.com'
url = 'https://analysesetdonnees.rte-france.com/en/generation/solar'
response = requests.get(url)

soup = bs(response.text, 'html.parser')
soup.find_all('a', href = True)


list_links = []
for html_line in soup.find_all('a', href = True):
    if 'csv' in str(html_line):
        #print(html_line['href'])
        list_links.append(html_line['href'])


def clean_name(name_string,find, replace):
    stop = False
    if find in name_string:
        name_string = name_string.replace(find,replace)
        stop = True
    
    return name_string , stop

folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_france_solar_capac'
find_replace_list = [
    ('solar','solar_generation'),
    ('parc','solar_capacity'),
    ('capacity','solar_capacity_factor'),
    ('share','solar_energy_share')
    ]
pre_existing_files = os.listdir(folder_path)

for link in list_links:
    url_link = raiz_url+link
    #print(url_link)
    response = requests.get(url_link)
    link.split('/')[-1].split()
    data_atualizacao = link.split('/')[-1].split('_')[-2].replace('-','')
    file_name = link.split('/')[-1].split('_')[0].lower()
    file_extension = link.split('.')[-1]
    for find,replace in find_replace_list:
        file_name , stop = clean_name(file_name,find,replace)
        if stop:
            break
    file_name = file_name + '_' + data_atualizacao + '.' +file_extension
    save_path = folder_path + '\\' + file_name
    #print(save_path)
    if file_name in pre_existing_files:
        #print('Já existe um arquivo salvo com esse nome.')
        None
    else:
        with open(save_path,'wb') as file:
            print(f'Baixando o arquivo {file_name}')
            file.write(response.content)

# 3. Trasnforming (limpando e manipulando os dados)

folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_france_solar_capac'
list_files = os.listdir(folder_path)
dict_df = {}
for file in list_files:
    file_path = folder_path+ '\\' + file 
    #print(file_path)
    df_i = pd.read_csv(file_path, sep= ';')
    dict_df[file] = df_i

def ajust_date(date_string):
    date_string = date_string.lower()
    if 't' in date_string:
        year = int(date_string.split('-')[-1])
        quarter = int(date_string.split('-')[0].replace('t',''))
        month = quarter*3
        date = datetime.datetime(year,month,1)
    elif '-' in date_string:
        first_str = date_string.split('-')[0]
        second_str = date_string.split('-')[1]
        if len(first_str) ==4:
            year = first_str
            month = second_str
        else:
            month = first_str
            year = second_str
        date = datetime.datetime(int(year), int(month), 1)
    else:
        year = int(date_string)
        date = datetime.datetime(year, 1 ,1)
    return date


def remove_special_caracters(s):
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return s


df_concat = pd.DataFrame()
for key in dict_df.keys():
    dict_df[key].columns = [col.lower().split(' ')[0] for col in dict_df[key].columns]
    new_columns = []
    for col in dict_df[key].columns:
        new_columns.append(remove_special_caracters(col))
    dict_df[key].columns = new_columns
    dict_df[key]['date'] = dict_df[key]['date'].apply(lambda row :ajust_date(row))
    #print(dict_df[key]['date'].unique())
    df_concat = pd.concat([df_concat,dict_df[key]])


df_concat['filiere'] = df_concat['filiere'].astype(str).str.lower().apply(lambda row:remove_special_caracters(row))


df_concat['filiere'] = df_concat['filiere'].str.strip().str.replace(' ','_')


df_concat = df_concat.drop_duplicates(subset = ['date','valeur'])


df_concat['valeur'] = df_concat['valeur'].str.replace(',','.').astype(float)

df_concat = df_concat[df_concat['date'].dt.year >= 2014]

df_concat['filiere'].unique()

def define_units(filiere):
    if filiere in ['nouvelles_capacites' , 'parc_installe']:
        return 'gw'
    elif filiere in ['maximum_capacity_factor','average_capacity_factor','maximum_share_of_demand','average_share_of_demand']:
        return '%'
    elif filiere in ['solar_power_generation']:
        return 'twh'
    else:
        return "couldn't define unit"


df_concat['units'] = df_concat['filiere'].apply(lambda row: define_units(row))


df_concat = df_concat.rename(columns = {'filiere':'measure_type','valeur':'value'})


df_concat = df_concat[df_concat['measure_type'].isin(['solar_power_generation','average_share_of_demand','parc_installe','average_capacity_factor'])]


df_concat['measure_type'] = df_concat['measure_type'].replace({'parc_installe':'installed_capacity'})


df_calc = df_concat.pivot_table(index = 'date', values = 'value' , columns = 'measure_type').reset_index()


df_calc = df_calc[['date','solar_power_generation','average_capacity_factor']]


def days_in_month(date):
    year = date.year
    month = date.month
    
    _, num_days = calendar.monthrange(year, month)
    
    return num_days


df_calc['n_days'] = df_calc['date'].apply(lambda row:days_in_month(row))


df_calc['calc__installed_capacity'] = df_calc['solar_power_generation']/(df_calc['average_capacity_factor']/100)/24/df_calc['n_days']*1000


df_calc = df_calc[['date','calc__installed_capacity']]
df_calc['measure_type'] = 'calc__installed_capacity'
df_calc = df_calc.rename(columns = {'calc__installed_capacity':'value'})
df_calc['units'] = 'gw'


df_concat = pd.concat([df_concat, df_calc])


df_concat = df_concat.dropna(subset = 'value')


df_concat['measure_type'] = df_concat['measure_type'] + '_' + df_concat['units'] 


df_concat = df_concat.pivot_table(columns = 'measure_type' , values = 'value' , index ='date').reset_index()

def last_day_of_month(date):
    # Get the year and month from the provided date
    year = date.year
    month = date.month
    
    # Find the last day of the month
    _, last_day = calendar.monthrange(year, month)
    
    # Return the last day of the month as a datetime object
    return datetime.datetime(year, month, last_day)


# ajusting date to the end of the month
df_concat['date'] = df_concat['date'].apply(lambda x: last_day_of_month(x))


# 4. Loading (importando para o banco de dados)


list_dict_upload = []
for index, row in df_concat.iterrows():
    dict_i = {
        '_id':{
            'date':row['date']
        },
        'average_capacity_factor_%':row['average_capacity_factor_%'],
        'average_share_of_demand_%':row['average_share_of_demand_%'],
        'calc__installed_capacity_gw':row['calc__installed_capacity_gw'],
        'installed_capacity_gw':row['installed_capacity_gw'],
        'solar_power_generation_twh':row['solar_power_generation_twh']
    }
    list_dict_upload.append(dict_i)


list_dict_upload


tipo_bd = 'PROD'
mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
france_solar_capac = mdb.client['gestao']['energy.fr_solar_capac']
mongo.bulk_update(france_solar_capac,list_dict_upload)
mdb.client.close()

print('Script rodou com sucesso!')

