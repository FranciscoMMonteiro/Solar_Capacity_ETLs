# 1. Importando Bibliotecas

import sys
import os
user = os.getlogin()
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib")
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib\\atmlib")

import requests
import pandas as pd
import json
import pymongo
import mongo # type: ignore
import MongoDB
import calendar
import datetime

print('Rodando ETL de capacidade de paineis de energia solar na Alemanha')

# # 2. Extract (API)

params = {
    'country':'de',
    'time_step':'monthly',
    'installation_decomission':'false'
}

api_url = 'https://api.energy-charts.info/installed_power?'
response = requests.get(api_url, params=params)

json_response = response.json()

dict_data = {}
for data in json_response['production_types']:
    dict_data[data['name']] = data['data']

dict_data['date'] = json_response['time']

df = pd.DataFrame(dict_data)

# 3. Trasnform (Limpando dados)

df = df[['date' , 'Biomass', 'Wind offshore', 'Wind onshore', 'Solar',
       'Battery Storage (Power)', 'Battery Storage (Capacity)']]

df = df.rename(columns={
    'Biomass':'biomass_gw',
    'Wind offshore':'wind_offshore_gw',
    'Wind onshore':'wind_onshore_gw',
    'Solar':'solar_capacity_gw',
    'Battery Storage (Power)':'battery_storage_pwr_gwh',
    'Battery Storage (Capacity)':'battery_storage_capacity_gw'
    })

df['date'] = pd.to_datetime(df['date'], format = '%m.%Y')

def last_day_of_month(date):
    # Get the year and month from the provided date
    year = date.year
    month = date.month
    
    # Find the last day of the month
    _, last_day = calendar.monthrange(year, month)
    
    # Return the last day of the month as a datetime object
    return datetime.datetime(year, month, last_day)

df['date'] = df['date'].apply(lambda x: last_day_of_month(x))

# 4. Load (upload para banco de dados MongoDB)

list_dict_upload = []
for index, row in df.iterrows():
    dict_i = {
        '_id':{
            'date':row['date'],
        },
        'solar_capacity_gw':row['solar_capacity_gw'],
        'wind_onshore_gw':row['wind_onshore_gw'],
        'wind_offshore_gw':row['wind_offshore_gw'],
        'biomass_gw':row['biomass_gw'],
        'battery_storage_capacity_gw':row['battery_storage_capacity_gw'],
        'battery_storage_pwr_gwh':row['battery_storage_pwr_gwh']
    }
    list_dict_upload.append(dict_i)

tipo_bd = "PROD"
mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
renew_germ_capacity = mdb.client['gestao']['energy.renew_germ_capac']
mongo.bulk_update(renew_germ_capacity,list_dict_upload)
mdb.client.close()
print('Script runned with success!')