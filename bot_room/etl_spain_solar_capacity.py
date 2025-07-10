
# 1. Importando biblioteca

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
import calendar

print('Rodando ETL de capacidade de paineis solares da Espanha.')

# 2. Extraction (API)
id = 'ea0042931-evolucion-mensual-de-energia-electrica-a-partir-de-las-fuentes-de-energia-renovables-cogeneracion-y-residuos-en-espana-energia-vendida-y-potencia-instalada-segun-tecnologia1'
#id = 'ea0042931-evolucion-mensual-de-energia-electrica-a-partir-de-las-fuentes-de-energia-renovables-cogeneracion-y-residuos-en-espana-energia-vendida-y-potencia-instalada-segun-tecnologia'
url_api = f'http://datos.gob.es/apidata/catalog/distribution/dataset/{id}'


response = requests.get(url_api)

for item in response.json()['result']['items']:
    if item['accessURL'].split('.')[-1] == 'json':
        download_url = item['accessURL']

response_json = requests.get(download_url).json()


# 3. Transform (limpando e manipulando dados)


df = pd.DataFrame(response_json)

df = df[df['tecnologia'].str.lower().str.contains('solar')].reset_index(drop = True)

df = df.rename(columns = {
    'fecha':'date',
    'tecnologia':'tech',
    'energia_vendida':'sold_energy',
    'potencia_instalada':'installed_capacity_mw',
    'n_instalaciones':'n_installed'
    })


def last_day_of_month(date):
    # Get the year and month from the provided date
    year = date.year
    month = date.month
    
    # Find the last day of the month
    _, last_day = calendar.monthrange(year, month)
    
    # Return the last day of the month as a datetime object
    return datetime.datetime(year, month, last_day)

df['date'] = pd.to_datetime(df['date'])
df['date'] = df['date'].apply(lambda x: last_day_of_month(x))

df['tech'] = df['tech'].str.lower().str.replace(' ','_')

# 4. Load (uploado para banco de dados)

list_dict_upload = []
for index, row in df.iterrows():
    dict_i = {
        '_id':{
            'date':row['date'],
            'tech':row['tech'],
        },
        'installed_capacity_mw':row['installed_capacity_mw'],
        'n_installed':row['n_installed']
    }
    list_dict_upload.append(dict_i)


tipo_bd = 'PROD'
mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
spain_solar_capac = mdb.client['gestao']['energy.spain_solar_capac']
mongo.bulk_update(spain_solar_capac,list_dict_upload)
mdb.client.close()

print('Script rodou com sucesso!')

