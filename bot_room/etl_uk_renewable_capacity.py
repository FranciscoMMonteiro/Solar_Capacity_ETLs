# 1.Importando bibliotecas

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
import calendar
import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

print('Rodando ETL de capacidade de paineis de energia solar em UK')

# 2. Extract - Webscraping site do governo ingles


url = 'https://www.gov.uk/government/statistics/solar-photovoltaics-deployment'
response = requests.get(url)

soup = bs(response.text, features = 'lxml')

dict_month = {
    'january':'01', 'february':'02', 'march':'03',
    'april':'04', 'may':'05', 'june':'06',
    'july':'07', 'august':'08', 'september':'09',
    'october':'10', 'november':'11', 'december':'12'
}

folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_uk_solar_capac'

pre_existing_files = os.listdir(folder_path)
max_date = datetime.datetime(1,1,1)
for file in pre_existing_files:
    file_year =  int(file.split('.')[0].split('_')[-2])
    file_month = int(file.split('.')[0].split('_')[-1])
    file_date = datetime.datetime(file_year,file_month,1)
    if file_date > max_date:
        max_date = file_date
        last_file_name = file

for row in soup.find_all('a', href = True):
    if 'solar' in row.text.lower() and 'excel' in row.text.lower():
        file_name_i = row.get('href').split('/')[-1]
        #file_name_i = file_name_i.split('\\')[-1].split('.')[0]
        year_i = file_name_i.split('.')[-2].split('_')[-1]
        month_str_i = file_name_i.split('.')[-2].split('_')[-2].lower()
        month_i = dict_month[month_str_i]
        new_file_name_i = 'uk_pv_capac_' + year_i + '_' + month_i + '.xlsx'
        save_path_i = folder_path + "\\" +new_file_name_i 

        response_i = requests.get(row.get('href'))
        if new_file_name_i in pre_existing_files and new_file_name_i != last_file_name:
            #print('Já existe um arquivo salvo com esse nome.')
            None
        else:
            with open(save_path_i,'wb') as file_i:
                print(f'Baixando o arquivo {new_file_name_i}')
                file_i.write(response_i.content)

# 3. Transform - Lendo e manipulando dados


folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_uk_solar_capac'
files = os.listdir(folder_path)
files_paths = []
for file_i in files:
    file_path_i = folder_path + '\\' + file_i
    files_paths.append(file_path_i)

dict_df = {}
for file_i in files_paths:
    file_name_i = file_i.split('\\')[-1].split('.')[0]
    month_i = file_name_i.split('_')[-1]
    year_i = file_name_i.split('_')[-2]
    date_i = f'{year_i}-{month_i}-1'
    excel_file_i = pd.ExcelFile(file_i)
    sheet_names_i = excel_file_i.sheet_names
    if 'Table_1_by_Capacity' in sheet_names_i:
        print(f'Lendo o arquivo {file_name_i}.')
        df_i = pd.read_excel(excel_file_i, sheet_name='Table_1_by_Capacity')
        dict_df[date_i] = df_i


list_keys = list(dict_df.keys())

i = 0 
list_keys = list(dict_df.keys())
while i < len(list_keys):
    for col in dict_df[list_keys[i]].columns:
        col = str(col)
        if 'unnamed:' in col.lower() or col.lower() in ['nan','na','none']:
            delete_row = True
        else:
            delete_row = False
    if delete_row:
        new_header_i = dict_df[list_keys[i]].iloc[0]
        dict_df[list_keys[i]] = dict_df[list_keys[i]][1:]
        dict_df[list_keys[i]].columns = list(new_header_i)
    else:
        i = i + 1



for key in dict_df.keys():
    df_header = pd.DataFrame([dict_df[key].columns], columns=dict_df[key].columns)
    dict_df[key] = pd.concat([df_header,dict_df[key]])
    dict_df[key].reset_index(inplace = True , drop = True)


def split_dataframe(df, split_values):
    # Find indices where the split_value occurs
    split_indices = df.index[df[df.columns[0]].isin(split_values)].tolist()
    
    # Add the end index of the DataFrame to the split indices
    split_indices.append(len(df)-1)

    # Find the split values
    split_values_at_index = []
    for index in split_indices:
        split_values_at_index.append(df.iloc[index, 0])
    
    # Split the DataFrame
    dfs = []
    for i in range(len(split_indices)-1):
        start = split_indices[i]
        end = split_indices[i+1]
        if i+2 == len(split_indices):
            part_df = df.iloc[start:]
        else:
            part_df = df.iloc[start:end]
        dfs.append(part_df)
    
    return dfs , split_values_at_index[:-1]


split_values_1 = ['CUMULATIVE COUNT','CUMULATIVE CAPACITY (MW) [note 1]']
split_values_2 = ['GB','UK', 'NI']


df_list , split_value_list  = split_dataframe(dict_df[key], split_values_1)


split_values_1 = ['CUMULATIVE COUNT','CUMULATIVE CAPACITY (MW) [note 1]']
split_values_2 = ['GB','UK', 'NI']


for key in dict_df.keys():
    df_list , split_value_list  = split_dataframe(dict_df[key], split_values_1)
    for i in range(len(df_list)):
        df_list[i]['measure'] = split_value_list[i]
        df_list[i] = df_list[i].iloc[1:].copy()
    df_concat = pd.concat(df_list)
    dict_df[key] = df_concat.reset_index(drop = True)



df_list , split_value_list  = split_dataframe(dict_df[key], split_values_2)


for key in dict_df.keys():
    df_list , split_value_list  = split_dataframe(dict_df[key], split_values_2)
    for i in range(len(df_list)):
        df_list[i]['place'] = split_value_list[i]
        df_list[i] = df_list[i].iloc[1:].copy()
    df_concat = pd.concat(df_list)
    dict_df[key] = df_concat.reset_index(drop = True)


month_mapping = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}


for key in dict_df.keys():
    dict_df[key] = dict_df[key].rename(columns = {'CUMULATIVE CAPACITY (MW) [note 1]':'capacity_bracket'})
    dict_df[key] = dict_df[key].melt(id_vars = ['capacity_bracket','measure','place'])
    dict_df[key]['variable'] = dict_df[key]['variable'].str.replace('\n','/')
    dict_df[key]['month'] = dict_df[key]['variable'].str.lower().str.split('/').str[0].str.strip().str[:3]
    dict_df[key]['year'] = dict_df[key]['variable'].str.lower().str.split('/').str[1].str.strip()
    dict_df[key]['month'] = dict_df[key].reset_index()['month'].replace(month_mapping)
    dict_df[key] = dict_df[key].drop(columns = ['variable'])
    dict_df[key] = dict_df[key].pivot_table(values = 'value',index = ['capacity_bracket','place','year','month'] , columns = 'measure')
    dict_df[key] = dict_df[key].reset_index()
    dict_df[key] = dict_df[key].rename(columns = {'CUMULATIVE CAPACITY (MW) [note 1]':'cumulative_capacity','CUMULATIVE COUNT':'cumulative_count'})
    dict_df[key]['extraction_date'] = key
    dict_df[key]['extraction_date'] = pd.to_datetime(dict_df[key]['extraction_date'])


df_concat = pd.DataFrame()
for key in dict_df.keys():
    df_concat = pd.concat([df_concat,dict_df[key]])


df_concat['date'] = df_concat['year'].astype(str)+ '/' + df_concat['month'].astype(str) + '/' + '01'
df_concat['date'] = pd.to_datetime(df_concat['date'])


def last_day_of_month(date):
    # Get the year and month from the provided date
    year = date.year
    month = date.month
    
    # Find the last day of the month
    _, last_day = calendar.monthrange(year, month)
    
    # Return the last day of the month as a datetime object
    return datetime.datetime(year, month, last_day)


df_concat['date'] = df_concat['date'].apply(lambda x:last_day_of_month(x))


# 4. Load - Importando para o Banco de Dados


list_dict_upload = []
for index, row in df_concat.iterrows():
    dict_i = {
        '_id':{
            'date':row['date'],
            'extraction_date':row['extraction_date'],
            'capacity_bracket':row['capacity_bracket'],
            'place':row['place']
            
        },
        'capacity_mw':row['cumulative_capacity'],
        'count_pv':row['cumulative_count']
    }
    list_dict_upload.append(dict_i)



tipo_bd = 'PROD'
mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
uk_solar_capac = mdb.client['gestao']['energy.uk_solar_capac']
mongo.bulk_update(uk_solar_capac,list_dict_upload)
mdb.client.close()

print('Script rodou com sucesso!')
