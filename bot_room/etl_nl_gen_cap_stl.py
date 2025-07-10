# 1. Importar bibliotecas

import sys
import os
user = os.getlogin()

sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib")
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib\\atmlib")

import numpy as np
import MongoDB
import mongo
import pandas as pd
import datetime
import numpy as np
from statsmodels.tsa.seasonal import STL
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt
import seaborn as sns
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import shutil
import re


register_matplotlib_converters()
sns.set_style("darkgrid")
plt.rc("figure", figsize=(16, 12))
plt.rc("font", size=13)

# 2. Solar generation

# 2.1 Extraction

url = 'https://opendata.cbs.nl/statline/#/CBS/en/dataset/84575ENG/table?ts=1723237113781'
driver = webdriver.Chrome()
driver.get(url)

time.sleep(5)
for elem in WebDriverWait(driver,10).until(EC.presence_of_all_elements_located((By.CLASS_NAME,'axis_0'))):
    print(elem)
    if 'Periods' in elem.text:
        filter_elem = elem.find_element(By.CLASS_NAME,'pvtAttrEditHandle')
        filter_elem.click()

time.sleep(5)
month_box = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.ID, 'ts_modal'))
)

for i,elem in enumerate(month_box.find_elements(By.TAG_NAME, 'span')):
    if 'Months' in elem.text:
        elem_year_icon =  elem
        break
month_box.find_elements(By.TAG_NAME, 'span')[i+2].click()

for el in driver.find_elements(By.ID, 'ts_modal'):
    el.find_element(By.CLASS_NAME , 'apply').click()

time.sleep(5)
driver.find_element(By.CLASS_NAME, 'fa-download').click()

for el in driver.find_elements(By.CLASS_NAME, 'DownloadCSV'):
    if 'with statistical' in el.text:
        el.click()

time.sleep(20)
driver.quit()

time.sleep(30)
dowload_folder = f'C:\\Users\\{user}\\Downloads'
os.chdir(dowload_folder)
list_downloads = sorted(filter(os.path.isfile, os.listdir('.')), key = os.path.getmtime, reverse=True)
for sourcefilename in list_downloads:
    if 'Electricity__supply_' in sourcefilename:
        source_file_path =  sourcefilename
        break


destination_folder = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_netherlands_gen'
for file in os.listdir(destination_folder):
    file_path = os.path.join(destination_folder,file)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
    except Exception as e:
        print(e)
        print(f"Couldn't delete files in {destination_folder}.")
shutil.move(source_file_path,destination_folder)

# # 2.2 Transform
folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_netherlands_gen'
file_name = os.listdir(folder_path)[0]
file_path = os.path.join(folder_path,file_name)
df_gen = pd.read_csv(file_path, dtype = str, sep = ';')

new_columns = []
for col in df_gen.columns:
    col_i = col.strip().lower().replace('_',' / ').replace('/','_').replace(' ','_')
    new_columns.append(col_i)
    df_gen[col] = df_gen[col].str.strip().replace('.',np.nan).str.lower()

df_gen.columns = new_columns

df_gen['periods'] = df_gen['periods'].str.replace('*','')

def extract_month_year(period):
    # Define month names for matching
    months = {'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'}
    
    # Regular expression to match months and years
    match = re.search(r'(\d{4})\s+(\w+)|(\w+)\s+(\d{4})', period.lower())
    if match:
        if match.group(1) and match.group(2):  # Year and month in correct order
            return match.group(2), match.group(1)
        elif match.group(3) and match.group(4):  # Month and year in inverted order
            month = match.group(3)
            year = match.group(4)
            # Ensure month is valid
            if month in months:
                return month, year
    return None, None

df_gen[['month','year']] = df_gen['periods'].apply(lambda x : pd.Series(extract_month_year(x)))
list_remove = ['2nd']

df_gen = df_gen[~df_gen['month'].isin(list_remove)]

month_mapping = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5,
    'june': 6, 'july': 7, 'august': 8, 'september': 9, 'october': 10,
    'november': 11, 'december': 12
}

df_gen['month'] = df_gen['month'].apply(lambda x: month_mapping[x])

df_gen['date'] = df_gen['year'].astype(str) + '-' + df_gen['month'].astype(str) + '-01'
df_gen['date'] = pd.to_datetime(df_gen['date'])

for col in df_gen.columns:
    if 'solar' in col:
        solar_col = col 

df_gen = df_gen[['date',solar_col]].rename(columns = {solar_col:'solar_gen_gwh'})

def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - datetime.timedelta(days=next_month.day)

df_gen['date'] = df_gen['date'].apply(lambda x: last_day_of_month(x))

df_gen = df_gen.set_index('date')

df_gen['solar_gen_gwh'] = pd.to_numeric(df_gen['solar_gen_gwh'])

df_gen

df_gen = df_gen[~df_gen['solar_gen_gwh'].isna()]

df_gen['log_gen'] = df_gen['solar_gen_gwh'].apply(lambda x:np.log(x))

df_log = df_gen['log_gen']

stl = STL(df_log, period=12,seasonal=5 , robust = True)
res = stl.fit()
fig = res.plot()

df_results = pd.DataFrame()
df_results['trend'] = res.trend
df_results['resid'] = res.resid
df_results['season'] = res.seasonal

df_results['trend_real'] = df_results['trend'].apply(lambda x:np.exp(x))

# 3. Yearly Solar Capacity

# 3.1 Extraction

url_statsline = 'https://opendata.cbs.nl/statline/#/CBS/en/dataset/82610ENG/table?ts=1723142538966'
driver = webdriver.Chrome()
driver.get(url_statsline)

time.sleep(0.5)
for elem in WebDriverWait(driver,10).until(EC.presence_of_all_elements_located((By.CLASS_NAME,'axis_2'))):
    print(elem)
    if 'Periods' in elem.text:
        filter_elem = elem.find_element(By.CLASS_NAME,'pvtAttrEditHandle')
        filter_elem.click()

time.sleep(0.5)
year_box = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.ID, 'ts_modal'))
)

for i,elem in enumerate(year_box.find_elements(By.TAG_NAME, 'span')):
    if 'Years' in elem.text:
        elem_year_icon =  elem
        break
year_box.find_elements(By.TAG_NAME, 'span')[i+2].click()

time.sleep(2)
for el in driver.find_elements(By.ID, 'ts_modal'):
    el.find_element(By.CLASS_NAME , 'apply').click()

time.sleep(5)
driver.find_element(By.CLASS_NAME, 'fa-download').click()

for el in driver.find_elements(By.CLASS_NAME, 'DownloadCSV'):
    if 'with statistical' in el.text:
        el.click()

dowload_folder = f'C:\\Users\\{user}\\Downloads'
os.chdir(dowload_folder)
list_downloads = sorted(filter(os.path.isfile, os.listdir('.')), key = os.path.getmtime, reverse=True)
for sourcefilename in list_downloads:
    if 'Renewable_electricity_' in sourcefilename:
        source_file_path = sourcefilename
        break

destination_folder = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_netherlands_capac'
for file in os.listdir(destination_folder):
    file_path = os.path.join(destination_folder,file)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
    except Exception as e:
        print(e)
        print(f"Couldn't delete files in {destination_folder}.")
shutil.move(source_file_path,destination_folder)

time.sleep(10)
driver.quit()

# 3.2. Transform

folder_path = f'C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Capital - Market Inteligence\\Data Analysis\\etl_capacidade_solar\\etl_energy_netherlands_capac'
file_name = os.listdir(folder_path)[0]
file_path = os.path.join(folder_path, file_name)

df_cap = pd.read_csv(file_path,dtype = str, sep = ';')

new_columns = []
for col in df_cap.columns:
    col_i = col.strip().lower().replace('_',' / ').replace('/','_').replace(' ','_')
    new_columns.append(col_i)
    df_cap[col] = df_cap[col].str.strip().replace('.',np.nan)

df_cap.columns = new_columns

df_cap= df_cap[['energy_sources___techniques','periods','installed_installations_electrical_capacity_end_of_year_(megawatt)']]

df_cap = df_cap.rename(
    columns = {
        df_cap.columns[0]:'source',
        df_cap.columns[2]:'capacity'
        }
    )

df_cap['source']= df_cap['source'].str.replace(' ','_').str.lower()

df_cap['source'].unique()

df_cap = df_cap[df_cap['source']=='solar_photovoltaic'] 
df_cap['capacity'] = pd.to_numeric(df_cap['capacity'])

df_cap['periods'] = df_cap['periods'].str.replace('*','')

df_cap = df_cap.rename(columns = {'capacity':'capacity_mw','periods':'date'})

df_cap['date'] = df_cap['date'].astype(str) + '-12-31' 

df_cap['date'] = pd.to_datetime(df_cap['date'])

df_cap['capacity_mwh'] = pd.to_numeric(df_cap['capacity_mw'])

# 4. Calculating monthly capacity

tipo_bd = 'PROD'
mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
df = df_results[['trend_real']].merge(df_cap, on ='date', how = 'left')
df['multiply'] = df['capacity_mw']/df['trend_real']
df_multiply = df[['date','multiply']].dropna()

df['multiply_rol'] = df['multiply'].rolling(window=25, min_periods = 1).mean()

df['cap_calc'] = df['multiply_rol']*df['trend_real']

df = df[['date','cap_calc','capacity_mw']].rename(columns = {'cap_calc':'solar_cap_calc_mw','capacity_mw':'solar_cap_real_mw'}).dropna(subset = 'solar_cap_calc_mw')

list_dict_upload = []
for i,row in df.iterrows():
    dict_i = {
        '_id':row['date'],
        'solar_cap_real_mw':round(row['solar_cap_real_mw'],0),
        'solar_cap_calc_mw':round(row['solar_cap_calc_mw'],2)
    }

    list_dict_upload.append(dict_i)

for i in range(len(list_dict_upload)):
    list_keys = list(list_dict_upload[i].keys())
    for j in range(len(list_keys)):
        if isinstance(list_dict_upload[i][list_keys[j]],float):
            if np.isnan(list_dict_upload[i][list_keys[j]]):
                list_dict_upload[i].pop(list_keys[j])
                j = j -1

nl_solar_cap_collection = mdb.client['gestao']['energy.nl_calc_solar_cap']
mongo.bulk_update(nl_solar_cap_collection,list_dict_upload)
mdb.client.close()
print('Script rodou com sucesso!')

