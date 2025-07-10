import subprocess
import time
import os
import pandas as pd
import warnings
from pandas.errors import SettingWithCopyWarning

def run_script(script_path, env):

    command = f'activate {env} && python "{script_path}"'
    sucesso = False
    tentativas = 0
    max_tentativas = 3
    while tentativas <max_tentativas:
        try:
            subprocess.run(command, shell=True, check=True)
            #print(f'Script {script_path} rodado com sucesso!')
            print('--------------------------------------------------------------')
            tentativas = max_tentativas+1
            sucesso = True
        except subprocess.CalledProcessError as e:
            print(f'Erro ao rodar o script {script_path}: {e}')
            tentativas = tentativas + 1

    return sucesso

if __name__ == "__main__":
    warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
    warnings.simplefilter(action='ignore', category=FutureWarning)
    user = os.getlogin()
    print("Rodando scripts de ETL de capacidade de paineis solares instalados na EUR e EUA.")
    folder_path = fr"C:\Users\{user}\ATMOSPHERE\Atmosphere Capital - Capital - Market Inteligence\Python\scripts_analise_dados\etl_solar_capacity\individual_scripts"
    scripts_to_run = sorted(os.listdir(folder_path) , reverse=True)
    #scripts_to_run = ["etl_portugal_solar_capacity.py","etl_spain_solar_capacity.py","etl_uk_renewable_capacity.py",
    #                 "etl_nl_gen_cap_stl.py","etl_germany_renewable_capacity.py","etl_france_solar_capacity.py",
    #                  "eia_net_metering_solar.py", "eia_non_net_metering_solar.py", "eia_small_scale_solar.py",
    #                  'eia_utility_scale.py']
    
    env = "pyQuant_3_11"

    # Rodar cada script no ambiente especificado
    dict_scripts = {}
    for script in scripts_to_run:
        script_path = os.path.join(folder_path,script)

        print(f"Rodando script {script}")
        dict_scripts[script] = [run_script(script_path, env)]

print('--------------------------------------------------------------')
print('Verifique abaixo quais scripts foram rodados com sucesso')
df = pd.DataFrame(dict_scripts).T
df.reset_index(inplace = True)
df.rename(columns = {'index':'scrip_name',0:'rodou'}, inplace = True)
print(df)
print('')
input('Pressione ENTER para sair.')




