import pandas as pd
import requests
import os
import zipfile

years = ["1992", "2001", "2011"]
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

estban_all = []

for year in years:
    for month in months:
        
        url = f"https://www.bcb.gov.br/content/estatisticas/estatistica_bancaria_estban/municipio/{year}{month}_ESTBAN.ZIP"
        response = requests.get(url)
        
        if response.status_code == 200:
            os.makedirs(f"estban/{year}", exist_ok=True)
            filename = f"estban/{year}/{year}{month}_ESTBAN.ZIP"
            with open(filename, "wb") as file:
                file.write(response.content)
            #print(f"Arquivo salvo: {filename}")
            
            # Extraindo o ZIP
            with zipfile.ZipFile(filename, "r") as zip_ref:
                zip_ref.extractall(f"estban/{year}")
            #print(f"Arquivo extraído em: estban/{year}")
        else:
            print(f"Erro ao baixar: {url}")
            
        estban = pd.read_csv(f"estban/{year}/{year}{month}_ESTBAN.CSV", encoding="latin1", sep=";", decimal=",", skiprows=2)
        estban["dt_year"] = estban["#DATA_BASE"].astype(str).str[0:4]
        estban_all.append(estban)
        
estban_full = pd.concat(estban_all)
estban_groupby = estban_full.groupby(["dt_year", "UF", "MUNICIPIO", "CODMUN_IBGE"], as_index=False).mean(numeric_only=True)
estban_groupby.to_parquet("estban/refined/estban_refined.gzip", compression="gzip")