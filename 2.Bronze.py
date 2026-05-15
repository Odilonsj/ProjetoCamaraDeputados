# Databricks notebook source
import requests as req
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json
import os

# COMMAND ----------

spark = SparkSession.builder\
    .appName("Camada_Bronze") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "128m") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true")\
    .getOrCreate()

caminho = "/Volumes/workspace/projeto_tiller/bronze"

# COMMAND ----------


#Primeiro endpoint Deputados
url = "https://dadosabertos.camara.leg.br/api/v2/deputados"
response = req.get(url, headers={"Accept": "application/json"})

if response.status_code == 200:
    data = response.json()
else:
    print(f"Error: {response.status_code}")

df = pd.json_normalize(data['dados'])
df_deputados = spark.createDataFrame(df)
df_deputados.write.mode("overwrite").parquet(caminho + "/deputados")



# COMMAND ----------

#Segundo endpoint Partidos

url = "https://dadosabertos.camara.leg.br/api/v2/partidos"
partidos = []
pagina = 1

while True:
    response = req.get(f"{url}?pagina={pagina}", headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        if not data['dados']:
            break
        partidos.extend(data['dados'])
        if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
            break
        pagina += 1
    else:
        print(f"Error: {response.status_code}")
        break

df = pd.json_normalize(partidos)
df_partidos = spark.createDataFrame(df)
df_partidos.write.mode("overwrite").parquet(caminho + "/partidos")

# COMMAND ----------

#Terceiro endpoint legislaturas

url = "https://dadosabertos.camara.leg.br/api/v2/legislaturas"
legislaturas = []
pagina = 1

while True:
    response = req.get(f"{url}?pagina={pagina}", headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        if not data['dados']:
            break
        legislaturas.extend(data['dados'])
        if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
            break
        pagina += 1
    else:
        print(f"Error: {response.status_code}")
        break

df = pd.json_normalize(legislaturas)
df_legislaturas = spark.createDataFrame(df)
df_legislaturas.write.mode("overwrite").parquet(caminho + "/legislaturas")

# COMMAND ----------

#Quarto endpoint frentes_parlamentares
# Não possui dados tão completos quanto os arquivos de frentes parlamentares. Por isso optei por baixar os aqruivos json das frentes parlamentares

url = "https://dadosabertos.camara.leg.br/api/v2/frentes"
frentes = []
pagina = 1

while True:
    response = req.get(f"{url}?pagina={pagina}", headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        if not data['dados']:
            break
        frentes.extend(data['dados'])
        if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
            break
        pagina += 1
    else:
        print(f"Error: {response.status_code}")
        break

df = pd.json_normalize(frentes)
df_frentes_parlamentares = spark.createDataFrame(df)
#df_frentes_parlamentares.write.mode("overwrite").parquet(caminho + "/frentes_parlamentares")

# COMMAND ----------

#Frentes Parlamentares
frentes = []
caminho_frentes_parlamentares = "/Volumes/workspace/projeto_tiller/bronze/frentes_parlamentares/parquet"
caminho_json = "/Volumes/workspace/projeto_tiller/bronze/frentes_parlamentares/json"

for file_name in os.listdir(caminho_json):
    if file_name.endswith(".json"):
        file_path = os.path.join(caminho_json, file_name)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if 'dados' in data:
                    frentes.extend(data['dados'])
        except Exception as e:
            print(f"Erro ao ler arquivo {file_name}: {e}")

if frentes:
    df = pd.json_normalize(frentes)
    df_frentes = spark.createDataFrame(df)
    df_frentes.write.mode("overwrite").parquet(caminho_frentes_parlamentares)
else:
    print("Nenhuma frente foi carregada. Verifique se os arquivos JSON existem no caminho especificado.")

# COMMAND ----------

#Quinto endpoint frentes_deputados

url = "https://dadosabertos.camara.leg.br/api/v2/frentes"
deputados_frentes = []
pagina = 1

while True:
    response = req.get(f"{url}?pagina={pagina}", headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        if not data['dados']:
            break
        for frente in data['dados']:
            id_frente = frente['id']
            pagina_dep = 1
            while True:
                url_deputados = f"https://dadosabertos.camara.leg.br/api/v2/frentes/{id_frente}/deputados?pagina={pagina_dep}"
                resp_dep = req.get(url_deputados, headers={"Accept": "application/json"})
                if resp_dep.status_code == 200:
                    data_dep = resp_dep.json()
                    if not data_dep['dados']:
                        break
                    for deputado in data_dep['dados']:
                        deputado['id_frente'] = id_frente
                        deputados_frentes.append(deputado)
                    if not data_dep.get('links') or not any(link['rel'] == 'next' for link in data_dep['links']):
                        break
                    pagina_dep += 1
                else:
                    print(f"Error: {resp_dep.status_code}")
                    break
        if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
            break
        pagina += 1
    else:
        print(f"Error: {response.status_code}")
        break

df = pd.json_normalize(deputados_frentes)
df_frentes_deputados = spark.createDataFrame(df)
#df_frentes_deputados.write.mode("overwrite").parquet(caminho + "/frentes_deputados")

# COMMAND ----------

#Frentes dos Deputados
frentes = []
caminho_frentes_deputados = "/Volumes/workspace/projeto_tiller/bronze/frentes_deputados/parquet"
caminho_json = "/Volumes/workspace/projeto_tiller/bronze/frentes_deputados/json"

for file_name in os.listdir(caminho_json):
    if file_name.endswith(".json"):
        file_path = os.path.join(caminho_json, file_name)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if 'dados' in data:
                    frentes.extend(data['dados'])
        except Exception as e:
            print(f"Erro ao ler arquivo {file_name}: {e}")

if frentes:
    df = pd.json_normalize(frentes)
    df_frentes = spark.createDataFrame(df)  
    df_frentes.write.mode("overwrite").parquet(caminho_frentes_deputados)
else:
    print("Nenhuma frentes foi carregada. Verifique se os arquivos JSON existem no caminho especificado.")

# COMMAND ----------

#Sexto endpoint blocos

url = "https://dadosabertos.camara.leg.br/api/v2/blocos"
response = req.get(url, headers={"Accept": "application/json"})

if response.status_code == 200:
    data = response.json()
else:
    print(f"Error: {response.status_code}")

df = pd.json_normalize(data['dados'])
df_blocos = spark.createDataFrame(df)
df_blocos.write.mode("overwrite").parquet(caminho + "/blocos")



# COMMAND ----------

#Sétimo endpoint orgaos

url = "https://dadosabertos.camara.leg.br/api/v2/orgaos"
orgaos = []
pagina = 1

while True:
    response = req.get(f"{url}?pagina={pagina}", headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        if not data['dados']:
            break
        orgaos.extend(data['dados'])
        if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
            break
        pagina += 1
    else:
        print(f"Error: {response.status_code}")
        break

df = pd.json_normalize(orgaos)
df_orgaos = spark.createDataFrame(df)
df_orgaos.write.mode("overwrite").parquet(caminho + "/orgaos")

# COMMAND ----------

# #Oitavo endpoint Orgãos Deputados

url_deputados = "https://dadosabertos.camara.leg.br/api/v2/deputados"
response = req.get(url_deputados, headers={"Accept": "application/json"})
deputados = response.json()['dados']

deputados_orgaos = []
for deputado in deputados:
    id_deputado = deputado['id']
    pagina = 1
    while True:
        url_orgaos = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{id_deputado}/orgaos?pagina={pagina}"
        resp = req.get(url_orgaos, headers={"Accept": "application/json"})
        if resp.status_code == 200:
            data = resp.json()
            if not data['dados']:
                break
            for orgao in data['dados']:
                orgao['id_deputado'] = id_deputado
                orgao['nome_deputado'] = deputado.get('nome')
                deputados_orgaos.append(orgao)
            if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
                break
            pagina += 1
        else:
            print(f"Erro ao buscar órgãos para deputado {id_deputado}: {resp.status_code}")
            break

df = pd.json_normalize(deputados_orgaos)
df_deputados_orgaos = spark.createDataFrame(df)
df_deputados_orgaos.write.mode("overwrite").parquet(caminho + "/deputados_orgaos")

# COMMAND ----------

#Nono endpoint proposições

ano_atual = datetime.datetime.now().year
anos = list(range(ano_atual - 7, ano_atual + 1))
proposicoes = []

for ano in anos:
    url = f"https://dadosabertos.camara.leg.br/api/v2/proposicoes?ano={ano}&formato=json"
    response = req.get(url, headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        if 'dados' in data and data['dados']:
            proposicoes.extend(data['dados'])
        else:
            print(f"Nenhum dado encontrado para o ano {ano}.")
    else:
        print(f"Erro ao fazer a requisição para o ano {ano}. Status code: {response.status_code}")

if proposicoes:
    df = pd.json_normalize(proposicoes)
    df_proposicoes = spark.createDataFrame(df)
   #df_proposicoes.write.mode("overwrite").parquet(caminho + "/proposicoes")
else:
    print("Nenhuma proposição foi carregada. Verifique se a API está disponível.")

# COMMAND ----------

# DBTITLE 1,Cell 9
#Proposições

ano_atual = datetime.datetime.now().year
anos = list(range(ano_atual - 7, ano_atual + 1))
proposicoes = []
caminho_proposicoes = "/Volumes/workspace/projeto_tiller/bronze/proposicoes/parquet"

for ano in anos:
    file_path = f"/Volumes/workspace/projeto_tiller/bronze/proposicoes/json/proposicoes-{ano}.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if 'dados' in data:
                proposicoes.extend(data['dados'])
    except Exception as e:
        print(f"Erro ao ler arquivo para o ano {ano}: {e}")

if proposicoes:
    df = pd.json_normalize(proposicoes)
    df_proposicoes = spark.createDataFrame(df)
    df_proposicoes.write.mode("overwrite").parquet(caminho_proposicoes)
else:
    print("Nenhuma proposição foi carregada. Verifique se os arquivos JSON existem no caminho especificado.")

# COMMAND ----------

# Proposições Temas

ano_atual = datetime.datetime.now().year
anos = list(range(ano_atual - 7, ano_atual + 1))
proposicoes_temas = []
caminho_proposicoes_temas = "/Volumes/workspace/projeto_tiller/bronze/proposicoes_temas/parquet"

for ano in anos:
    file_path = f"/Volumes/workspace/projeto_tiller/bronze/proposicoes_temas/json/proposicoesTemas-{ano}.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if 'dados' in data:
                proposicoes_temas.extend(data['dados'])
    except Exception as e:
        print(f"Erro ao ler arquivo para o ano {ano}: {e}")

if proposicoes_temas:
    df = pd.json_normalize(proposicoes_temas)
    df_proposicoes_temas = spark.createDataFrame(df)
    df_proposicoes_temas.write.mode("overwrite").parquet(caminho_proposicoes_temas)
else:
    print("Nenhum tema de proposição foi carregado. Verifique se os arquivos JSON existem no caminho especificado.")

# COMMAND ----------

#Nono endpoint votações

ano_atual = datetime.datetime.now().year
anos = list(range(ano_atual - 7, ano_atual + 1))
votacoes = []

for ano in anos:
    url = f"https://dadosabertos.camara.leg.br/api/v2/votacoes?ano={ano}&formato=json"
    response = req.get(url, headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        if 'dados' in data and data['dados']:
            votacoes.extend(data['dados'])
        else:
            print(f"Nenhum dado encontrado para o ano {ano}.")
    else:
        print(f"Erro ao fazer a requisição para o ano {ano}. Status code: {response.status_code}")

if votacoes:
    df = pd.json_normalize(votacoes)
    df_votacoes = spark.createDataFrame(df)
    #df_votacoes.write.mode("overwrite").parquet(caminho + "/votacoes")
else:
    print("Nenhuma votação foi carregada. Verifique se a API está disponível.")

# COMMAND ----------

#Votações 
ano_atual = datetime.datetime.now().year
anos = list(range(ano_atual - 7, ano_atual + 1))
votacoes = []
caminho_votacoes = "/Volumes/workspace/projeto_tiller/bronze/votacoes/parquet"

for ano in anos:
    file_path = f"/Volumes/workspace/projeto_tiller/bronze/votacoes/json/votacoes-{ano}.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if 'dados' in data:
                votacoes.extend(data['dados'])
    except Exception as e:
        print(f"Erro ao ler arquivo para o ano {ano}: {e}")

if votacoes:
    df = pd.json_normalize(votacoes)
    df_votacoes = spark.createDataFrame(df)
    df_votacoes.write.mode("overwrite").parquet(caminho_votacoes)
else:
    print("Nenhuma votação foi carregada. Verifique se os arquivos JSON existem no caminho especificado.")

# COMMAND ----------

# DBTITLE 1,Cell 12
#Décimo endpoint eventos

url = "https://dadosabertos.camara.leg.br/api/v2/eventos"
eventos = []
pagina = 1

while True:
    response = req.get(f"{url}?pagina={pagina}&ordem=ASC&ordenarPor=dataHoraInicio", headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        if not data['dados']:
            break
        eventos.extend(data['dados'])
        if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
            break
        pagina += 1
    else:
        print(f"Error: {response.status_code}")
        break

if eventos:
    df = pd.json_normalize(eventos)
    df_eventos = spark.createDataFrame(df)

    # Drop columns with VOID type (unsupported by Parquet)
    for col_name, col_type in df_eventos.dtypes:
        if col_type == 'void':
            df_eventos = df_eventos.drop(col_name)
    df_eventos.write.mode("overwrite").parquet(caminho + "/eventos")
else:
    print("Nenhum evento foi carregado. Verifique se a API está disponível ou se os parâmetros estão corretos.")

# COMMAND ----------

# Ler arquivos JSON do volume eventos e carregar em um DataFrame

eventos = []
caminho_eventos_json = "/Volumes/workspace/projeto_tiller/bronze/eventos/json"
caminho_eventos_parquet = "/Volumes/workspace/projeto_tiller/bronze/eventos/parquet"

for file_name in os.listdir(caminho_eventos_json):
    if file_name.endswith(".json"):
        file_path = os.path.join(caminho_eventos_json, file_name)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if 'dados' in data:
                    eventos.extend(data['dados'])
        except Exception as e:
            print(f"Erro ao ler arquivo {file_name}: {e}")

if eventos:
    df = pd.json_normalize(eventos)
    df_eventos = spark.createDataFrame(df)
    df_eventos.write.mode("overwrite").option("overwriteschema", "true").parquet(caminho_eventos_parquet)
else:
    print("Nenhum evento foi carregado. Verifique se os arquivos JSON existem no caminho especificado.")

# COMMAND ----------

# DBTITLE 1,Cell 21
# #Oitavo endpoint Eventos por Deputado

url_deputados = "https://dadosabertos.camara.leg.br/api/v2/deputados"
response = req.get(url_deputados, headers={"Accept": "application/json"})
deputados = response.json()['dados']

deputados_eventos = []
for deputado in deputados:
    id_deputado = deputado['id']
    pagina = 1
    while True:
        url_eventos = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{id_deputado}/eventos?pagina={pagina}"
        resp = req.get(url_eventos, headers={"Accept": "application/json"})
        if resp.status_code == 200:
            data = resp.json()
            if not data['dados']:
                break
            for evento in data['dados']:
                evento['id_deputado'] = id_deputado
                evento['nome_deputado'] = deputado.get('nome')
                deputados_eventos.append(evento)
            if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
                break
            pagina += 1
        else:
            print(f"Erro ao buscar eventos para deputado {id_deputado}: {resp.status_code}")
            break
        
df = pd.json_normalize(deputados_eventos)
df_deputados_eventos = spark.createDataFrame(df)

# Drop columns with VOID type (unsupported by Parquet)
for col_name, col_type in df_deputados_eventos.dtypes:
    if col_type == 'void':
        df_deputados_eventos = df_deputados_eventos.drop(col_name)

df_deputados_eventos.write.mode("overwrite").parquet(caminho + "/deputados_eventos")

# COMMAND ----------

#Décimo primeiro endpoint despesas de cada deputado

url_base = "https://dadosabertos.camara.leg.br/api/v2/deputados"
response = req.get(url_base, headers={"Accept": "application/json"})
deputados = response.json()['dados']

despesas = []
for deputado in deputados:
    id_deputado = deputado['id']
    pagina = 1
    while True:
        url_despesas = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{id_deputado}/despesas?pagina={pagina}"
        resp = req.get(url_despesas, headers={"Accept": "application/json"})
        if resp.status_code == 200:
            data = resp.json()
            if not data['dados']:
                break
            for despesa in data['dados']:
                despesa['id_deputado'] = id_deputado
                despesas.append(despesa)
            if not data.get('links') or not any(link['rel'] == 'next' for link in data['links']):
                break
            pagina += 1
        else:
            print(f"Erro ao buscar despesas para deputado {id_deputado}: {resp.status_code}")
            break

df = pd.json_normalize(despesas)
df_despesas = spark.createDataFrame(df)
#df_despesas.write.mode("overwrite").parquet(caminho + "/despesas")

# COMMAND ----------

#Despesas 

ano_atual = datetime.datetime.now().year
anos = list(range(ano_atual - 7, ano_atual + 1))
despesas = []
caminho_despesas = "/Volumes/workspace/projeto_tiller/bronze/despesas/parquet"

for ano in anos:
    file_path = f"/Volumes/workspace/projeto_tiller/bronze/despesas/json/Ano-{ano}.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if 'dados' in data:
                despesas.extend(data['dados'])
    except Exception as e:
        print(f"Erro ao ler arquivo para o ano {ano}: {e}")

if despesas:
    df = pd.json_normalize(despesas)
    df_despesas = spark.createDataFrame(df)
    df_despesas.write.mode("overwrite").parquet(caminho_despesas)
else:
    print("Nenhuma despesa foi carregada. Verifique se os arquivos JSON existem no caminho especificado.")