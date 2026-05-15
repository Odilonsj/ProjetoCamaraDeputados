# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Criação de schema, diretórios e tabelas
# MAGIC

# COMMAND ----------

#criar schema Tiller
spark.sql("create schema if not exists workspace.projeto_tiller")

#Criar volumes e camadas
spark.sql("create volume if not exists workspace.projeto_tiller.bronze")
spark.sql("create volume if not exists workspace.projeto_tiller.prata")
spark.sql("create volume if not exists workspace.projeto_tiller.ouro")



# COMMAND ----------

#Criar diretórios no dbfs

path_folder = [
    "dbfs:/Volumes/workspace/projeto_tiller/bronze",
    "dbfs:/Volumes/workspace/projeto_tiller/prata",
    "dbfs:/Volumes/workspace/projeto_tiller/ouro"
]
for path in path_folder:
    if not dbutils.fs.ls(path):
        dbutils.fs.mkdirs(path)

# COMMAND ----------

#Criar diretórios para cada endpoint/arquivo

path_subfolder = [
    #Acesso via api/endpoint
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/deputados",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/partidos",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/legislaturas",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/blocos",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/orgaos",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/deputados_orgaos",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/deputados_eventos",
    #acesso via download de arquivos
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/frentes_parlamentares/json",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/frentes_parlamentares/parquet",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/frentes_deputados/json",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/frentes_deputados/parquet",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/proposicoes/json",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/proposicoes/parquet",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/proposicoes_temas/json",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/proposicoes_temas/parquet",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/votacoes/json",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/votacoes/parquet",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/eventos/json",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/eventos/parquet",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/despesas/json",
    "dbfs:/Volumes/workspace/projeto_tiller/bronze/despesas/parquet"
]

for path in path_subfolder:
    if not dbutils.fs.ls(path):
        dbutils.fs.mkdirs(path)