# Databricks notebook source
from pyspark.sql.functions import col, lit, monotonically_increasing_id, to_date, explode, sequence
from pyspark.sql.types import DateType

# COMMAND ----------

# Load tables from Silver layer
df_frentes = spark.table("projeto_tiller.frentes_parlamentares")
df_membros = spark.table("projeto_tiller.frentes_deputados")
df_deputados = spark.table("projeto_tiller.deputados")
df_partidos = spark.table("projeto_tiller.partidos")
df_legislaturas = spark.table("projeto_tiller.legislaturas")
df_blocos = spark.table("projeto_tiller.blocos")
df_orgaos = spark.table("projeto_tiller.orgaos")
df_proposicoes = spark.table("projeto_tiller.proposicoes")
df_votacoes = spark.table("projeto_tiller.votacoes")
df_eventos = spark.table("projeto_tiller.eventos")
df_despesas = spark.table("projeto_tiller.despesas")

# Obter a legislatura atual
legislatura_atual = df_legislaturas.orderBy(df_legislaturas["datainicio"].desc()).first()["id"]

# Filtrar apenas legislatura vigente
df_frentes_ativas = df_frentes.filter(df_frentes.idLegislatura == legislatura_atual)

# COMMAND ----------

display(df_orgaos)

# COMMAND ----------

display(df_eventos)

# COMMAND ----------

display(df_eventos.filter(df_eventos.orgaos.isNotNull()))

# COMMAND ----------

# Dimensão Órgão
df_deputados_orgaos = spark.table("projeto_tiller.deputados_orgaos")
dim_orgao = (
    df_deputados_orgaos
    .select(
        col("idOrgao").alias("orgao_id"),
        col("siglaOrgao").alias("sigla"),
        col("nomeOrgao").alias("nome"),
        col("titulo").alias("tipoOrgao"),
        col("data_inicio"),
        col("data_fim"),
        col("is_current")
    )
    .dropDuplicates(["orgao_id"])
)
dim_orgao.write.mode("overwrite").saveAsTable("projeto_tiller.dim_orgao")

# Dimensão Tipo de Evento
df_deputados_eventos = spark.table("projeto_tiller.deputados_eventos")
dim_tipo_evento = (
    df_deputados_eventos
    .select(col("descricaoTipo").alias("tipo_evento"))
    .distinct()
    .withColumn("tipo_evento_id", monotonically_increasing_id())
    .select("tipo_evento_id", "tipo_evento")
)
#dim_tipo_evento.write.mode("overwrite").saveAsTable("projeto_tiller.dim_tipo_evento")
display(dim_tipo_evento)

# Dimensão Data (desde 2019-01-01 até hoje)
start_date = "2019-01-01"
end_date = spark.sql("SELECT current_date() as today").first()["today"]

date_df = (
    spark
    .createDataFrame([(start_date, end_date)], ["start", "end"])
    .select(explode(sequence(to_date(col("start")), to_date(lit(end_date)))).alias("data"))
    .withColumn("data_id", monotonically_increasing_id())
    .withColumn("ano", col("data").cast(DateType()).substr(1,4))
    .withColumn("mes", col("data").cast(DateType()).substr(6,2))
    .withColumn("dia", col("data").cast(DateType()).substr(9,2))
)
dim_data = date_df.select("data_id", "data", "ano", "mes", "dia")
dim_data.write.mode("overwrite").saveAsTable("projeto_tiller.dim_data")
# Fato Evento
fato_evento = (
    df_deputados_eventos
    .join(dim_orgao, "orgao_id")
    .join(dim_tipo_evento, "descricaoTipo")
    .join(dim_data, col("dataHoraInicio").cast(DateType()) == dim_data.data)
    .select(
        col("data_id"),
        col("orgao_id"),
        col("tipo_evento_id"),
        col("descricao"),
        col("dataHoraInicio"),
        col("dataHoraFim"),
        col("situacao"),
#         col("id").alias("evento_id")
#     )
# )
fato_evento.write.mode("overwrite").saveAsTable("projeto_tiller.fato_evento")
display(fato_evento)
# Dimensão Frente Parlamentar


# COMMAND ----------

# Dimensão Órgão
df_deputados_orgaos = spark.table("projeto_tiller.deputados_orgaos")
dim_orgao = (
    df_deputados_orgaos
    .select(
        col("idOrgao").alias("orgao_id"),
        col("siglaOrgao").alias("sigla"),
        col("nomePublicacao").alias("nome"),
        col("titulo").alias("tipo_orgao"),
        col("id_deputado"),
        col("nome_deputado")
    )
    .filter(col("is_current") == True)
    .select("*")
)
dim_orgao.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.dim_orgao")

# Dimensão Tipo de Evento
df_deputados_eventos = spark.table("projeto_tiller.deputados_eventos")
dim_tipo_evento = (
    df_deputados_eventos
    .select(
        col("id_evento"),
        col("descricao"),
        col("descricaoTipo").alias("tipo_evento"),
        col("situacao"),
        col("id_deputado"),
        col("id_orgaos")
    )
    .filter(col("is_current") == True)
    .select("*")
)
dim_tipo_evento.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.dim_tipo_evento")

# Dimensão Data (datas da tabela eventos)
df_eventos = spark.table("projeto_tiller.eventos")
min_max_dates = df_eventos.agg(
    {"dataHoraInicio": "min", "dataHoraFim": "max"}
).collect()[0]
start_date = min_max_dates["min(dataHoraInicio)"]
end_date = min_max_dates["max(dataHoraFim)"]

date_df = (
    spark
    .createDataFrame([(start_date, end_date)], ["start", "end"])
    .select(explode(sequence(to_date(col("start")), to_date(col("end")))).alias("data"))
    .withColumn("data_id", monotonically_increasing_id())
    .withColumn("ano", col("data").cast(DateType()).substr(1,4))
    .withColumn("mes", col("data").cast(DateType()).substr(6,2))
    .withColumn("dia", col("data").cast(DateType()).substr(9,2))
)
dim_data = date_df.select("data_id", "data", "ano", "mes", "dia")
dim_data.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.dim_data")

# COMMAND ----------

