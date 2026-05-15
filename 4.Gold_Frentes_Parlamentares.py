# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# Load tables from Silver layer
df_frentes = spark.table("projeto_tiller.frentes_parlamentares")
df_membros = spark.table("projeto_tiller.frentes_deputados")
df_deputados = spark.table("projeto_tiller.deputados")
df_partidos = spark.table("projeto_tiller.partidos")
df_legislaturas = spark.table("projeto_tiller.legislaturas")
df_blocos = spark.table("projeto_tiller.blocos")
df_orgaos = spark.table("projeto_tiller.orgaos")
df_deputados_orgaos = spark.table("projeto_tiller.deputados_orgaos")
df_proposicoes = spark.table("projeto_tiller.proposicoes")
df_proposicoes_temas = spark.table("projeto_tiller.proposicoes_temas")
df_votacoes = spark.table("projeto_tiller.votacoes")
df_eventos = spark.table("projeto_tiller.eventos")
df_despesas = spark.table("projeto_tiller.despesas")

# Obter a legislatura atual
legislatura_atual = df_legislaturas.orderBy(df_legislaturas["datainicio"].desc()).first()["id"]

# Filtrar apenas legislatura vigente
df_frentes_ativas = df_frentes.filter(df_frentes.idLegislatura == legislatura_atual)

# COMMAND ----------

df_result = (
    df_frentes_ativas
    .join(df_membros, df_frentes_ativas.id == df_membros.id)
    .join(df_deputados, df_membros.iddeputado == df_deputados.id)
    .select(
        df_frentes_ativas["id"].alias("id_frente"),
        df_frentes_ativas["titulo"].alias("nome_frente"),
        df_frentes_ativas["datacriacao"],
        df_frentes_ativas["idLegislatura"],
        df_membros["iddeputado"],
        df_membros["nome"],
        df_deputados["siglaPartido"].alias("partido"),
        df_membros["siglaUf"].alias("uf"),
    )
)
df_result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "projeto_tiller.gold_frentes_parlamentares"
)

# COMMAND ----------

df_gold_frentes = spark.table("projeto_tiller.gold_frentes_parlamentares")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.gold_frentes_parlamentares

# COMMAND ----------

# Calcular a proporção de cada partido em cada frente
df_partidos_frentes_count = (
    df_gold_frentes.groupBy("id_frente", "nome_frente", "partido")
    .agg(F.count("*").alias("count_partido"))
)

df_membros_frentes_count = (
    df_gold_frentes.groupBy("id_frente", "nome_frente")
    .agg(F.count("*").alias("total_membros"))
)
# Join para obter total de membros por frente
df_diversidade = (
    df_partidos_frentes_count
    .join(df_membros_frentes_count, ["id_frente", "nome_frente"])
    .withColumn("proporcao", F.col("count_partido") / F.col("total_membros"))
)
# Calcular índice de Herfindahl para cada frente
df_herfindahl = (
    df_diversidade.groupBy("id_frente", "nome_frente")
    .agg(F.sum(F.pow("proporcao", 2)).alias("herfindahl_index"))
    .withColumn("diversidade", 1 - F.col("herfindahl_index"))
    .orderBy(F.col("diversidade").desc())
    .limit(20)
)
df_herfindahl.createOrReplaceTempView("vw_herfindahl")

# COMMAND ----------

# DBTITLE 1,Cell 7
display(spark.sql("SELECT * FROM vw_herfindahl"))

# COMMAND ----------

df_dep_mais_frentes = (
    df_gold_frentes
    .groupBy("iddeputado", "nome", "partido", "uf")
    .agg(F.countDistinct("id_frente").alias("num_frentes"))
    .orderBy(F.col("num_frentes").desc())
)
df_dep_mais_frentes.createOrReplaceTempView("vw_dep_mais_frentes")

# COMMAND ----------

display(spark.sql("SELECT * FROM vw_dep_mais_frentes"))

# COMMAND ----------

df_frentes_por_legislatura = (
    df_frentes.groupBy("idLegislatura")
    .agg(F.countDistinct("id").alias("qtd_frentes"))
    .orderBy(F.col("idLegislatura").desc())
)
display(df_frentes_por_legislatura)

# COMMAND ----------

display(df_deputados_orgaos.orderBy("idorgao"))

# COMMAND ----------

display(df_orgaos)

# COMMAND ----------

display(df_proposicoes)