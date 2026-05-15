# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Documentação - Estratégias de Carga Incremental
# MAGIC %md
# MAGIC # Estratégias de Carga Incremental - Camada Silver
# MAGIC
# MAGIC Este notebook implementa cargas incrementais otimizadas usando Delta Lake com **SCD Type 2** para rastreamento completo de histórico.
# MAGIC
# MAGIC ## 📊 **Estratégias por Tipo de Tabela:**
# MAGIC
# MAGIC ### **SCD Type 2 (Slowly Changing Dimension) - Tabelas de Dimensão**
# MAGIC Mantém **histórico completo** de todas as mudanças. Cada registro tem:
# MAGIC * `data_inicio` - Quando o registro ficou ativo
# MAGIC * `data_fim` - Quando foi substituído (NULL se ainda ativo)
# MAGIC * `is_current` - Flag indicando se é a versão atual
# MAGIC * `_hash` - Hash MD5 dos campos para detectar mudanças
# MAGIC
# MAGIC **Tabelas com SCD Type 2:**
# MAGIC * **deputados** - Rastreia mudanças de partido, UF, status
# MAGIC * **partidos** - Histórico de mudanças de nome, sigla
# MAGIC * **frentes_parlamentares** - Mudanças de situação, coordenação
# MAGIC * **frentes_deputados** - Rastreia quando deputados entram/saem da coordenação
# MAGIC * **blocos** - Alterações na composição
# MAGIC * **orgaos** - Atualizações de informações
# MAGIC * **proposicoes** - Evolução do status de tramitação
# MAGIC * **deputados_eventos** - Mudanças em horários, situação, coordenadores ou descrição dos eventos dos deputados
# MAGIC * **deputados_orgaos** - Mudanças na participação do deputado em órgãos (entrada/saída, tipo, nome, sigla, etc.)
# MAGIC
# MAGIC ### **APPEND - Tabelas de Fatos**
# MAGIC Adiciona apenas novos registros, preservando todo o histórico.
# MAGIC
# MAGIC * **votacoes** - Eventos imutáveis no tempo
# MAGIC * **despesas** - Lançamentos financeiros
# MAGIC * **proposicoes_temas** - Associação de temas às proposições (eventos históricos imutáveis, não mudam após inserção)
# MAGIC
# MAGIC ### **OVERWRITE - Dados Estáticos**
# MAGIC Reescrita completa para dados que raramente mudam.
# MAGIC
# MAGIC * **legislaturas** - Dados históricos fixos
# MAGIC
# MAGIC ## ✅ **Benefícios do SCD Type 2:**
# MAGIC
# MAGIC 1. **Histórico Completo** - Todas as versões dos dados são preservadas
# MAGIC 2. **Auditoria** - Rastreamento de quando cada mudança ocorreu
# MAGIC 3. **Análise Temporal** - Consultas "point-in-time" (como estava em uma data específica)
# MAGIC 4. **Conformidade** - Atende requisitos regulatórios de rastreabilidade
# MAGIC 5. **Performance** - Processa apenas dados novos/alterados usando hash
# MAGIC 6. **ACID** - Transações Delta Lake garantem consistência

# COMMAND ----------

# DBTITLE 1,Cell 1
from pyspark.sql.functions import to_date, to_timestamp, col, lit, try_to_timestamp, current_date, md5, concat_ws,regexp_extract
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Deputados
# Ler dados do Bronze
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/deputados")
df_new = (
    df.withColumnRenamed("uri", "url")
    .withColumnRenamed("uriPartido", "urlPartido")
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

# Criar hash dos atributos para detectar mudanças
df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("nome"), col("siglaPartido"), col("siglaUf"), col("url"), col("urlPartido")))
)

if spark.catalog.tableExists("projeto_tiller.deputados"):
    # Adicionar colunas SCD Type 2 se não existirem
    existing_df = spark.table("projeto_tiller.deputados")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.deputados
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        # Atualizar registros existentes para marcar como atuais
        spark.sql("""
            UPDATE projeto_tiller.deputados
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', nome, siglaPartido, siglaUf, url, urlPartido))
            WHERE is_current IS NULL
        """)
    
    # SCD Type 2: Desativar registros antigos que mudaram e inserir novos
    deltaTable = DeltaTable.forName(spark, "projeto_tiller.deputados")
    
    # Recarregar após ALTER TABLE
    existing_df = spark.table("projeto_tiller.deputados")
    
    # 1. Desativar registros que mudaram (marca data_fim e is_current=false)
    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id = source.id AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    # 2. Inserir novos registros (novos IDs ou IDs que mudaram)
    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        col("source.id") == col("target.id"),
        "left"
    ).where(
        "target.id IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").saveAsTable("projeto_tiller.deputados")
        print(f"✓ Tabela deputados atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em deputados")
else:
    df_new.write.mode("overwrite").saveAsTable("projeto_tiller.deputados")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.deputados

# COMMAND ----------

# DBTITLE 1,Partidos
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/partidos")
df_new = (
    df.withColumnRenamed("uri", "url")
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

# Hash para detectar mudanças
df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("sigla"), col("nome"), col("url")))
)

if spark.catalog.tableExists("projeto_tiller.partidos"):
    # Adicionar colunas SCD Type 2 se não existirem
    existing_df = spark.table("projeto_tiller.partidos")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.partidos
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        # Atualizar registros existentes para marcar como atuais
        spark.sql("""
            UPDATE projeto_tiller.partidos
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', sigla, nome, url))
            WHERE is_current IS NULL
        """)
    
    deltaTable = DeltaTable.forName(spark, "projeto_tiller.partidos")
    
    # Recarregar após ALTER TABLE
    existing_df = spark.table("projeto_tiller.partidos")
    
    # Desativar registros que mudaram
    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id = source.id AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    # Inserir novos e atualizados
    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        col("source.id") == col("target.id"),
        "left"
    ).where(
        "target.id IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").saveAsTable("projeto_tiller.partidos")
        print(f"✓ Tabela partidos atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em partidos")
else:
    df_new.write.mode("overwrite").saveAsTable("projeto_tiller.partidos")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.partidos

# COMMAND ----------

# DBTITLE 1,Legislaturas
# Legislaturas são dados históricos que raramente mudam
# Mantém OVERWRITE pois é uma tabela pequena e estática
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/legislaturas")

df_prata = (
    df.withColumnRenamed("uri", "url")
    .withColumn("dataInicio", to_date(col("dataInicio"), "yyyy-MM-dd"))
    .withColumn("dataFim", to_date(col("dataFim"), "yyyy-MM-dd"))
)

df_prata.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.legislaturas")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.legislaturas

# COMMAND ----------

# DBTITLE 1,Frentes Parlamentares
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/frentes_parlamentares/parquet")

# Renomear colunas
new_columns = [col_name.replace("uri", "url").replace(".", "_") for col_name in df.columns]
df_new = df.toDF(*new_columns)

df_new = (
    df_new.withColumn("datacriacao", to_date(col("datacriacao"), "yyyy-MM-dd"))
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

# Hash para detectar mudanças (principais campos)
df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("titulo"), col("situacao"), col("telefone"), col("urlDocumento")))
)

if spark.catalog.tableExists("projeto_tiller.frentes_parlamentares"):
    # Adicionar colunas SCD Type 2 se não existirem
    existing_df = spark.table("projeto_tiller.frentes_parlamentares")
    # Atualizar registros existentes para marcar como atuais
    spark.sql("""
        UPDATE projeto_tiller.frentes_parlamentares
        SET data_inicio = CURRENT_DATE,
            is_current = true,
            _hash = md5(concat_ws('|', titulo, situacao, telefone, urlDocumento))
        WHERE is_current IS NULL
    """)

    deltaTable = DeltaTable.forName(spark, "projeto_tiller.frentes_parlamentares")
    
    # Recarregar após ALTER TABLE
    existing_df = spark.table("projeto_tiller.frentes_parlamentares")
    
    # Desativar registros que mudaram
    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id = source.id AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    # Inserir novos e atualizados
    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        col("source.id") == col("target.id"),
        "left"
    ).where(
        "target.id IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.frentes_parlamentares")
        print(f"✓ Tabela frentes_parlamentares atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em frentes_parlamentares")
else:
    df_new.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.frentes_parlamentares")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.frentes_parlamentares order by id

# COMMAND ----------

# DBTITLE 1,Frentes Deputados
# Frentes_deputados - SCD Type 2: rastreia quando deputados entram/saem da coordenação
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/frentes_deputados/parquet")

# Renomear todas as colunas: trocar "uri" por "url" e "." por "_"
new_columns = [col_name.replace("uri", "url").replace(".", "_") for col_name in df.columns]
df_new = df.toDF(*new_columns)

df_new = (
    df_new.withColumn("iddeputado", col("iddeputado").cast("int"))
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

# Hash para detectar mudanças na relação (id + iddeputado + titulo)
df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("id"), col("iddeputado"), col("tituloDeputado")))
)

# Remover duplicatas baseadas na chave composta (id, iddeputado)
df_new = df_new.dropDuplicates(["id", "iddeputado"])

if spark.catalog.tableExists("projeto_tiller.frentes_deputados"):
    existing_df = spark.table("projeto_tiller.frentes_deputados")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.frentes_deputados
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        spark.sql("""
            UPDATE projeto_tiller.frentes_deputados
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', CAST(id AS STRING), CAST(iddeputado AS STRING), titulo))
            WHERE is_current IS NULL
        """)
        print("✓ Colunas SCD Type 2 adicionadas à tabela frentes_deputados")
    
    deltaTable = DeltaTable.forName(spark, "projeto_tiller.frentes_deputados")
    existing_df = spark.table("projeto_tiller.frentes_deputados")
    
    # Desativar registros que mudaram (deputado saiu da coordenação)
    # Usar <=> (null-safe equals) para comparar corretamente valores NULL
    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id = source.id AND (target.iddeputado <=> source.iddeputado) AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    # Inserir novos e atualizados (deputado entrou na coordenação ou dados mudaram)
    # Usar eqNullSafe para comparar corretamente valores NULL
    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        (col("source.id") == col("target.id")) & col("source.iddeputado").eqNullSafe(col("target.iddeputado")),
        "left"
    ).where(
        "target.id IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.frentes_deputados")
        print(f"✓ Tabela frentes_deputados atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em frentes_deputados")
else:
    df_new.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.frentes_deputados")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.frentes_deputados

# COMMAND ----------

# DBTITLE 1,Blocos
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/blocos")

# Renomear colunas
df_new = (
    df.withColumnRenamed("uri", "url")
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

# Hash para detectar mudanças (principais campos)
df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("nome"), col("idLegislatura"), col("federacao")))
)

if spark.catalog.tableExists("projeto_tiller.blocos"):
    # Adicionar colunas SCD Type 2 se não existirem
    existing_df = spark.table("projeto_tiller.blocos")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.blocos
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        # Atualizar registros existentes para marcar como atuais
        spark.sql("""
            UPDATE projeto_tiller.blocos
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', nome, idLegislatura, federacao))
            WHERE is_current IS NULL
        """)

    deltaTable = DeltaTable.forName(spark, "projeto_tiller.blocos")
    
    # Recarregar após ALTER TABLE
    existing_df = spark.table("projeto_tiller.blocos")
      
    # Desativar registros que mudaram
    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id = source.id AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    # Inserir novos e atualizados
    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        col("source.id") == col("target.id"),
        "left"
    ).where(
        "target.id IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.blocos")
        print(f"✓ Tabela blocos atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em blocos")
else:
    df_new.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.blocos")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.blocos

# COMMAND ----------

# DBTITLE 1,Órgãos
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/orgaos")
df_new = (
    df.withColumnRenamed("uri", "url")
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("nome"), col("sigla"), col("tipoOrgao")))
)

if spark.catalog.tableExists("projeto_tiller.orgaos"):
    existing_df = spark.table("projeto_tiller.orgaos")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.orgaos
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        spark.sql("""
            UPDATE projeto_tiller.orgaos
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', nome, sigla, tipoOrgao))
            WHERE is_current IS NULL
        """)
        print("✓ Colunas SCD Type 2 adicionadas à tabela existente")
    
    deltaTable = DeltaTable.forName(spark, "projeto_tiller.orgaos")

    existing_df = spark.table("projeto_tiller.orgaos")

    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id = source.id AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()

    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        col("source.id") == col("target.id"),
        "left"
    ).where(
        "target.id IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.orgaos")
        print(f"✓ Tabela orgaos atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em orgaos")
else:
    df_new.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.orgaos")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.orgaos

# COMMAND ----------

# DBTITLE 1,Orgãos Deputados
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/deputados_orgaos")
df_new = (
    df.withColumnRenamed("uri", "url")
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("id_Deputado"), col("idOrgao"), col("nomeOrgao"), col("siglaOrgao")))
)

if spark.catalog.tableExists("projeto_tiller.deputados_orgaos"):
    existing_df = spark.table("projeto_tiller.deputados_orgaos")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.deputados_orgaos
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        spark.sql("""
            UPDATE projeto_tiller.deputados_orgaos
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', id_Deputado, idOrgao, nomeOrgao, tipoOrgao))
            WHERE is_current IS NULL
        """)
        print("✓ Colunas SCD Type 2 adicionadas à tabela existente")
    
    deltaTable = DeltaTable.forName(spark, "projeto_tiller.deputados_orgaos")

    existing_df = spark.table("projeto_tiller.deputados_orgaos")

    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id_Deputado = source.idDeputado AND target.idOrgao = source.idOrgao AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()

    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        (col("source.id_Deputado") == col("target.id_Deputado")) & (col("source.idOrgao") == col("target.idOrgao")),
        "left"
    ).where(
        "target.id_Deputado IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.deputados_orgaos")
        print(f"✓ Tabela deputados_orgaos atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em deputados_orgaos")
else:
    df_new.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.deputados_orgaos")

# COMMAND ----------

# MAGIC %sql
# MAGIC select idorgao,siglaOrgao,nomePublicacao,id_deputado,nome_deputado
# MAGIC  from  projeto_tiller.deputados_orgaos order by idorgao

# COMMAND ----------

# DBTITLE 1,Proposições
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/proposicoes/parquet")

# Renomear colunas
new_columns = [col_name.replace("uri", "url").replace(".", "_") for col_name in df.columns]
df_new = df.toDF(*new_columns)

df_new = (
    df_new.withColumn("dataApresentacao", to_timestamp(col("dataApresentacao"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("ultimoStatus_data", to_timestamp(col("ultimoStatus_data"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

# Hash baseado em campos que podem mudar (status, ementa)
df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("ementa"), col("ultimoStatus_descricaoTramitacao"), col("ultimoStatus_descricaoSituacao")))
)

if spark.catalog.tableExists("projeto_tiller.proposicoes"):
    existing_df = spark.table("projeto_tiller.proposicoes")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.proposicoes
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        spark.sql("""
            UPDATE projeto_tiller.proposicoes
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', ementa, ultimoStatus_descricaoTramitacao, ultimoStatus_descricaoSituacao))
            WHERE is_current IS NULL
        """)
    
    deltaTable = DeltaTable.forName(spark, "projeto_tiller.proposicoes")
    
    # Recarregar após ALTER TABLE
    existing_df = spark.table("projeto_tiller.proposicoes")
      
    # Desativar registros que mudaram
    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id = source.id AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    # Inserir novos e atualizados
    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        col("source.id") == col("target.id"),
        "left"
    ).where(
        "target.id IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.proposicoes")
        print(f"✓ Tabela proposicoes atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em proposicoes")
else:
    df_new.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.proposicoes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.proposicoes

# COMMAND ----------

# DBTITLE 1,Proposições Temas
# Temas são eventos históricos imutáveis - usa APPEND
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/proposicoes_temas/parquet")

# Renomear colunas: trocar "uriProposicao" por "urlProposicao" e "." por "_"
new_columns = [col_name.replace("uriProposicao", "urlProposicao").replace(".", "_") for col_name in df.columns]
df_prata = df.toDF(*new_columns)

# Extrair id_proposicao do campo urlProposicao (após 'proposicoes/')
df_prata = df_prata.withColumn(
    "id_proposicao",
    regexp_extract(col("urlProposicao"), r"proposicoes/(\d+)", 1)
)

if spark.catalog.tableExists("projeto_tiller.proposicoes_temas"):
    df_prata.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.proposicoes_temas")
else:
    df_prata.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.proposicoes_temas")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.proposicoes_temas

# COMMAND ----------

# DBTITLE 1,Votações
# Votações são eventos históricos imutáveis - usa APPEND
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/votacoes/parquet")

# Renomear todas as colunas: trocar "uri" por "url" e "." por "_"
new_columns = [col_name.replace("uri", "url").replace(".", "_") for col_name in df.columns]
df_prata = df.toDF(*new_columns)

df_prata = (
    df_prata.withColumn("data", to_date(col("data"), "yyyy-MM-dd"))
    .withColumn("dataHoraRegistro", try_to_timestamp(col("dataHoraRegistro"), lit("yyyy-MM-dd'T'HH:mm:ss")))
    .withColumn("ultimaAberturaVotacao_dataHoraRegistro", try_to_timestamp(col("ultimaAberturaVotacao_dataHoraRegistro"), lit("yyyy-MM-dd'T'HH:mm:ss")))
    .withColumn("ultimaApresentacaoProposicao_dataHoraRegistro", try_to_timestamp(col("ultimaApresentacaoProposicao_dataHoraRegistro"), lit("yyyy-MM-dd'T'HH:mm:ss")))
)

if spark.catalog.tableExists("projeto_tiller.votacoes"):
    df_prata.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.votacoes")
else:
    df_prata.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.votacoes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.votacoes

# COMMAND ----------

# DBTITLE 1,Eventos
# Eventos - SCD Type 2: mantém histórico e atualiza campos mutáveis (ex: dataHoraFim, situacaoEvento)
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/eventos/parquet")

# Renomear colunas: substituir "." por "_"
new_columns = [col_name.replace(".", "_") for col_name in df.columns]
df = df.toDF(*new_columns)

df_prata = (
    df.withColumnRenamed("uri", "url")
      .withColumn("dataHoraInicio", try_to_timestamp(col("dataHoraInicio")))
      .withColumn("dataHoraFim", try_to_timestamp(col("dataHoraFim")))
      .withColumn("data_inicio", current_date())
      .withColumn("data_fim", lit(None).cast("date"))
      .withColumn("is_current", lit(True))
)

# Hash para detectar mudanças relevantes (ex: dataHoraFim, situacaoEvento)
df_prata = df_prata.withColumn(
    "_hash",
    md5(concat_ws("|", col("dataHoraInicio"), col("dataHoraFim"), col("situacao")))
)

if spark.catalog.tableExists("projeto_tiller.eventos"):
    existing_df = spark.table("projeto_tiller.eventos")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.eventos
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        spark.sql("""
            UPDATE projeto_tiller.eventos
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', dataHoraInicio, dataHoraFim, situacao))
            WHERE is_current IS NULL
        """)
        print("✓ Colunas SCD Type 2 adicionadas à tabela eventos")
    
    from delta.tables import DeltaTable
    deltaTable = DeltaTable.forName(spark, "projeto_tiller.eventos")
    existing_df = spark.table("projeto_tiller.eventos")
    
    # Desativar registros antigos que mudaram
    deltaTable.alias("target").merge(
        df_prata.alias("source"),
        "target.id = source.id AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    # Inserir novos e atualizados
    updates_and_inserts = df_prata.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        col("source.id") == col("target.id"),
        "left"
    ).where(
        "target.id IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.eventos")
        print(f"✓ Tabela eventos atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em eventos")
else:
    df_prata.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.eventos")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.eventos

# COMMAND ----------

# DBTITLE 1,Eventos Deputados
from pyspark.sql.functions import col, lit, current_date, md5, concat_ws, array_join, transform

df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/deputados_eventos")

# id_orgaos como string (join dos ids separados por vírgula)
df_new = (
    df.select(
        col("id_deputado"),
        col("nome_deputado"),
        col("id").alias("id_evento"),
        col("uri").alias("url"),
        col("dataHoraInicio"),
        col("dataHoraFim"),
        col("situacao"),
        col("descricaoTipo"),
        col("descricao"),
        array_join(transform(col("orgaos"), lambda x: x["id"].cast("int")), ",").alias("id_orgaos")  # string
    )
    .withColumn("data_inicio", current_date())
    .withColumn("data_fim", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

df_new = df_new.withColumn(
    "_hash",
    md5(concat_ws("|", col("id_deputado"), col("id_evento"), col("dataHoraInicio"), col("dataHoraFim"), col("situacao"), col("descricaoTipo"), col("descricao"), col("id_orgaos")))
)

# Remove duplicates based on the merge keys
df_new = df_new.dropDuplicates(["id_deputado", "id_evento"])

if spark.catalog.tableExists("projeto_tiller.deputados_eventos"):
    existing_df = spark.table("projeto_tiller.deputados_eventos")
    if "is_current" not in existing_df.columns:
        spark.sql("""
            ALTER TABLE projeto_tiller.deputados_eventos
            ADD COLUMNS (
                data_inicio DATE,
                data_fim DATE,
                is_current BOOLEAN,
                _hash STRING
            )
        """)
        spark.sql("""
            UPDATE projeto_tiller.deputados_eventos
            SET data_inicio = CURRENT_DATE,
                is_current = true,
                _hash = md5(concat_ws('|', CAST(id_deputado AS STRING), CAST(id_evento AS STRING), dataHoraInicio, dataHoraFim, situacao, descricaoTipo, descricao, id_orgaos))
            WHERE is_current IS NULL
        """)
    
    from delta.tables import DeltaTable
    deltaTable = DeltaTable.forName(spark, "projeto_tiller.deputados_eventos")
    existing_df = spark.table("projeto_tiller.deputados_eventos")
    
    deltaTable.alias("target").merge(
        df_new.alias("source"),
        "target.id_deputado = source.id_deputado AND target.id_evento = source.id_evento AND target.is_current = true AND target._hash != source._hash"
    ).whenMatchedUpdate(
        set = {
            "data_fim": "current_date()",
            "is_current": "false"
        }
    ).execute()
    
    updates_and_inserts = df_new.alias("source").join(
        existing_df.filter(col("is_current") == True).alias("target"),
        (col("source.id_deputado") == col("target.id_deputado")) &
        (col("source.id_evento") == col("target.id_evento")),
        "left"
    ).where(
        "target.id_deputado IS NULL OR target._hash != source._hash"
    ).select("source.*")
    
    if updates_and_inserts.count() > 0:
        updates_and_inserts.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.deputados_eventos")
        print(f"✓ Tabela deputados_eventos atualizada via SCD Type 2 ({updates_and_inserts.count()} registros)")
    else:
        print("✓ Nenhuma mudança detectada em deputados_eventos")
else:
    df_new.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.deputados_eventos")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from projeto_tiller.deputados_eventos

# COMMAND ----------

# DBTITLE 1,Despesas
# Despesas são lançamentos históricos imutáveis - usa APPEND
df = spark.read.parquet("/Volumes/workspace/projeto_tiller/bronze/despesas/parquet")

df_prata = (
    df.withColumn("dataEmissao", try_to_timestamp(col("dataEmissao")))
    .withColumn("valorDocumento", col("valorDocumento").cast("decimal(18,2)"))
    .withColumn("valorGlosa", col("valorGlosa").cast("decimal(18,2)"))
    .withColumn("valorLiquido", col("valorLiquido").cast("decimal(18,2)"))
)

if spark.catalog.tableExists("projeto_tiller.despesas"):
    df_prata.write.mode("append").option("mergeSchema", "true").saveAsTable("projeto_tiller.despesas")
else:
    df_prata.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("projeto_tiller.despesas")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projeto_tiller.despesas

# COMMAND ----------

# DBTITLE 1,Monitoramento - Estatísticas das Cargas
# MAGIC %md
# MAGIC ## 📋 Monitoramento e Auditoria
# MAGIC
# MAGIC Execute as células abaixo para ver estatísticas das cargas incrementais.

# COMMAND ----------

# DBTITLE 1,Verificar tamanho das tabelas
# Listar todas as tabelas do schema com contagem de registros
tabelas = [
    "deputados", "partidos", "legislaturas", "frentes_parlamentares",
    "frentes_deputados", "blocos", "orgaos", "proposicoes",
    "votacoes", "eventos", "despesas"
]

print("\n┌" + "─" * 75 + "┐")
print("│" + " " * 15 + "ESTATÍSTICAS DAS TABELAS SILVER (SCD Type 2)" + " " * 15 + "│")
print("├" + "─" * 75 + "┤")
print(f"│ {'Tabela':<30} | {'Total':>10} | {'Ativos':>10} | {'Estratégia':<15} │")
print("├" + "─" * 75 + "┤")

estrategias = {
    "deputados": "SCD Type 2",
    "partidos": "SCD Type 2",
    "legislaturas": "OVERWRITE",
    "frentes_parlamentares": "SCD Type 2",
    "frentes_deputados": "SCD Type 2",
    "blocos": "SCD Type 2",
    "orgaos": "SCD Type 2",
    "proposicoes": "SCD Type 2",
    "votacoes": "APPEND",
    "eventos": "SCD Type 2",
    "despesas": "APPEND"
}

for tabela in tabelas:
    try:
        total = spark.table(f"projeto_tiller.{tabela}").count()
        estrategia = estrategias.get(tabela, "N/A")
        
        # Para tabelas SCD Type 2, mostrar contagem de registros ativos
        if estrategia == "SCD Type 2":
            ativos = spark.table(f"projeto_tiller.{tabela}").filter(col("is_current") == True).count()
            print(f"│ {tabela:<30} | {total:>10,} | {ativos:>10,} | {estrategia:<15} │")
        else:
            print(f"│ {tabela:<30} | {total:>10,} | {'N/A':>10} | {estrategia:<15} │")
    except Exception as e:
        print(f"│ {tabela:<30} | {'Erro':<10} | {'N/A':>10} | {estrategias.get(tabela, 'N/A'):<15} │")

print("└" + "─" * 75 + "┘\n")
print("💡 Tabelas SCD Type 2: 'Total' inclui histórico, 'Ativos' mostra apenas registros atuais")
print("   Para ver histórico de um ID específico: SELECT * FROM tabela WHERE id = X ORDER BY data_inicio")
print("   Para ver apenas versões atuais: SELECT * FROM tabela WHERE is_current = true")

# COMMAND ----------

# DBTITLE 1,Verificar histórico Delta (Time Travel)
# Exemplo: Ver histórico de versões da tabela deputados
print("Histórico de Versões - Tabela deputados:")

try:
    history_df = spark.sql("""
        DESCRIBE HISTORY projeto_tiller.deputados
        LIMIT 5
    """)
    
    display(history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    ))
except Exception as e:
    print(f"Erro ao buscar histórico: {e}")

# COMMAND ----------

# DBTITLE 1,Exemplos de Consultas SCD Type 2
# MAGIC %md
# MAGIC ## 🔍 Exemplos de Consultas com SCD Type 2
# MAGIC
# MAGIC ### **1. Ver apenas versões atuais (mais comum)**
# MAGIC ```sql
# MAGIC SELECT * FROM projeto_tiller.deputados
# MAGIC WHERE is_current = true
# MAGIC ```
# MAGIC
# MAGIC ### **2. Ver histórico completo de um deputado específico**
# MAGIC ```sql
# MAGIC SELECT id, nome, siglaPartido, siglaUf, data_inicio, data_fim, is_current
# MAGIC FROM projeto_tiller.deputados
# MAGIC WHERE id = 123
# MAGIC ORDER BY data_inicio DESC
# MAGIC ```
# MAGIC
# MAGIC ### **3. Point-in-Time Query (como estava em uma data específica)**
# MAGIC ```sql
# MAGIC SELECT * FROM projeto_tiller.deputados
# MAGIC WHERE id = 123
# MAGIC   AND data_inicio <= '2026-05-01'
# MAGIC   AND (data_fim IS NULL OR data_fim > '2026-05-01')
# MAGIC ```
# MAGIC
# MAGIC ### **4. Encontrar registros que mudaram recentemente**
# MAGIC ```sql
# MAGIC SELECT * FROM projeto_tiller.deputados
# MAGIC WHERE data_inicio >= CURRENT_DATE - INTERVAL '7' DAY
# MAGIC ORDER BY data_inicio DESC
# MAGIC ```
# MAGIC
# MAGIC ### **5. Contar quantas vezes um registro mudou**
# MAGIC ```sql
# MAGIC SELECT id, nome, COUNT(*) as num_versoes
# MAGIC FROM projeto_tiller.deputados
# MAGIC GROUP BY id, nome
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY num_versoes DESC
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exemplo: Ver histórico de mudanças de partido
# MAGIC %sql
# MAGIC -- Exemplo prático: deputados que mudaram de partido
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     nome,
# MAGIC     siglaPartido,
# MAGIC     data_inicio,
# MAGIC     data_fim,
# MAGIC     is_current,
# MAGIC     DATEDIFF(COALESCE(data_fim, CURRENT_DATE), data_inicio) as dias_neste_partido
# MAGIC FROM projeto_tiller.deputados
# MAGIC WHERE id IN (
# MAGIC     SELECT id FROM projeto_tiller.deputados
# MAGIC     GROUP BY id HAVING COUNT(*) > 1
# MAGIC )
# MAGIC ORDER BY nome, data_inicio DESC
# MAGIC LIMIT 20