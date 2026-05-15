# Databricks notebook source
import re
from pyspark.sql import functions as F 
from pyspark.sql.functions import udf, explode, lower, trim, regexp_replace
from pyspark.sql.types import ArrayType, StringType

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

# DBTITLE 1,Extração de palavras-chave dos títulos
# Extrair palavras-chave relevantes dos títulos das frentes

# Palavras irrelevantes (stopwords) para remover
stopwords = {'de', 'da', 'do', 'dos', 'das', 'em', 'e', 'o', 'a', 'os', 'as', 'para', 'pelo', 'pela', 'pelos', 'pelas', 'ao', 'à', 'aos', 'às', 'na', 'no', 'nas', 'nos', 'por', 'com', 'contra', 'mista', 'parlamentar', 'frente', 'brasil'}

def extrair_palavras_chave(titulo):
    if not titulo:
        return []
    # Converter para minúsculas e remover pontuação
    titulo_limpo = re.sub(r'[^a-záàâãéêíóôõúç\s]', '', titulo.lower())
    # Dividir em palavras
    palavras = titulo_limpo.split()
    # Filtrar stopwords e palavras muito curtas
    palavras_relevantes = [p for p in palavras if p not in stopwords and len(p) > 3]
    return palavras_relevantes

extrair_palavras_udf = udf(extrair_palavras_chave, ArrayType(StringType()))

# Aplicar extração de palavras-chave
df_frentes_keywords = df_frentes_ativas.withColumn(
    "palavras_chave",
    extrair_palavras_udf(F.col("titulo"))
)

display(df_frentes_keywords.select("id", "titulo", "palavras_chave"))

# COMMAND ----------

# DBTITLE 1,Top temas mais frequentes
# Análise das palavras-chave mais frequentes
df_temas_expandidos = df_frentes_keywords.select(
    "id",
    "titulo",
    explode("palavras_chave").alias("tema")
)

df_temas_frequentes = (
    df_temas_expandidos
    .groupBy("tema")
    .agg(
        F.count("*").alias("frequencia"),
        F.collect_set("titulo").alias("exemplos_frentes")
    )
    .orderBy(F.col("frequencia").desc())
)

display(df_temas_frequentes)

# COMMAND ----------

# DBTITLE 1,Categorização por grandes temas
# Criar categorias temáticas baseadas em palavras-chave
from pyspark.sql.functions import when, array_contains, size

# Definir categorias temáticas
categorias = {
    'Saúde': ['saúde', 'médico', 'hospital', 'medicina', 'doença', 'paciente', 'sus', 'medicamento', 'enfermagem'],
    'Educação': ['educação', 'escola', 'ensino', 'professor', 'universidade', 'estudante', 'educacional'],
    'Agricultura': ['agricultura', 'rural', 'agropecuária', 'campo', 'produtor', 'agrícola', 'agrário'],
    'Meio Ambiente': ['meio', 'ambiente', 'ambiental', 'sustentável', 'natureza', 'ecologia', 'clima'],
    'Economia': ['economia', 'econômico', 'empresarial', 'negócio', 'comércio', 'financeiro', 'indústria'],
    'Infraestrutura': ['infraestrutura', 'transporte', 'rodovia', 'ferrovia', 'obra', 'construção'],
    'Direitos Humanos': ['direitos', 'humanos', 'cidadania', 'igualdade', 'social', 'inclusão'],
    'Segurança': ['segurança', 'polícia', 'crime', 'violência', 'defesa'],
    'Cultura': ['cultura', 'cultural', 'arte', 'artístico', 'patrimônio'],
    'Tecnologia': ['tecnologia', 'digital', 'inovação', 'internet', 'tecnológico']
}

# Função para classificar frente em categorias
def classificar_tema(palavras_chave):
    if not palavras_chave:
        return ['Sem Categoria']
    
    categorias_encontradas = []
    for categoria, palavras in categorias.items():
        if any(palavra in palavras_chave for palavra in palavras):
            categorias_encontradas.append(categoria)
    
    return categorias_encontradas if categorias_encontradas else ['Outros']

classificar_udf = udf(classificar_tema, ArrayType(StringType()))

df_frentes_categorias = df_frentes_keywords.withColumn(
    "categorias",
    classificar_udf(F.col("palavras_chave"))
)

# Salvar tabela com categorias
df_frentes_categorias.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "projeto_tiller.gold_frentes_categorias"
)

print("Tabela gold_frentes_categorias criada com sucesso!")

# COMMAND ----------

# DBTITLE 1,Distribuição por categoria
# Visualizar distribuição de frentes por categoria
df_gold_categorias = spark.table("projeto_tiller.gold_frentes_categorias")

df_categorias_expandidas = df_gold_categorias.select(
    "id",
    "titulo",
    explode("categorias").alias("categoria")
)

df_dist_categorias = (
    df_categorias_expandidas
    .groupBy("categoria")
    .agg(
        F.count("*").alias("num_frentes"),
        F.collect_list("titulo").alias("exemplos")
    )
    .orderBy(F.col("num_frentes").desc())
)

display(df_dist_categorias)

# COMMAND ----------

# DBTITLE 1,Matriz de correlação - Deputados em frentes do mesmo tema
# Análise de correlação: deputados que participam de múltiplas frentes do mesmo tema
df_gold_frentes = spark.table("projeto_tiller.gold_frentes_parlamentares")

# Join para adicionar categorias às frentes com deputados
df_deputados_categorias = (
    df_gold_frentes
    .join(
        df_gold_categorias.select("id", "categorias"),
        df_gold_frentes.id_frente == df_gold_categorias.id
    )
    .select(
        "iddeputado",
        "nome",
        "partido",
        "id_frente",
        "nome_frente",
        explode("categorias").alias("categoria")
    )
)

# Contar quantas frentes cada deputado participa por categoria
df_deputados_temas = (
    df_deputados_categorias
    .groupBy("iddeputado", "nome", "partido", "categoria")
    .agg(F.countDistinct("id_frente").alias("num_frentes_categoria"))
    .filter(F.col("num_frentes_categoria") >= 2)  # Apenas deputados com 2+ frentes na mesma categoria
    .orderBy(F.col("num_frentes_categoria").desc())
)

display(df_deputados_temas)

# COMMAND ----------

# DBTITLE 1,Frentes relacionadas por sobreposição de membros
# Identificar frentes relacionadas baseado em membros em comum
df_membros_frentes = df_gold_frentes.select("iddeputado", "id_frente", "nome_frente")

# Self-join para encontrar pares de frentes com membros em comum
df_relacao_frentes = (
    df_membros_frentes.alias("f1")
    .join(
        df_membros_frentes.alias("f2"),
        (F.col("f1.iddeputado") == F.col("f2.iddeputado")) & 
        (F.col("f1.id_frente") < F.col("f2.id_frente"))  # Evitar duplicatas
    )
    .groupBy(
        F.col("f1.id_frente").alias("frente_1_id"),
        F.col("f1.nome_frente").alias("frente_1_nome"),
        F.col("f2.id_frente").alias("frente_2_id"),
        F.col("f2.nome_frente").alias("frente_2_nome")
    )
    .agg(F.count("*").alias("membros_comum"))
    .filter(F.col("membros_comum") >= 5)  # Pelo menos 5 membros em comum
    .orderBy(F.col("membros_comum").desc())
)

display(df_relacao_frentes.limit(50))