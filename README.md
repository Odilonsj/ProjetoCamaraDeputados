readme_content = """
# Projeto Tiller - Eventos Legislativos

Este projeto tem como objetivo construir um pipeline de dados para análise de eventos legislativos da Câmara dos Deputados, utilizando Databricks e Spark.

## Setup

1. **Configuração do ambiente**
   - Utilize Databricks para executar notebooks com Spark.
   - Certifique-se de ter acesso aos dados brutos (Bronze) e tabelas intermediárias (Silver).

2. **Carregamento das tabelas Silver**
   - As tabelas Silver são carregadas via `spark.table`, incluindo:
     - frentes_parlamentares
     - frentes_deputados
     - deputados
     - partidos
     - legislaturas
     - blocos
     - orgaos
     - proposicoes
     - votacoes
     - eventos
     - despesas

3. **Filtragem da legislatura vigente**
   - A legislatura atual é obtida ordenando a tabela de legislaturas pela data de início.

4. **Criação das dimensões**
   - **Órgão:** Seleção e transformação dos dados de órgãos, com deduplicação e escrita em `dim_orgao`.
   - **Tipo de Evento:** Extração dos tipos de eventos distintos e geração de IDs, escrita em `dim_tipo_evento`.
   - **Data:** Geração de uma dimensão de datas desde 2019-01-01 até hoje, escrita em `dim_data`.

5. **Criação do fato de eventos**
   - Junção das dimensões com a tabela de eventos para criar o fato `fato_evento`, contendo informações detalhadas sobre cada evento legislativo.

6. **Salvamento das tabelas**
   - As dimensões e fatos são salvos como tabelas Delta no Databricks, utilizando o modo `overwrite`.

## Arquivo Gold

- O arquivo final `gold_eventos_legislativos` é construído a partir das tabelas de dimensão e fato, agregando informações relevantes para análise de eventos legislativos.
- Este arquivo pode ser utilizado para dashboards, análises exploratórias e geração de insights sobre a atuação parlamentar.

## Estrutura dos Notebooks

- O projeto está organizado em notebooks Databricks, cada um responsável por uma etapa do pipeline:
  - Carregamento de dados
  - Transformações e criação de dimensões
  - Criação de fatos
  - Geração do arquivo Gold

## Como executar

1. Clone o repositório e abra os notebooks no Databricks.
2. Execute as células sequencialmente, garantindo que as tabelas intermediárias sejam criadas.
3. O arquivo final estará disponível como tabela Delta `gold_eventos_legislativos`.

## Contato

Dúvidas ou sugestões? Entre em contato com o responsável pelo projeto.

"""

with open("README.md", "w", encoding="utf-8") as f:
    f.write(readme_content)
