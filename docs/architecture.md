# Arquitetura

## Visão geral

Este projeto segue a **arquitetura medalhão** (medallion architecture), padrão
recomendado pela Databricks para pipelines de engenharia de dados no
Lakehouse, organizados em três camadas dentro do Unity Catalog:

| Camada  | Objetivo                                              | Schema        |
|---------|--------------------------------------------------------|---------------|
| Bronze  | Ingestão de dados crus, sem transformação de negócio    | `bronze`      |
| Silver  | Limpeza, deduplicação, tipagem e qualidade de dados     | `silver`      |
| Gold    | Agregações e métricas de negócio prontas para consumo   | `gold`        |

## Fluxo de execução

O job `etl_pipeline_job` (definido em `resources/jobs/etl_pipeline_job.yml`)
orquestra três tasks sequenciais, cada uma executando um notebook:

1. `01_bronze_ingestion.py`
2. `02_silver_transformation.py`
3. `03_gold_aggregation.py`

Cada task depende da anterior (`depends_on`), garantindo que a promoção dos
dados só avance se a etapa anterior for concluída com sucesso — incluindo as
verificações de qualidade de dados aplicadas na camada Silver
(`data_engineering.utils.data_quality`).

## Código reutilizável vs. notebooks

- O código de transformação (lógica de negócio) vive em
  `src/data_engineering/` como funções Python puras, testáveis localmente com
  PySpark (`local[*]`), sem depender de um cluster Databricks.
- Os notebooks em `notebooks/` são finos: apenas orquestram a leitura/escrita
  de tabelas e chamam as funções do pacote `data_engineering`.

Essa separação permite:

- Testar a lógica de negócio via `pytest` no CI, sem custo de cluster.
- Reutilizar as mesmas funções em diferentes pipelines/jobs.
- Empacotar o código como wheel (`setup.py`) e distribuí-lo via Databricks
  Asset Bundles.

## Ambientes

O `databricks.yml` define três targets (`dev`, `staging`, `prod`), cada um com
seu próprio workspace, catálogo e (em staging/prod) service principal de
execução — seguindo o princípio de isolamento entre ambientes.
