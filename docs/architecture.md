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

O job roda em cadência near-time (a cada 15 minutos, `pause_status: PAUSED`
por padrão) — pensado para acompanhar o andamento de um evento como a Black
Friday em quase tempo real; fora da janela do evento, o job deve ficar
pausado (ou usar uma cadência diária) para evitar custo desnecessário.

## Dados sintéticos: cenário Black Friday

A camada Bronze não lê de uma fonte externa real — ela gera um dataset
sintético de varejo (ver `src/data_engineering/bronze/synthetic_data.py`)
com cinco entidades: `products`, `stores`, `customers`, `addresses` e
`orders`. A geração é determinística por `seed`, o que facilita comparar
execuções e depurar a Silver.

`orders` está no grão de **item de pedido** (uma linha por produto dentro de
um pedido, não por pedido) — o formato típico de um tópico de eventos de
checkout — com dois timestamps distintos:

- `order_timestamp`: quando o evento de fato ocorreu, concentrado no dia da
  Black Friday com um padrão de tráfego realista (pico à meia-noite, vale
  matinal, segundo pico no fim da tarde).
- `source_ingested_at`: quando o *sistema de origem* capturou o evento
  (`order_timestamp` + latência simulada — a maioria em segundos, uma cauda
  longa de minutos). **Não confundir** com a coluna de auditoria genérica
  `_ingested_at`, adicionada por `bronze.ingest.ingest_raw_source` e que
  reflete quando *este pipeline* gravou a linha na Bronze.

### Ruído

`src/data_engineering/bronze/noise.py` concentra os utilitários de ruído,
combinados pelos geradores de cada entidade:

- **Valores ausentes**: `brand` (produtos), `phone`/`gender` (consumidores),
  `payment_method` (pedidos).
- **Formatos inconsistentes**: e-mails, telefones e CEPs com variações de
  formatação; `unit_price` sempre representado como string "crua" — ora
  formatada em pt-BR (`"R$ 1.234,56"`), ora plana (`"199.90"`), ocasionalmente
  negativa por erro de digitação (ver `dirty_currency`/`parse_dirty_price`).
- **Duplicatas de reenvio**: simulam falha de idempotência de sistemas
  upstream sob carga — mais frequentes em `orders`, refletindo o pico da
  Black Friday.
- **Referências órfãs**: FKs apontando para IDs inexistentes (ex.: endereço
  com `customer_id` que ainda não foi propagado; pedido com `product_id`
  desconhecido) — inconsistência referencial comum entre sistemas de origem.
- **Anomalias pontuais**: quantidade negativa (erro de leitor/scanner),
  valores de linha "fat-finger" (magnitude muito acima do esperado), status
  com caixa/espaçamento inconsistentes.

A limpeza desse ruído é responsabilidade da camada Silver — hoje ela trata
duplicidade, nulos e espaçamento (`data_engineering.silver.transform`); o
parsing de preços "sujos" (`parse_dirty_price`) e o tratamento de referências
órfãs ainda são pontos de evolução natural do pipeline.

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
execução — seguindo o princípio de isolamento entre ambientes. Atualmente
apenas `dev` aponta para um workspace real
(`dbc-fc266d3f-2a0d.cloud.databricks.com`); `staging`/`prod` seguem como
placeholders até que esses workspaces existam.
