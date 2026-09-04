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

## Privacidade na camada Gold

`gold.customer_summary` (`data_engineering.gold.aggregate.build_customer_summary`)
combina dados cadastrais do cliente (`full_name`, `email`, `cpf`) com
métricas agregadas de pedidos (`total_orders`, `total_spent`,
`last_order_at`). As três colunas cadastrais são PII e precisam de controle
de acesso diferenciado — é exatamente o tipo de tabela onde um time de BI
precisa das métricas agregadas, mas não deveria enxergar CPF/e-mail/nome em
claro por padrão.

`data_engineering.gold.privacy` implementa esse controle via **Unity
Catalog Column Masks** — mascaramento no nível da coluna, não da tabela:

1. `build_mask_function_sql` monta a DDL de três funções SQL
   (`mask_full_name`, `mask_email`, `mask_cpf`), uma por coluna PII. Cada
   função usa `is_account_group_member(admin_group)`: membros do grupo
   administrativo veem o valor real; os demais veem uma versão ofuscada
   (ex.: primeiro nome + `"***"`; domínio do e-mail preservado, local-part
   mascarado; CPF com só os 2 últimos dígitos visíveis).
2. `build_apply_mask_sql` monta os `ALTER TABLE ... ALTER COLUMN ... SET
   MASK ...`, um por coluna PII presente na tabela (colunas sem PII
   conhecida — como `total_orders` — são ignoradas).
3. `build_grant_sql` concede `SELECT` na tabela aos grupos de leitura
   (`analyst_group` etc.), sempre incluindo o `admin_group`.
4. `apply_privacy_layer` orquestra os três passos via `spark.sql(...)`.

A separação entre "montar SQL" (funções puras, testáveis sem cluster) e
"executar SQL" (`apply_privacy_layer`, que depende de um workspace com
Unity Catalog) é proposital: os testes unitários (`tests/test_privacy.py`)
validam a DDL gerada com um dublê de `SparkSession`, sem precisar de um
catálogo real. A aplicação de fato (`ALTER TABLE ... SET MASK`,
`is_account_group_member`) só funciona em um workspace Databricks com
Unity Catalog habilitado — não roda em Spark OSS local.

O notebook `03_gold_aggregation.py` chama `apply_privacy_layer` logo após
gravar `customer_summary`, usando os widgets `admin_group`/`analyst_group`
(também expostos como variáveis do bundle em `databricks.yml`). O
mascaramento é aplicado a cada execução (idempotente via `CREATE OR REPLACE
FUNCTION`/`SET MASK`), então funciona tanto no primeiro deploy quanto em
execuções recorrentes.

## Simulação de estresse: skew e picos de ingestão

`data_engineering.gold.simulate` traz duas funções para testar a Gold sob
condições adversas comuns na Black Friday, antes do dia real:

- **`simulate_data_skew`**: sorteia uma fração pequena das chaves de uma
  coluna (ex.: `product_id`) e replica as linhas correspondentes,
  concentrando volume nelas — reproduz o cenário de um item "viral" de
  campanha-relâmpago, que gera partições de shuffle muito maiores que as
  demais em `groupBy`/`join` por essa chave (a causa clássica de
  stragglers/OOM em Spark sob tráfego desbalanceado).
- **`simulate_ingestion_spike`**: replica as linhas cuja coluna de
  timestamp cai dentro de uma janela curta configurável, simulando a
  explosão de tráfego da abertura de uma campanha (ex.: meia-noite da Black
  Friday). Quando a coluna `source_ingested_at` está presente, também
  adiciona um atraso extra às linhas do pico, refletindo o backlog de fila
  que aparece sob carga muito acima do normal.

Ambas retornam um DataFrame comum — podem alimentar qualquer agregação da
Gold (ex.: `build_near_time_sales_summary`) para validar se ela (e o
cluster/autoscaling do job) aguenta o cenário. O notebook
`03_gold_aggregation.py` roda esse teste de estresse quando o widget
`run_stress_test=true`, gravando o resultado em uma tabela
`*_stress_test` separada — nunca sobrescrevendo a métrica "de verdade".

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

O `databricks.yml` define três targets (`dev`, `staging`, `prod`). `dev` e
`prod` compartilham hoje o único workspace real disponível
(`dbc-fc266d3f-2a0d.cloud.databricks.com`), isolados entre si pelo catálogo
do Unity Catalog (`dev`/`prod`) e pelo `root_path` — não há, por ora, um
service principal dedicado para `prod`: o deploy roda com a identidade do
token configurado (`DBX_SECRET_TRIAL`). `staging` segue como placeholder até
que um workspace próprio exista. Quando um workspace de produção separado
(ou um service principal dedicado) estiver disponível, atualize o target
`prod` de acordo — a separação por catálogo é um ponto de partida razoável
para o estágio atual do projeto, não a configuração final recomendada para
produção em Databricks.
