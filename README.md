# DBX Vibe Code Test — Projeto de Engenharia de Dados (Databricks)

Projeto de engenharia de dados construído para rodar no **Databricks**, usando
**Databricks Asset Bundles (DABs)** para deploy, **PySpark** para as transformações
e a **arquitetura medalhão** (Bronze → Silver → Gold) para organização dos dados.

**Workspace:** `dbc-fc266d3f-2a0d.cloud.databricks.com` (target `dev` em `databricks.yml`).

## 🛍️ Cenário: acompanhamento near-time da Black Friday

O pipeline simula o acompanhamento em quase tempo real de um varejo durante a
Black Friday, usando **dados 100% sintéticos** (nenhum dado real é usado).
A camada Bronze gera e ingere cinco entidades:

| Entidade     | Descrição                                                            |
|--------------|-----------------------------------------------------------------------|
| `products`   | Catálogo de produtos (categoria, marca, preço)                       |
| `stores`     | Rede de lojas físicas e digitais                                     |
| `customers`  | Base de consumidores                                                 |
| `addresses`  | Endereços de entrega dos consumidores                                |
| `orders`     | Pedidos, no grão de **item de pedido** — um evento de checkout      |

A tabela `orders` é a mais importante para o acompanhamento near-time: os
timestamps são concentrados no dia da Black Friday (última sexta-feira de
novembro), seguindo um padrão de tráfego realista (pico à meia-noite, vale
pela manhã, segundo pico no fim da tarde), e cada evento carrega
`source_ingested_at` simulando o atraso de ingestão do sistema de origem.

**Os dados incluem ruído proposital**, para se aproximar do mundo real:
valores nulos, e-mails/telefones/CEPs mal formatados, preços ora numéricos
ora como string em formato BRL (`"R$ 1.234,56"`), duplicatas por
reenvio/retry (mais frequentes em `orders`, simulando o pico de carga),
referências órfãs (ex.: pedido apontando para um `customer_id` inexistente)
e status/categorias com caixa e espaçamento inconsistentes. Veja
`src/data_engineering/bronze/synthetic_data.py` (geradores) e
`src/data_engineering/bronze/noise.py` (utilitários de ruído, reutilizáveis).

Para regenerar o dataset com outros volumes, ajuste os widgets do notebook
`01_bronze_ingestion.py` (`n_customers`, `n_products`, `n_stores`, `n_orders`,
`black_friday_year`) ou os parâmetros do job em
`resources/jobs/etl_pipeline_job.yml`.

### 🔒 Privacidade na camada Gold

A tabela `gold.customer_summary` combina dados cadastrais (nome, e-mail,
CPF) com métricas de pedidos. Essas três colunas são PII e ficam protegidas
por **Unity Catalog Column Masks** (`data_engineering.gold.privacy`):

- Membros do grupo `admin_group` (padrão: `admins`) veem o valor real.
- Qualquer outro grupo com `SELECT` na tabela (padrão: `analyst_group` =
  `analysts`) vê o valor ofuscado (ex.: `"João ***"`, `"***@gmail.com"`,
  `"***.***.***-21"`), mas continua enxergando `total_orders`,
  `total_spent`, `loyalty_tier` etc. normalmente — o mascaramento é por
  **coluna**, não por tabela inteira.

O mascaramento é aplicado automaticamente pelo notebook
`03_gold_aggregation.py` logo após gravar a tabela. Ajuste `admin_group`/
`analyst_group` via widgets do notebook ou variáveis do bundle
(`databricks.yml`) para os grupos reais do seu workspace.

### 🔥 Simulação de skew e picos de ingestão

`data_engineering.gold.simulate` traz duas funções para testar a
resiliência do pipeline antes do dia real:

- `simulate_data_skew`: concentra o volume em uma fração pequena de chaves
  (ex.: 2% dos produtos recebendo a maior parte dos pedidos), simulando um
  item "viral" de campanha-relâmpago — o cenário clássico de partição
  desbalanceada em `groupBy`/`join` no Spark.
- `simulate_ingestion_spike`: multiplica o volume de uma janela curta de
  tempo (ex.: 5 minutos na abertura da Black Friday), simulando o pico de
  tráfego típico de meia-noite, incluindo atraso extra de ingestão
  (backlog de fila).

O notebook `03_gold_aggregation.py` roda esses cenários quando o widget
`run_stress_test=true`, gravando o resultado em
`gold.near_time_sales_by_channel_stress_test` — sem afetar a métrica real.

## 🏗️ Arquitetura

```
                 ┌───────────┐      ┌───────────┐      ┌───────────┐
   Fontes  ───▶  │  BRONZE   │ ───▶ │  SILVER   │ ───▶ │   GOLD    │ ───▶ Consumo
 (arquivos,      │ dados     │      │ dados     │      │ dados     │     (BI, ML,
  APIs, CDC)     │ crus      │      │ limpos e  │      │ agregados │      relatórios)
                 │           │      │ validados │      │ e prontos │
                 └───────────┘      └───────────┘      └───────────┘
```

- **Bronze**: ingestão dos dados brutos, sem transformação, com metadados de auditoria.
- **Silver**: limpeza, deduplicação, tipagem e validação de qualidade de dados.
- **Gold**: agregações e modelos de negócio prontos para consumo (dashboards, ML, etc).

## 📁 Estrutura do repositório

```
.
├── databricks.yml                 # Configuração do Databricks Asset Bundle
├── resources/
│   └── jobs/
│       └── etl_pipeline_job.yml   # Definição do Job (workflow) no Databricks
├── src/
│   └── data_engineering/
│       ├── bronze/                # Ingestão de dados crus
│       ├── silver/                # Limpeza e transformação
│       ├── gold/                  # Agregações de negócio
│       └── utils/                 # Utilitários compartilhados (SparkSession, config, logging)
├── notebooks/                     # Notebooks Databricks (formato .py sincronizável via Git)
├── tests/                         # Testes unitários (pytest + PySpark local)
├── conf/                          # Configurações por ambiente (dev/staging/prod)
├── docs/                          # Documentação adicional
└── .github/workflows/             # Pipelines de CI/CD
```

## ⚙️ Pré-requisitos

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) `v0.220+`
- Python `3.10+`
- Acesso a um workspace Databricks configurado (`databricks auth login`)

## 🚀 Como usar

### 1. Instalar dependências de desenvolvimento

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### 2. Rodar os testes localmente

```bash
pytest tests/ -v
```

### 3. Validar o bundle

```bash
databricks bundle validate -t dev
```

### 4. Deploy no Databricks (ambiente de dev)

```bash
databricks bundle deploy -t dev
```

### 5. Executar o pipeline

```bash
databricks bundle run etl_pipeline_job -t dev
```

## 🌍 Ambientes

O `databricks.yml` define os targets `dev`, `staging` e `prod`. `dev` e `prod`
apontam para o único workspace disponível
(`dbc-fc266d3f-2a0d.cloud.databricks.com`), isolados entre si pelo catálogo
do Unity Catalog (`dev` vs `prod`) e pelo `root_path`; `staging` ainda usa
placeholders — atualize o host (e o service principal) quando esse workspace
existir. `prod` roda sem `run_as` dedicado (usa a identidade do token
cadastrado em `DBX_SECRET_TRIAL`) até que um service principal próprio
esteja disponível.

## ✅ Qualidade de dados e testes

- Testes unitários das transformações usam PySpark local (sem necessidade de cluster).
- Validações de qualidade de dados (nulos, duplicidade, schema) são aplicadas na
  camada Silver antes da promoção para Gold.

## 🔄 CI/CD

- **CI** (`.github/workflows/ci.yml`): lint (ruff), formatação (black) e testes (pytest)
  em cada pull request.
- **CD** (`.github/workflows/cd.yml`): deploy automático do bundle para o ambiente
  correspondente ao fazer merge/push na branch principal.

## 📄 Licença

Uso interno / definir conforme necessidade do projeto.
