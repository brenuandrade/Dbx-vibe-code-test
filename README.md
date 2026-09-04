# DBX Vibe Code Test — Projeto de Engenharia de Dados (Databricks)

Projeto de engenharia de dados construído para rodar no **Databricks**, usando
**Databricks Asset Bundles (DABs)** para deploy, **PySpark** para as transformações
e a **arquitetura medalhão** (Bronze → Silver → Gold) para organização dos dados.

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

O `databricks.yml` define os targets `dev`, `staging` e `prod`. Cada um aponta
para um workspace/host diferente e usa o arquivo de configuração correspondente
em `conf/`.

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
