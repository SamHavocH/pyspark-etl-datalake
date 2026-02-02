# 📦 PySpark ETL Data Lake

Pipeline de ETL incremental em estilo produção, construído com PySpark em Linux, ingerindo dados de uma API pública para um Data Lake em Parquet particionado e disponibilizando analytics via DuckDB.

Este projeto demonstra conceitos centrais de engenharia de dados, como processamento incremental, qualidade de dados, escrita idempotente, gerenciamento de estado (watermark) e automação em ambiente Linux.

## 🏗️ Visão Geral da Arquitetura

```
API Pública
  │
  ▼
Extract (Python)
  │
  ▼
Transform (PySpark)
  │
  ▼
Data Lake Parquet (particionado por data)
  │
  ├─ DuckDB VIEW (leitura direta do lake)
  └─ DuckDB TABLE (camada de serving / analytics)
```

- **Data Lake**: arquivos Parquet particionados por date_utc
- **Camada de Serving**: DuckDB lendo diretamente do lake
- **Gerenciamento de Estado**: watermark em state.json
- **Automação**: script Linux e agendamento opcional via systemd

## ⚙️ Stack Tecnológica

- Python 3.11+
- PySpark (modo local)
- DuckDB
- Parquet
- Linux (Ubuntu)
- systemd (opcional)
- Jupyter Notebook (demo analítica)

## 📂 Estrutura do Projeto

```
pyspark-etl-datalake/
├─ src/
│  └─ etl/
│     ├─ pipeline.py
│     ├─ spark.py
│     ├─ extract.py
│     ├─ transform_spark.py
│     ├─ load_spark.py
│     ├─ duckdb_serving.py
│     ├─ quality.py
│     └─ state.py
├─ scripts/
│  └─ run_etl.sh
├─ notebooks/
│  └─ duckdb_analysis.ipynb
├─ data/
│  ├─ processed/
│  └─ state.json
├─ logs/
├─ .env.example
├─ requirements.txt
└─ README.md
```

## 🔁 Processamento Incremental

O pipeline utiliza uma estratégia incremental baseada em watermark:

- O timestamp da última execução bem-sucedida é armazenado em `data/state.json`
- Cada execução processa apenas dados novos desde o último run
- Um overlap configurável garante segurança contra dados atrasados ou revisados
- As partições Parquet são sobrescritas dinamicamente (idempotência por partição)
- Caso o pipeline falhe antes da conclusão, o watermark não é atualizado

## ✅ Qualidade de Dados

Antes da escrita dos dados, o pipeline executa verificações de qualidade:

- O dataset não pode estar vazio
- `ts_utc` não pode ser nulo
- Umidade deve estar entre 0 e 100
- Precipitação não pode ser negativa
- Temperatura deve estar dentro de um intervalo plausível

Se qualquer regra falhar, o job é interrompido e o estado anterior é preservado.

## ▶️ Como Executar

### 1. Criar ambiente virtual

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

### 3. Executar o pipeline

```bash
./scripts/run_etl.sh
```

## 📊 Consultando os Dados (DuckDB)

### Total de registros

```sql
SELECT COUNT(*) FROM weather_hourly;
```

### Temperatura média por dia

```sql
SELECT
  date_utc,
  AVG(temperature_2m) AS avg_temp
FROM weather_hourly
GROUP BY 1
ORDER BY 1;
```

### Maiores eventos de precipitação

```sql
SELECT
  ts_utc,
  precipitation
FROM weather_hourly
ORDER BY precipitation DESC NULLS LAST
LIMIT 10;
```

## 📓 Notebook Analítico

Um notebook Jupyter está disponível em `notebooks/` demonstrando consultas e visualizações sobre os dados curados usando DuckDB.

O notebook é opcional e não faz parte do pipeline de produção.