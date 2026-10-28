# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze v2: ingestão incremental com Auto Loader
# MAGIC Regras desta camada:
# MAGIC - Dado como chegou: tudo STRING
# MAGIC - Metadados de origem: `_source_file`, `_ingestion_timestamp`
# MAGIC - Incremental: só arquivos novos, sem duplicar
# MAGIC - Nada de limpeza (isso é Silver)

# COMMAND ----------

# ===== 1. Configuração =====
CATALOG = "workspace"                                          # catálogo do Unity Catalog
SCHEMA = "bronze"                                              # schema da camada Bronze
RAW_PATH = "/Volumes/workspace/bronze/raw_files/archive/"      # pasta dos CSVs (corrige o caminho sem archive/)
CHECKPOINT_BASE = "/Volumes/workspace/bronze/checkpoints/"     # onde o Auto Loader guarda o progresso
LOG_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_log"                # tabela de log das cargas

# Cria o Volume de checkpoints, se ainda não existir
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# Confere se os 9 CSVs estão na pasta
display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# ===== 2. Mapa arquivo -> tabela =====
# Lista de dicionários: cada item = 1 fonte
# multiline=True só onde o texto tem quebra de linha (reviews)
SOURCES = [
    {"file": "olist_customers_dataset.csv",           "table": "olist_customers",                   "multiline": False},
    {"file": "olist_geolocation_dataset.csv",         "table": "olist_geolocation",                 "multiline": False},
    {"file": "olist_order_items_dataset.csv",         "table": "olist_order_items",                 "multiline": False},
    {"file": "olist_order_payments_dataset.csv",      "table": "olist_order_payments",              "multiline": False},
    {"file": "olist_order_reviews_dataset.csv",       "table": "olist_order_reviews",               "multiline": True},
    {"file": "olist_orders_dataset.csv",              "table": "olist_orders",                      "multiline": False},
    {"file": "olist_products_dataset.csv",            "table": "olist_products",                    "multiline": False},
    {"file": "olist_sellers_dataset.csv",             "table": "olist_sellers",                     "multiline": False},
    {"file": "product_category_name_translation.csv", "table": "product_category_name_translation", "multiline": False},
]

# COMMAND ----------

# ===== 3. Reset (só na 1ª execução da v2) =====
# Por quê: as tabelas atuais foram criadas em batch (overwrite).
# O checkpoint novo não sabe disso -> leria tudo de novo e duplicaria.
# True = apaga tabelas + checkpoints e recarrega do zero.
# ATENÇÃO: depois da 1ª execução, mude para False.
RESET_BRONZE = True

if RESET_BRONZE:                                               # só roda se o flag estiver ligado
    for src in SOURCES:                                        # percorre cada fonte
        full_name = f"{CATALOG}.{SCHEMA}.{src['table']}"       # nome completo: catalogo.schema.tabela
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")         # apaga a tabela antiga
        try:
            dbutils.fs.rm(CHECKPOINT_BASE + src["table"], True)  # apaga o checkpoint (True = recursivo)
        except Exception:
            pass                                               # checkpoint ainda não existia: ignora
    spark.sql(f"DROP TABLE IF EXISTS {LOG_TABLE}")             # zera o log também
    print("Bronze resetada.")

# COMMAND ----------

# ===== 4. Tabela de log das cargas =====
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    table_name    STRING,     -- tabela carregada
    rows_ingested BIGINT,     -- linhas novas nesta execução
    total_rows    BIGINT,     -- total na tabela após a carga
    status        STRING,     -- SUCCESS ou FAILED
    error_message STRING,     -- erro, se houver
    started_at    TIMESTAMP,  -- início da carga
    finished_at   TIMESTAMP   -- fim da carga
)
COMMENT 'Log de execucoes da ingestao Bronze'
""")

# Schema usado para gravar cada linha de log
LOG_SCHEMA = (
    "table_name STRING, rows_ingested BIGINT, total_rows BIGINT, "
    "status STRING, error_message STRING, started_at TIMESTAMP, finished_at TIMESTAMP"
)

# COMMAND ----------

# ===== 5. Funções de leitura e escrita =====
from datetime import datetime                                  # marca início/fim da carga
from pyspark.sql.functions import col, current_timestamp       # funções de coluna


def read_source(src):
    """Lê 1 CSV com Auto Loader. Retorna um DataFrame de streaming."""
    table = src["table"]                                       # nome da tabela destino
    return (
        spark.readStream                                       # leitura em streaming (incremental)
        .format("cloudFiles")                                  # cloudFiles = Auto Loader
        .option("cloudFiles.format", "csv")                    # formato dos arquivos
        .option("cloudFiles.schemaLocation", CHECKPOINT_BASE + table + "/_schema")  # salva o schema inferido
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # coluna nova no CSV -> entra na tabela
        .option("pathGlobFilter", src["file"])                 # pega só este arquivo da pasta
        .option("header", True)                                # 1ª linha = nomes das colunas
        .option("encoding", "UTF-8")                           # evita acento quebrado
        .option("multiLine", src["multiline"])                 # aceita quebra de linha dentro de aspas
        .option("escape", '"')                                 # aspas no texto vêm como "" (padrão CSV)
        .load(RAW_PATH)                                        # pasta de origem
        # Sem inferSchema: em CSV o Auto Loader lê tudo como STRING
        # Ele cria sozinho a coluna _rescued_data (dado fora do schema)
        .withColumn("_source_file", col("_metadata.file_path"))  # de qual arquivo veio a linha
        .withColumn("_ingestion_timestamp", current_timestamp())       # quando a linha entrou
    )


def write_bronze(df, src):
    """Grava o stream na tabela Delta. Processa só arquivos novos e encerra."""
    table = src["table"]
    query = (
        df.writeStream                                         # escrita em streaming
        .format("delta")                                       # formato Delta Lake
        .outputMode("append")                                  # só adiciona, nunca sobrescreve
        .option("checkpointLocation", CHECKPOINT_BASE + table) # lembra quais arquivos já foram lidos
        .option("mergeSchema", True)                           # aceita colunas novas na tabela
        .trigger(availableNow=True)                            # processa o que tem e para (modo batch)
        .queryName(f"bronze_{table}")                          # nome do job na UI
        .toTable(f"{CATALOG}.{SCHEMA}.{table}")                # tabela destino
    )
    query.awaitTermination()                                   # espera terminar antes de seguir


def count_rows(full_name):
    """Conta linhas da TABELA (não do CSV). 0 se não existir."""
    if spark.catalog.tableExists(full_name):                   # a tabela já existe?
        return spark.table(full_name).count()                  # conta na tabela Delta
    return 0                                                   # 1ª carga: nada antes

# COMMAND ----------

# ===== 6. Execução: carrega todas as fontes =====
for src in SOURCES:
    full_name = f"{CATALOG}.{SCHEMA}.{src['table']}"           # ex: workspace.bronze.olist_orders
    started_at = datetime.now()                                # início desta carga
    try:
        before = count_rows(full_name)                         # linhas antes
        write_bronze(read_source(src), src)                    # lê + grava (incremental)
        total = count_rows(full_name)                          # linhas depois
        rows = total - before                                  # linhas novas = depois - antes
        # Documenta a origem na própria tabela
        spark.sql(f"COMMENT ON TABLE {full_name} IS 'Bronze: {src['file']} (Olist) via Auto Loader'")
        status, error = "SUCCESS", None                        # deu certo
        print(f"{src['table']}: +{rows} linhas (total {total})")
    except Exception as e:                                     # 1 fonte falhou: registra e segue
        # Obs: coluna nova no CSV faz o stream falhar 1 vez; rodar de novo resolve
        rows, total, status, error = 0, None, "FAILED", str(e)[:1000]
        print(f"{src['table']}: FALHOU -> {error[:200]}")

    # Grava 1 linha no log
    log_row = [(src["table"], rows, total, status, error, started_at, datetime.now())]
    spark.createDataFrame(log_row, LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validações

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Última execução de cada tabela
# MAGIC SELECT * FROM workspace.bronze.ingestion_log
# MAGIC ORDER BY started_at DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Todas as colunas devem ser STRING + _rescued_data, _source_file, _ingestion_timestamp
# MAGIC DESCRIBE workspace.bronze.olist_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CEP com zero à esquerda deve aparecer (antes virava int e perdia o zero)
# MAGIC SELECT customer_zip_code_prefix, customer_city, _source_file
# MAGIC FROM workspace.bronze.olist_customers
# MAGIC WHERE customer_zip_code_prefix LIKE '0%'
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reviews: o dataset original tem 99.224 linhas
# MAGIC -- Número maior = multiLine/escape não funcionou
# MAGIC SELECT COUNT(*) AS total,
# MAGIC        COUNT(_rescued_data) AS linhas_resgatadas
# MAGIC FROM workspace.bronze.olist_order_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Teste de idempotência
# MAGIC Rode a etapa 6 de novo (com `RESET_BRONZE = False`).
# MAGIC Esperado: `+0 linhas` em todas. Nada duplica.
