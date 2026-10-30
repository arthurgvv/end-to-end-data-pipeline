# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: limpeza e padronização
# MAGIC Regras desta camada:
# MAGIC - Lê a Bronze (tudo STRING) e aplica o tipo certo (cast)
# MAGIC - Padroniza textos: trim, vazio -> NULL, cidade minúscula sem acento, UF maiúscula, CEP com 5 dígitos
# MAGIC - Remove duplicados pela chave (fica a linha mais recente da Bronze)
# MAGIC - Remove linhas inválidas: chave nula, preço <= 0, nota fora de 1-5, coordenada fora do Brasil
# MAGIC - Recarga completa (overwrite): rodar 2x dá o mesmo resultado
# MAGIC - Regra de negócio e agregação ficam na Gold

# COMMAND ----------

# ===== 1. Configuração =====
CATALOG = "workspace"                                    # catálogo do Unity Catalog
BRONZE = f"{CATALOG}.bronze"                             # schema de origem
SILVER = f"{CATALOG}.silver"                             # schema de destino
LOG_TABLE = f"{SILVER}.transformation_log"               # tabela de log da Silver
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"                        # formato das datas no Olist (ex: 2017-10-02 10:56:33)
META = ["_source_file", "_ingestion_timestamp"]          # metadados da Bronze que seguem para a Silver (linhagem)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")       # cria o schema silver, se não existir

# COMMAND ----------

# ===== 2. Funções auxiliares =====
from datetime import datetime                            # marca início/fim da carga
from pyspark.sql import functions as F                   # F.col, F.trim, F.when... (F = apelido)
from pyspark.sql.window import Window                    # janela: usada no dedup

# Letras com acento e sua versão sem acento (mesma posição = troca)
ACCENTS    = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
NO_ACCENTS = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


def fix_header(df):
    """Remove o BOM (\\ufeff) que alguns CSVs deixam no nome da 1ª coluna."""
    return df.toDF(*[c.replace("﻿", "") for c in df.columns])  # toDF = renomeia todas as colunas


def clean_strings(df):
    """Trim em toda coluna STRING. Texto vazio vira NULL."""
    for name, dtype in df.dtypes:                        # dtypes = lista de (nome, tipo)
        if dtype == "string":                            # só mexe em texto
            trimmed = F.trim(F.col(name))                # tira espaço do início e do fim
            df = df.withColumn(name, F.when(trimmed == "", None).otherwise(trimmed))  # "" -> NULL
    return df


# Casts com try_: valor inválido vira NULL em vez de quebrar o job
def to_int(c):    return F.expr(f"try_cast({c} AS INT)")              # texto -> inteiro
def to_double(c): return F.expr(f"try_cast({c} AS DOUBLE)")           # texto -> decimal flutuante (lat/lng)
def to_dec(c):    return F.expr(f"try_cast({c} AS DECIMAL(10,2))")    # texto -> dinheiro (2 casas, exato)
def to_ts(c):     return F.expr(f"try_to_timestamp({c}, '{TS_FORMAT}')")  # texto -> data/hora

# Padronizações de texto
def norm_city(c):  return F.translate(F.lower(F.col(c)), ACCENTS, NO_ACCENTS)  # "São Paulo" -> "sao paulo"
def norm_state(c): return F.upper(F.col(c))                                    # "sp" -> "SP"
def norm_zip(c):   return F.lpad(F.col(c), 5, "0")                             # "1037" -> "01037"


def dedup(df, keys):
    """Mantém 1 linha por chave: a mais recente da Bronze."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingestion_timestamp").desc())  # grupo = chave; mais nova primeiro
    return (
        df.withColumn("_rn", F.row_number().over(w))     # numera 1, 2, 3... dentro de cada chave
        .filter(F.col("_rn") == 1)                       # fica só a 1ª (mais recente)
        .drop("_rn")                                     # remove a coluna auxiliar
    )

# COMMAND ----------

# ===== 3. Transformações por tabela =====
# Cada função recebe a Bronze já limpa (trim) e devolve: colunas tipadas + filtro de inválidos.
# *META = "desempacota" a lista: vira "_source_file", "_ingestion_timestamp" dentro do select


def silver_customers(df):
    return df.select(
        "customer_id",                                                    # id do cliente no pedido
        "customer_unique_id",                                             # id real da pessoa (repete entre pedidos)
        norm_zip("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
        norm_city("customer_city").alias("customer_city"),
        norm_state("customer_state").alias("customer_state"),
        *META,
    )


def silver_geolocation(df):
    df = df.select(
        norm_zip("geolocation_zip_code_prefix").alias("geolocation_zip_code_prefix"),
        to_double("geolocation_lat").alias("geolocation_lat"),
        to_double("geolocation_lng").alias("geolocation_lng"),
        norm_city("geolocation_city").alias("geolocation_city"),
        norm_state("geolocation_state").alias("geolocation_state"),
        *META,
    )
    # Olist tem coordenadas fora do Brasil (erro de cadastro): fica só o que cai no país
    return df.filter(
        F.col("geolocation_lat").between(-35, 6)          # latitude do Brasil
        & F.col("geolocation_lng").between(-75, -34)      # longitude do Brasil
    )


def silver_order_items(df):
    df = df.select(
        "order_id",
        to_int("order_item_id").alias("order_item_id"),                   # posição do item no pedido (1, 2, 3...)
        "product_id",
        "seller_id",
        to_ts("shipping_limit_date").alias("shipping_limit_date"),        # prazo para o vendedor despachar
        to_dec("price").alias("price"),
        to_dec("freight_value").alias("freight_value"),
        *META,
    )
    return df.filter((F.col("price") > 0) & (F.col("freight_value") >= 0))  # preço positivo, frete não negativo


def silver_order_payments(df):
    df = df.select(
        "order_id",
        to_int("payment_sequential").alias("payment_sequential"),         # 1º, 2º pagamento do mesmo pedido
        F.lower(F.col("payment_type")).alias("payment_type"),             # credit_card, boleto, voucher...
        to_int("payment_installments").alias("payment_installments"),     # nº de parcelas
        to_dec("payment_value").alias("payment_value"),
        *META,
    )
    return df.filter(F.col("payment_value") >= 0)                        # voucher pode ser 0; negativo não


def silver_order_reviews(df):
    df = df.select(
        "review_id",
        "order_id",
        to_int("review_score").alias("review_score"),
        "review_comment_title",
        "review_comment_message",
        to_ts("review_creation_date").alias("review_creation_date"),
        to_ts("review_answer_timestamp").alias("review_answer_timestamp"),
        *META,
    )
    return df.filter(F.col("review_score").between(1, 5))                 # nota válida: 1 a 5


def silver_orders(df):
    return df.select(
        "order_id",
        "customer_id",
        F.lower(F.col("order_status")).alias("order_status"),             # delivered, shipped, canceled...
        to_ts("order_purchase_timestamp").alias("order_purchase_timestamp"),          # compra
        to_ts("order_approved_at").alias("order_approved_at"),                        # pagamento aprovado
        to_ts("order_delivered_carrier_date").alias("order_delivered_carrier_date"),  # entregue à transportadora
        to_ts("order_delivered_customer_date").alias("order_delivered_customer_date"),# entregue ao cliente
        to_ts("order_estimated_delivery_date").alias("order_estimated_delivery_date"),# prazo prometido
        *META,
    )


def silver_category_translation(df):
    return df.select(
        F.lower(F.col("product_category_name")).alias("product_category_name"),
        F.lower(F.col("product_category_name_english")).alias("product_category_name_english"),
        *META,
    )


def silver_products(df):
    df = df.select(
        "product_id",
        F.coalesce(F.col("product_category_name"), F.lit("sem_categoria"))   # coalesce = 1º valor não nulo
         .alias("product_category_name"),
        to_int("product_name_lenght").alias("product_name_length"),          # corrige o typo "lenght" do dataset
        to_int("product_description_lenght").alias("product_description_length"),
        to_int("product_photos_qty").alias("product_photos_qty"),
        to_int("product_weight_g").alias("product_weight_g"),
        to_int("product_length_cm").alias("product_length_cm"),
        to_int("product_height_cm").alias("product_height_cm"),
        to_int("product_width_cm").alias("product_width_cm"),
        *META,
    )
    # Tradução da categoria (lê a Silver: a tabela de tradução roda antes no loop)
    tr = spark.table(f"{SILVER}.product_category_name_translation").select(
        "product_category_name", "product_category_name_english"          # só as 2 colunas (evita META duplicado)
    )
    df = df.join(tr, on="product_category_name", how="left")             # left = mantém produto sem tradução
    return df.withColumn(
        "product_category_name_english",
        F.coalesce(F.col("product_category_name_english"), F.col("product_category_name")),  # sem tradução: usa o PT
    )


def silver_sellers(df):
    return df.select(
        "seller_id",
        norm_zip("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
        norm_city("seller_city").alias("seller_city"),
        norm_state("seller_state").alias("seller_state"),
        *META,
    )

# COMMAND ----------

# ===== 4. Mapa tabela -> chave -> função =====
# "fn" guarda a função em si (sem parênteses): ela só roda no loop
# keys = colunas que identificam 1 linha única. None = a linha inteira
# A tradução vem ANTES de products (products lê dela)
TABLES = [
    {"table": "olist_customers",                   "keys": ["customer_id"],                       "fn": silver_customers},
    {"table": "olist_geolocation",                 "keys": None,                                  "fn": silver_geolocation},
    {"table": "olist_order_items",                 "keys": ["order_id", "order_item_id"],         "fn": silver_order_items},
    {"table": "olist_order_payments",              "keys": ["order_id", "payment_sequential"],    "fn": silver_order_payments},
    {"table": "olist_order_reviews",               "keys": ["review_id", "order_id"],             "fn": silver_order_reviews},
    {"table": "olist_orders",                      "keys": ["order_id"],                          "fn": silver_orders},
    {"table": "product_category_name_translation", "keys": ["product_category_name"],             "fn": silver_category_translation},
    {"table": "olist_products",                    "keys": ["product_id"],                        "fn": silver_products},
    {"table": "olist_sellers",                     "keys": ["seller_id"],                         "fn": silver_sellers},
]

# COMMAND ----------

# ===== 5. Tabela de log =====
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    table_name    STRING,     -- tabela processada
    bronze_rows   BIGINT,     -- linhas lidas da Bronze
    silver_rows   BIGINT,     -- linhas gravadas na Silver
    removed_rows  BIGINT,     -- bronze - silver (duplicados + inválidos)
    status        STRING,     -- SUCCESS ou FAILED
    error_message STRING,     -- erro, se houver
    started_at    TIMESTAMP,  -- início
    finished_at   TIMESTAMP   -- fim
)
COMMENT 'Log de execucoes da transformacao Silver'
""")

# Schema usado para gravar cada linha de log
LOG_SCHEMA = (
    "table_name STRING, bronze_rows BIGINT, silver_rows BIGINT, removed_rows BIGINT, "
    "status STRING, error_message STRING, started_at TIMESTAMP, finished_at TIMESTAMP"
)

# COMMAND ----------

# ===== 6. Execução: Bronze -> Silver =====
for t in TABLES:
    src = f"{BRONZE}.{t['table']}"                       # ex: workspace.bronze.olist_orders
    dst = f"{SILVER}.{t['table']}"                       # ex: workspace.silver.olist_orders
    started_at = datetime.now()                          # início desta tabela
    bronze_rows, silver_rows, removed = None, None, None # valores padrão (caso falhe cedo)
    try:
        df = clean_strings(fix_header(spark.table(src))) # lê Bronze + corrige header + trim
        bronze_rows = df.count()                         # linhas na Bronze

        df = t["fn"](df)                                 # chama a função da tabela: cast + padroniza + filtra
        keys = t["keys"] or [c for c in df.columns if c not in META]  # None -> todas as colunas de negócio
        df = df.dropna(subset=keys)                      # chave nula: descarta
        df = dedup(df, keys)                             # 1 linha por chave
        df = df.withColumn("_silver_timestamp", F.current_timestamp())  # quando entrou na Silver

        (
            df.write.format("delta")                     # formato Delta Lake
            .mode("overwrite")                           # recarga completa: substitui a tabela
            .option("overwriteSchema", True)             # aceita mudança de colunas/tipos
            .saveAsTable(dst)                            # grava no Unity Catalog
        )

        silver_rows = spark.table(dst).count()           # linhas gravadas
        removed = bronze_rows - silver_rows              # quanto foi descartado
        spark.sql(f"COMMENT ON TABLE {dst} IS 'Silver: {t['table']} tipada, padronizada e sem duplicados'")
        status, error = "SUCCESS", None
        print(f"{t['table']}: {bronze_rows} -> {silver_rows} (removidas {removed})")
    except Exception as e:                               # 1 tabela falhou: registra e segue
        status, error = "FAILED", str(e)[:1000]
        print(f"{t['table']}: FALHOU -> {error[:200]}")

    # Grava 1 linha no log
    log_row = [(t["table"], bronze_rows, silver_rows, removed, status, error, started_at, datetime.now())]
    spark.createDataFrame(log_row, LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validações

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Última execução: todas SUCCESS?
# MAGIC SELECT * FROM workspace.silver.transformation_log
# MAGIC ORDER BY started_at DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tipos: datas devem ser TIMESTAMP (não mais STRING)
# MAGIC DESCRIBE workspace.silver.olist_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Chave única: linhas = chaves distintas
# MAGIC -- Esperado (dataset original): orders 99.441 | customers 99.441 | products 32.951 | sellers 3.095
# MAGIC SELECT 'olist_orders' AS tabela, COUNT(*) AS linhas, COUNT(DISTINCT order_id) AS chaves FROM workspace.silver.olist_orders
# MAGIC UNION ALL
# MAGIC SELECT 'olist_customers', COUNT(*), COUNT(DISTINCT customer_id) FROM workspace.silver.olist_customers
# MAGIC UNION ALL
# MAGIC SELECT 'olist_products', COUNT(*), COUNT(DISTINCT product_id) FROM workspace.silver.olist_products
# MAGIC UNION ALL
# MAGIC SELECT 'olist_sellers', COUNT(*), COUNT(DISTINCT seller_id) FROM workspace.silver.olist_sellers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cast falhou? try_cast vira NULL. Data de compra nunca pode ser nula.
# MAGIC -- Esperado: 0
# MAGIC SELECT COUNT(*) AS compras_sem_data
# MAGIC FROM workspace.silver.olist_orders
# MAGIC WHERE order_purchase_timestamp IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Órfãos: item sem pedido correspondente. Esperado: 0
# MAGIC SELECT COUNT(*) AS itens_orfaos
# MAGIC FROM workspace.silver.olist_order_items i
# MAGIC LEFT ANTI JOIN workspace.silver.olist_orders o
# MAGIC   ON i.order_id = o.order_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Padronização: CEP com 5 dígitos, UF maiúscula, cidade sem acento
# MAGIC SELECT customer_zip_code_prefix, customer_city, customer_state
# MAGIC FROM workspace.silver.olist_customers
# MAGIC WHERE customer_city LIKE 'sao paulo%'
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Categorias sem tradução (usam o nome em PT como fallback)
# MAGIC SELECT DISTINCT product_category_name, product_category_name_english
# MAGIC FROM workspace.silver.olist_products
# MAGIC WHERE product_category_name = product_category_name_english

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Teste de idempotência
# MAGIC Rode a etapa 6 de novo.
# MAGIC Esperado: mesmos números de `silver_rows` no log. Nada duplica (overwrite).
