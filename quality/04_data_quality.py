# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Qualidade de dados (todas as camadas)
# MAGIC Roda depois da Gold. Cada teste é um SQL que **conta violações** (0 = passou).
# MAGIC - `ERROR`: quebra o job (dado errado não pode chegar ao dashboard)
# MAGIC - `WARN`: só registra (anomalia conhecida do dataset, vale acompanhar)
# MAGIC
# MAGIC Tipos de teste:
# MAGIC - **Unicidade**: chave primária sem duplicado e sem nulo
# MAGIC - **Integridade**: fato sempre aponta para uma dimensão que existe (sem órfão)
# MAGIC - **Reconciliação**: nada se perde/duplica entre camadas
# MAGIC - **Regra de negócio**: valores dentro do esperado
# MAGIC
# MAGIC Resultado de cada execução: `<catalog>.gold.data_quality_log`

# COMMAND ----------

# ===== 1. Configuração =====
# Parâmetro: o Job envia "catalog"; rodando à mão, usa o valor padrão do widget
dbutils.widgets.text("catalog", "workspace", "Catálogo")
CATALOG = dbutils.widgets.get("catalog")
B, S, G = f"{CATALOG}.bronze", f"{CATALOG}.silver", f"{CATALOG}.gold"  # atalhos dos schemas
LOG_TABLE = f"{G}.data_quality_log"

# COMMAND ----------

# ===== 2. Geradores de teste (evitam repetir SQL) =====

def unique_key(table, keys):
    """Violações = linhas com chave nula + linhas a mais por chave repetida."""
    k = ", ".join(keys)
    nulls = " OR ".join(f"{c} IS NULL" for c in keys)
    return f"""SELECT (SELECT COUNT(*) FROM {table} WHERE {nulls})
                    + (SELECT COUNT(*) - COUNT(DISTINCT {k}) FROM {table})"""


def orphans(child, child_col, parent, parent_col):
    """Violações = linhas do filho cuja chave não existe no pai."""
    return f"""SELECT COUNT(*) FROM {child} c
               LEFT ANTI JOIN {parent} p ON c.{child_col} = p.{parent_col}
               WHERE c.{child_col} IS NOT NULL"""


def same_count(t1, t2):
    """Violações = diferença absoluta de linhas entre duas tabelas."""
    return f"SELECT ABS((SELECT COUNT(*) FROM {t1}) - (SELECT COUNT(*) FROM {t2}))"

# COMMAND ----------

# ===== 3. Catálogo de testes =====
# (nome, camada, severidade, sql que devolve o nº de violações)
CHECKS = [
    # --- Bronze: a carga não pode duplicar arquivo (incidente de 28/10: carga batch + Auto Loader) ---
    ("bronze_orders_sem_duplicar_carga", "bronze", "ERROR",
     f"SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {B}.olist_orders"),
    ("bronze_customers_sem_duplicar_carga", "bronze", "ERROR",
     f"SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM {B}.olist_customers"),
    ("bronze_items_sem_duplicar_carga", "bronze", "ERROR",
     f"SELECT COUNT(*) - COUNT(DISTINCT order_id, order_item_id) FROM {B}.olist_order_items"),
    ("bronze_1_arquivo_por_tabela", "bronze", "ERROR",
     f"SELECT COUNT(DISTINCT _source_file) - 1 FROM {B}.olist_orders"),
    ("bronze_sem_dado_resgatado", "bronze", "WARN",
     f"SELECT COUNT(*) FROM {B}.olist_order_reviews WHERE _rescued_data IS NOT NULL"),

    # --- Silver: unicidade ---
    ("silver_pk_orders",      "silver", "ERROR", unique_key(f"{S}.olist_orders", ["order_id"])),
    ("silver_pk_customers",   "silver", "ERROR", unique_key(f"{S}.olist_customers", ["customer_id"])),
    ("silver_pk_products",    "silver", "ERROR", unique_key(f"{S}.olist_products", ["product_id"])),
    ("silver_pk_sellers",     "silver", "ERROR", unique_key(f"{S}.olist_sellers", ["seller_id"])),
    ("silver_pk_order_items", "silver", "ERROR", unique_key(f"{S}.olist_order_items", ["order_id", "order_item_id"])),
    ("silver_pk_payments",    "silver", "ERROR", unique_key(f"{S}.olist_order_payments", ["order_id", "payment_sequential"])),

    # --- Silver: integridade e regras ---
    ("silver_items_sem_pedido",     "silver", "ERROR", orphans(f"{S}.olist_order_items", "order_id", f"{S}.olist_orders", "order_id")),
    ("silver_orders_sem_cliente",   "silver", "ERROR", orphans(f"{S}.olist_orders", "customer_id", f"{S}.olist_customers", "customer_id")),
    ("silver_compra_sem_data",      "silver", "ERROR", f"SELECT COUNT(*) FROM {S}.olist_orders WHERE order_purchase_timestamp IS NULL"),
    ("silver_preco_nao_positivo",   "silver", "ERROR", f"SELECT COUNT(*) FROM {S}.olist_order_items WHERE price <= 0"),
    ("silver_nota_fora_1_5",        "silver", "ERROR", f"SELECT COUNT(*) FROM {S}.olist_order_reviews WHERE review_score NOT BETWEEN 1 AND 5"),
    ("silver_entrega_antes_compra", "silver", "WARN",
     f"SELECT COUNT(*) FROM {S}.olist_orders WHERE order_delivered_customer_date < order_purchase_timestamp"),
    ("silver_ultima_execucao_ok",   "silver", "ERROR",
     f"""SELECT COUNT(*) FROM (
             SELECT table_name, MAX_BY(status, started_at) AS last_status
             FROM {S}.transformation_log GROUP BY table_name)
         WHERE last_status <> 'SUCCESS'"""),

    # --- Gold: unicidade ---
    ("gold_pk_dim_customers",    "gold", "ERROR", unique_key(f"{G}.dim_customers", ["customer_id"])),
    ("gold_pk_dim_products",     "gold", "ERROR", unique_key(f"{G}.dim_products", ["product_id"])),
    ("gold_pk_dim_sellers",      "gold", "ERROR", unique_key(f"{G}.dim_sellers", ["seller_id"])),
    ("gold_pk_dim_date",         "gold", "ERROR", unique_key(f"{G}.dim_date", ["date_key"])),
    ("gold_pk_fact_orders",      "gold", "ERROR", unique_key(f"{G}.fact_orders", ["order_id"])),
    ("gold_pk_fact_order_items", "gold", "ERROR", unique_key(f"{G}.fact_order_items", ["order_id", "order_item_id"])),
    ("gold_pk_customer_rfm",     "gold", "ERROR", unique_key(f"{G}.customer_rfm", ["customer_unique_id"])),

    # --- Gold: integridade do modelo estrela (fato -> dimensão) ---
    ("gold_fk_items_produto",   "gold", "ERROR", orphans(f"{G}.fact_order_items", "product_id", f"{G}.dim_products", "product_id")),
    ("gold_fk_items_vendedor",  "gold", "ERROR", orphans(f"{G}.fact_order_items", "seller_id", f"{G}.dim_sellers", "seller_id")),
    ("gold_fk_items_pedido",    "gold", "ERROR", orphans(f"{G}.fact_order_items", "order_id", f"{G}.fact_orders", "order_id")),
    ("gold_fk_orders_cliente",  "gold", "ERROR", orphans(f"{G}.fact_orders", "customer_id", f"{G}.dim_customers", "customer_id")),
    ("gold_fk_orders_data",     "gold", "ERROR", orphans(f"{G}.fact_orders", "purchase_date_key", f"{G}.dim_date", "date_key")),

    # --- Reconciliação entre camadas ---
    ("recon_orders_silver_gold",    "recon", "ERROR", same_count(f"{S}.olist_orders", f"{G}.fact_orders")),
    ("recon_items_silver_gold",     "recon", "ERROR", same_count(f"{S}.olist_order_items", f"{G}.fact_order_items")),
    ("recon_customers_silver_gold", "recon", "ERROR", same_count(f"{S}.olist_customers", f"{G}.dim_customers")),
    ("recon_receita_silver_gold",   "recon", "ERROR",       # soma de preço não pode mudar entre camadas
     f"""SELECT CAST(ABS((SELECT SUM(price) FROM {S}.olist_order_items)
                       - (SELECT SUM(price) FROM {G}.fact_order_items)) > 0.01 AS INT)"""),
    ("recon_gmv_mart_vs_fato",      "recon", "ERROR",       # mart mensal soma o mesmo que o fato
     f"""SELECT CAST(ABS((SELECT SUM(gmv) FROM {G}.agg_sales_monthly)
                       - (SELECT SUM(order_value) FROM {G}.fact_orders WHERE is_valid_sale)) > 1 AS INT)"""),

    # --- Gold: regras de negócio ---
    ("gold_valor_pedido_negativo", "gold", "ERROR", f"SELECT COUNT(*) FROM {G}.fact_orders WHERE order_value < 0"),
    ("gold_venda_sem_item",        "gold", "WARN",      # pedido válido sem item = buraco do dataset original
     f"SELECT COUNT(*) FROM {G}.fact_orders WHERE is_valid_sale AND items_qty = 0"),
    ("gold_pago_diferente_do_valor", "gold", "WARN",    # pago vs (itens + frete) com diferença > R$ 1
     f"SELECT COUNT(*) FROM {G}.fact_orders WHERE ABS(payment_value - order_value) > 1"),
    ("gold_cliente_sem_coordenada", "gold", "WARN",
     f"SELECT COUNT(*) FROM {G}.dim_customers WHERE customer_lat IS NULL"),
]

# COMMAND ----------

# ===== 4. Tabela de resultados =====
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    run_id        STRING,     -- 1 id por execução (agrupa os testes)
    check_name    STRING,     -- nome do teste
    layer         STRING,     -- bronze / silver / gold / recon
    severity      STRING,     -- ERROR ou WARN
    violations    BIGINT,     -- nº de linhas que violam a regra
    passed        BOOLEAN,    -- violations = 0
    error_message STRING,     -- se o próprio teste quebrou
    checked_at    TIMESTAMP
)
COMMENT 'Resultado dos testes de qualidade de dados (bronze, silver, gold)'
""")

LOG_SCHEMA = (
    "run_id STRING, check_name STRING, layer STRING, severity STRING, violations BIGINT, "
    "passed BOOLEAN, error_message STRING, checked_at TIMESTAMP"
)

# COMMAND ----------

# ===== 5. Execução =====
import uuid
from datetime import datetime

run_id = str(uuid.uuid4())                                   # identifica esta rodada
results = []
for name, layer, severity, sql in CHECKS:
    try:
        violations = spark.sql(sql).collect()[0][0] or 0     # 1ª linha, 1ª coluna; NULL -> 0
        error = None
    except Exception as e:                                   # teste com erro de SQL conta como falha
        violations, error = None, str(e)[:500]
    passed = violations == 0
    results.append((run_id, name, layer, severity, violations, passed, error, datetime.now()))
    icon = "OK  " if passed else ("FAIL" if severity == "ERROR" else "WARN")
    print(f"[{icon}] {layer:<6} {name:<35} violações={violations}")

spark.createDataFrame(results, LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TABLE)

# COMMAND ----------

# ===== 6. Veredito: ERROR quebra o job =====
errors = [r[1] for r in results if r[3] == "ERROR" and not r[5]]
warns  = [r[1] for r in results if r[3] == "WARN" and not r[5]]
print(f"\n{len(results)} testes | {len(errors)} erros | {len(warns)} avisos")
if errors:
    raise Exception(f"Qualidade reprovada: {errors}")

# COMMAND ----------

# Resultado da última execução
display(spark.sql(f"""
    SELECT check_name, layer, severity, violations, passed
    FROM {CATALOG}.gold.data_quality_log
    WHERE run_id = (SELECT MAX_BY(run_id, checked_at) FROM {CATALOG}.gold.data_quality_log)
    ORDER BY passed, severity, layer
"""))
