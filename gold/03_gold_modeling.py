# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: modelo dimensional e marts de negócio
# MAGIC Regras desta camada:
# MAGIC - Lê só a Silver (dado já tipado, limpo e sem duplicados)
# MAGIC - **Modelo estrela**: dimensões (`dim_*`) + fatos (`fact_*`) para análise livre
# MAGIC - **Marts** (`agg_*`, `customer_rfm`): respostas prontas para dashboard
# MAGIC - Regras de negócio moram aqui (venda válida, atraso, região, RFM...)
# MAGIC - Recarga completa (`CREATE OR REPLACE`): rodar 2x dá o mesmo resultado
# MAGIC
# MAGIC | Tabela | Grão (1 linha =) |
# MAGIC |---|---|
# MAGIC | `dim_customers` | 1 customer_id (cliente no pedido) |
# MAGIC | `dim_products` | 1 produto |
# MAGIC | `dim_sellers` | 1 vendedor |
# MAGIC | `dim_date` | 1 dia do calendário |
# MAGIC | `fact_order_items` | 1 item de pedido |
# MAGIC | `fact_orders` | 1 pedido |
# MAGIC | `agg_sales_monthly` | 1 mês |
# MAGIC | `agg_category_performance` | 1 categoria |
# MAGIC | `agg_seller_performance` | 1 vendedor |
# MAGIC | `agg_state_performance` | 1 UF do cliente |
# MAGIC | `agg_payment_methods` | 1 forma de pagamento |
# MAGIC | `customer_rfm` | 1 pessoa (customer_unique_id) |

# COMMAND ----------

# ===== 1. Configuração =====
# Parâmetro: o Job envia "catalog"; rodando à mão, usa o valor padrão do widget
dbutils.widgets.text("catalog", "workspace", "Catálogo")
CATALOG = dbutils.widgets.get("catalog")                 # catálogo do Unity Catalog (ex: workspace, dev, prod)
SILVER = f"{CATALOG}.silver"                             # schema de origem
GOLD = f"{CATALOG}.gold"                                 # schema de destino
LOG_TABLE = f"{GOLD}.gold_log"                           # tabela de log da Gold

# Pedidos que NÃO contam como venda (não geraram receita)
INVALID_STATUS = "('canceled', 'unavailable')"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD} COMMENT 'Camada Gold: modelo dimensional e marts de negocio (Olist)'")

# COMMAND ----------

# ===== 2. Regras reutilizáveis (pedaços de SQL) =====
# region_case (UF -> região IBGE) fica em src/olist_utils.py e é testada em tests/
import os, sys
sys.path.append(os.path.abspath("../src"))               # pasta src/ do repositório
from olist_utils import region_case


# 1 coordenada por CEP: o Olist tem vários pontos por prefixo -> média
GEO_BY_ZIP = f"""
    SELECT geolocation_zip_code_prefix AS zip,
           ROUND(AVG(geolocation_lat), 6) AS lat,
           ROUND(AVG(geolocation_lng), 6) AS lng
    FROM {SILVER}.olist_geolocation
    GROUP BY geolocation_zip_code_prefix
"""

# COMMAND ----------

# ===== 3. Dimensões =====

SQL_DIM_CUSTOMERS = f"""
WITH geo AS ({GEO_BY_ZIP})
SELECT
    c.customer_id,                                       -- chave usada nos pedidos
    c.customer_unique_id,                                -- a pessoa (pode ter vários customer_id)
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    {region_case('c.customer_state')} AS customer_region,
    g.lat AS customer_lat,                               -- NULL se o CEP não está na geolocalização
    g.lng AS customer_lng
FROM {SILVER}.olist_customers c
LEFT JOIN geo g ON c.customer_zip_code_prefix = g.zip
"""

SQL_DIM_SELLERS = f"""
WITH geo AS ({GEO_BY_ZIP})
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    {region_case('s.seller_state')} AS seller_region,
    g.lat AS seller_lat,
    g.lng AS seller_lng
FROM {SILVER}.olist_sellers s
LEFT JOIN geo g ON s.seller_zip_code_prefix = g.zip
"""

SQL_DIM_PRODUCTS = f"""
SELECT
    product_id,
    product_category_name         AS category_name_pt,
    product_category_name_english AS category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_length_cm * product_height_cm * product_width_cm AS product_volume_cm3,  -- cubagem
    CASE                                                  -- faixa de peso (ajuda a explicar frete)
        WHEN product_weight_g IS NULL  THEN 'desconhecido'
        WHEN product_weight_g <= 500   THEN 'ate 500g'
        WHEN product_weight_g <= 2000  THEN '500g a 2kg'
        WHEN product_weight_g <= 10000 THEN '2kg a 10kg'
        ELSE 'acima de 10kg'
    END AS weight_band
FROM {SILVER}.olist_products
"""

# Calendário contínuo: do 1º ao último dia com pedido (sem buracos, mesmo em dia sem venda)
SQL_DIM_DATE = f"""
WITH bounds AS (
    SELECT DATE_TRUNC('YEAR', MIN(order_purchase_timestamp))::DATE AS d0,
           LAST_DAY(DATE_TRUNC('YEAR', MAX(order_purchase_timestamp)) + INTERVAL 11 MONTHS) AS d1
    FROM {SILVER}.olist_orders
),
days AS (
    SELECT EXPLODE(SEQUENCE(d0, d1, INTERVAL 1 DAY)) AS date FROM bounds
)
SELECT
    CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT) AS date_key,  -- chave inteira: 20180315
    date,
    YEAR(date)                    AS year,
    QUARTER(date)                 AS quarter,
    MONTH(date)                   AS month,
    DATE_FORMAT(date, 'yyyy-MM')  AS year_month,
    ELEMENT_AT(ARRAY('Janeiro','Fevereiro','Marco','Abril','Maio','Junho','Julho',
                     'Agosto','Setembro','Outubro','Novembro','Dezembro'), MONTH(date)) AS month_name,
    DAY(date)                     AS day,
    DAYOFWEEK(date)               AS day_of_week,             -- 1 = domingo ... 7 = sábado
    ELEMENT_AT(ARRAY('Domingo','Segunda','Terca','Quarta','Quinta','Sexta','Sabado'),
               DAYOFWEEK(date))   AS day_name,
    DAYOFWEEK(date) IN (1, 7)     AS is_weekend
FROM days
"""

# COMMAND ----------

# ===== 4. Fatos =====

SQL_FACT_ORDER_ITEMS = f"""
SELECT
    i.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    CAST(DATE_FORMAT(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS purchase_date_key,
    o.order_purchase_timestamp,
    o.order_status,
    o.order_status NOT IN {INVALID_STATUS} AS is_valid_sale,
    i.shipping_limit_date,
    o.order_delivered_carrier_date,
    o.order_delivered_carrier_date > i.shipping_limit_date AS is_seller_late,  -- vendedor despachou fora do prazo
    i.price,
    i.freight_value,
    i.price + i.freight_value AS item_total_value
FROM {SILVER}.olist_order_items i
JOIN {SILVER}.olist_orders o ON i.order_id = o.order_id
"""

SQL_FACT_ORDERS = f"""
WITH items AS (
    SELECT order_id,
           COUNT(*)                  AS items_qty,
           COUNT(DISTINCT product_id) AS products_qty,
           COUNT(DISTINCT seller_id) AS sellers_qty,
           SUM(price)                AS items_value,
           SUM(freight_value)        AS freight_value
    FROM {SILVER}.olist_order_items
    GROUP BY order_id
),
pay AS (
    SELECT order_id,
           SUM(payment_value)                  AS payment_value,
           COUNT(*)                            AS payments_qty,
           MAX(payment_installments)           AS max_installments,
           MAX_BY(payment_type, payment_value) AS main_payment_type  -- forma que pagou mais
    FROM {SILVER}.olist_order_payments
    GROUP BY order_id
),
rev AS (                                                 -- pedido pode ter >1 review: fica a mais recente
    SELECT order_id,
           MAX_BY(review_score, review_creation_date) AS review_score,
           MAX(review_comment_message IS NOT NULL)    AS has_review_comment
    FROM {SILVER}.olist_order_reviews
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    CAST(DATE_FORMAT(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS purchase_date_key,
    o.order_status,
    o.order_status NOT IN {INVALID_STATUS} AS is_valid_sale,

    -- Linha do tempo
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Valores
    COALESCE(i.items_qty, 0)        AS items_qty,
    COALESCE(i.products_qty, 0)     AS products_qty,
    COALESCE(i.sellers_qty, 0)      AS sellers_qty,
    COALESCE(i.items_value, 0)      AS items_value,
    COALESCE(i.freight_value, 0)    AS freight_value,
    COALESCE(i.items_value, 0) + COALESCE(i.freight_value, 0) AS order_value,  -- GMV do pedido
    p.payment_value,
    p.payments_qty,
    p.max_installments,
    p.main_payment_type,

    -- Logística (dias)
    ROUND((UNIX_TIMESTAMP(o.order_approved_at) - UNIX_TIMESTAMP(o.order_purchase_timestamp)) / 3600, 2)
        AS approval_hours,
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)      AS delivery_days,
    DATEDIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp)      AS estimated_delivery_days,
    DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS delivery_delay_days,  -- >0 = atrasou
    CASE WHEN o.order_delivered_customer_date IS NULL THEN NULL               -- não entregue: indefinido
         ELSE TO_DATE(o.order_delivered_customer_date) > TO_DATE(o.order_estimated_delivery_date)
    END AS is_late,

    -- Satisfação
    r.review_score,
    COALESCE(r.has_review_comment, FALSE) AS has_review_comment
FROM {SILVER}.olist_orders o
LEFT JOIN {SILVER}.olist_customers c ON o.customer_id = c.customer_id
LEFT JOIN items i ON o.order_id = i.order_id
LEFT JOIN pay   p ON o.order_id = p.order_id
LEFT JOIN rev   r ON o.order_id = r.order_id
"""

# COMMAND ----------

# ===== 5. Marts (respostas prontas para o negócio) =====
# Todos consideram só venda válida (is_valid_sale) e leem dos fatos/dimensões da própria Gold.

SQL_AGG_SALES_MONTHLY = f"""
SELECT
    d.year_month,
    d.year,
    d.month,
    COUNT(*)                               AS orders,
    COUNT(DISTINCT f.customer_unique_id)   AS customers,
    SUM(f.items_qty)                       AS items_sold,
    ROUND(SUM(f.items_value), 2)           AS items_revenue,
    ROUND(SUM(f.freight_value), 2)         AS freight_revenue,
    ROUND(SUM(f.order_value), 2)           AS gmv,
    ROUND(AVG(f.order_value), 2)           AS avg_ticket,
    ROUND(AVG(f.review_score), 2)          AS avg_review_score,
    ROUND(AVG(f.delivery_days), 1)         AS avg_delivery_days,
    ROUND(AVG(CAST(f.is_late AS INT)), 4)  AS late_rate,                     -- % entregue fora do prazo
    ROUND(SUM(f.order_value) / LAG(SUM(f.order_value)) OVER (ORDER BY d.year_month) - 1, 4)
                                           AS gmv_mom_growth                 -- crescimento vs mês anterior
FROM {GOLD}.fact_orders f
JOIN {GOLD}.dim_date d ON f.purchase_date_key = d.date_key
WHERE f.is_valid_sale
GROUP BY d.year_month, d.year, d.month
"""

SQL_AGG_CATEGORY = f"""
WITH order_review AS (SELECT order_id, review_score FROM {GOLD}.fact_orders)
SELECT
    p.category_name,
    p.category_name_pt,
    COUNT(*)                              AS items_sold,
    COUNT(DISTINCT i.order_id)            AS orders,
    COUNT(DISTINCT i.seller_id)           AS sellers,
    ROUND(SUM(i.price), 2)                AS revenue,
    ROUND(AVG(i.price), 2)                AS avg_price,
    ROUND(AVG(i.freight_value), 2)        AS avg_freight,
    ROUND(SUM(i.freight_value) / SUM(i.price), 4) AS freight_to_price_ratio,
    ROUND(AVG(r.review_score), 2)         AS avg_review_score,
    ROUND(SUM(i.price) / SUM(SUM(i.price)) OVER (), 4) AS revenue_share,       -- participação na receita
    RANK() OVER (ORDER BY SUM(i.price) DESC) AS revenue_rank
FROM {GOLD}.fact_order_items i
JOIN {GOLD}.dim_products p ON i.product_id = p.product_id
LEFT JOIN order_review r   ON i.order_id = r.order_id
WHERE i.is_valid_sale
GROUP BY p.category_name, p.category_name_pt
"""

SQL_AGG_SELLER = f"""
WITH ord AS (SELECT order_id, review_score, is_late, delivery_days FROM {GOLD}.fact_orders)
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_region,
    COUNT(DISTINCT i.order_id)              AS orders,
    COUNT(*)                                AS items_sold,
    COUNT(DISTINCT i.product_id)            AS distinct_products,
    ROUND(SUM(i.price), 2)                  AS revenue,
    ROUND(AVG(i.price), 2)                  AS avg_item_price,
    ROUND(AVG(o.review_score), 2)           AS avg_review_score,
    ROUND(AVG(CAST(o.is_late AS INT)), 4)   AS late_delivery_rate,
    ROUND(AVG(CAST(i.is_seller_late AS INT)), 4) AS late_dispatch_rate,      -- culpa do vendedor (despacho)
    ROUND(AVG(o.delivery_days), 1)          AS avg_delivery_days,
    MIN(i.order_purchase_timestamp)         AS first_sale_at,
    MAX(i.order_purchase_timestamp)         AS last_sale_at,
    RANK() OVER (ORDER BY SUM(i.price) DESC) AS revenue_rank
FROM {GOLD}.fact_order_items i
JOIN {GOLD}.dim_sellers s ON i.seller_id = s.seller_id
LEFT JOIN ord o           ON i.order_id = o.order_id
WHERE i.is_valid_sale
GROUP BY s.seller_id, s.seller_city, s.seller_state, s.seller_region
"""

SQL_AGG_STATE = f"""
SELECT
    c.customer_state                      AS state,
    c.customer_region                     AS region,
    COUNT(DISTINCT f.customer_unique_id)  AS customers,
    COUNT(*)                              AS orders,
    ROUND(SUM(f.order_value), 2)          AS gmv,
    ROUND(AVG(f.order_value), 2)          AS avg_ticket,
    ROUND(AVG(f.freight_value), 2)        AS avg_freight,
    ROUND(AVG(f.delivery_days), 1)        AS avg_delivery_days,
    ROUND(AVG(CAST(f.is_late AS INT)), 4) AS late_rate,
    ROUND(AVG(f.review_score), 2)         AS avg_review_score
FROM {GOLD}.fact_orders f
JOIN {GOLD}.dim_customers c ON f.customer_id = c.customer_id
WHERE f.is_valid_sale
GROUP BY c.customer_state, c.customer_region
"""

SQL_AGG_PAYMENT = f"""
SELECT
    p.payment_type,
    COUNT(*)                               AS payments,
    COUNT(DISTINCT p.order_id)             AS orders,
    ROUND(SUM(p.payment_value), 2)         AS total_value,
    ROUND(AVG(p.payment_value), 2)         AS avg_value,
    ROUND(AVG(p.payment_installments), 2)  AS avg_installments,
    ROUND(SUM(p.payment_value) / SUM(SUM(p.payment_value)) OVER (), 4) AS value_share
FROM {SILVER}.olist_order_payments p
JOIN {GOLD}.fact_orders f ON p.order_id = f.order_id
WHERE f.is_valid_sale
GROUP BY p.payment_type
"""

# RFM: Recência (dias desde a última compra), Frequência (nº pedidos), Valor (gasto total).
# Referência = dia seguinte à última compra do dataset (dado histórico, não "hoje").
# Frequência: ~97% dos clientes do Olist compram 1 vez, então usamos faixas fixas em vez de quintil.
SQL_CUSTOMER_RFM = f"""
WITH ref AS (
    SELECT DATE_ADD(MAX(TO_DATE(order_purchase_timestamp)), 1) AS ref_date
    FROM {GOLD}.fact_orders WHERE is_valid_sale
),
base AS (
    SELECT
        f.customer_unique_id,
        DATEDIFF(ref.ref_date, MAX(TO_DATE(f.order_purchase_timestamp))) AS recency_days,
        COUNT(*)                         AS frequency,
        ROUND(SUM(f.order_value), 2)     AS monetary,
        MIN(f.order_purchase_timestamp)  AS first_purchase_at,
        MAX(f.order_purchase_timestamp)  AS last_purchase_at
    FROM {GOLD}.fact_orders f CROSS JOIN ref
    WHERE f.is_valid_sale
    GROUP BY f.customer_unique_id, ref.ref_date
),
scored AS (
    SELECT *,
        6 - NTILE(5) OVER (ORDER BY recency_days) AS r_score,          -- mais recente = 5
        CASE WHEN frequency >= 4 THEN 5 WHEN frequency = 3 THEN 4
             WHEN frequency = 2 THEN 3 ELSE 1 END AS f_score,
        NTILE(5) OVER (ORDER BY monetary)          AS m_score           -- gastou mais = 5
    FROM base
)
SELECT *,
    CONCAT(r_score, f_score, m_score) AS rfm_code,
    CASE
        WHEN r_score >= 4 AND f_score >= 3                  THEN 'Campeoes'
        WHEN f_score >= 3                                   THEN 'Fieis'
        WHEN r_score >= 4 AND m_score >= 4                  THEN 'Novos de alto valor'
        WHEN r_score >= 4                                   THEN 'Novos'
        WHEN r_score <= 2 AND m_score >= 4                  THEN 'Em risco (alto valor)'
        WHEN r_score <= 2                                   THEN 'Perdidos'
        ELSE 'Precisam de atencao'
    END AS segment
FROM scored
"""

# COMMAND ----------

# ===== 6. Mapa tabela -> SQL -> descrição =====
# A ORDEM importa: marts leem dos fatos/dimensões criados antes.
TABLES = [
    {"table": "dim_customers",            "sql": SQL_DIM_CUSTOMERS,     "comment": "Dimensao cliente (customer_id) com regiao e coordenadas"},
    {"table": "dim_sellers",              "sql": SQL_DIM_SELLERS,       "comment": "Dimensao vendedor com regiao e coordenadas"},
    {"table": "dim_products",             "sql": SQL_DIM_PRODUCTS,      "comment": "Dimensao produto com categoria PT/EN, cubagem e faixa de peso"},
    {"table": "dim_date",                 "sql": SQL_DIM_DATE,          "comment": "Dimensao calendario (date_key = yyyyMMdd)"},
    {"table": "fact_order_items",         "sql": SQL_FACT_ORDER_ITEMS,  "comment": "Fato item de pedido: preco, frete e prazo de despacho"},
    {"table": "fact_orders",              "sql": SQL_FACT_ORDERS,       "comment": "Fato pedido: valores, pagamento, logistica e review"},
    {"table": "agg_sales_monthly",        "sql": SQL_AGG_SALES_MONTHLY, "comment": "Mart: KPIs de vendas por mes (so vendas validas)"},
    {"table": "agg_category_performance", "sql": SQL_AGG_CATEGORY,      "comment": "Mart: desempenho por categoria de produto"},
    {"table": "agg_seller_performance",   "sql": SQL_AGG_SELLER,        "comment": "Mart: desempenho por vendedor"},
    {"table": "agg_state_performance",    "sql": SQL_AGG_STATE,         "comment": "Mart: desempenho por UF do cliente"},
    {"table": "agg_payment_methods",      "sql": SQL_AGG_PAYMENT,       "comment": "Mart: formas de pagamento"},
    {"table": "customer_rfm",             "sql": SQL_CUSTOMER_RFM,      "comment": "Mart: segmentacao RFM por pessoa (customer_unique_id)"},
]

# COMMAND ----------

# ===== 7. Tabela de log =====
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    table_name    STRING,     -- tabela gerada
    gold_rows     BIGINT,     -- linhas gravadas
    status        STRING,     -- SUCCESS ou FAILED
    error_message STRING,     -- erro, se houver
    started_at    TIMESTAMP,  -- início
    finished_at   TIMESTAMP   -- fim
)
COMMENT 'Log de execucoes da camada Gold'
""")

LOG_SCHEMA = (
    "table_name STRING, gold_rows BIGINT, status STRING, error_message STRING, "
    "started_at TIMESTAMP, finished_at TIMESTAMP"
)

# COMMAND ----------

# ===== 8. Execução: Silver -> Gold =====
from datetime import datetime

failures = []                                            # guarda o que falhou para quebrar o job no fim
for t in TABLES:
    dst = f"{GOLD}.{t['table']}"
    started_at = datetime.now()
    rows = None
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {dst} COMMENT '{t['comment']}' AS {t['sql']}")  # recria a tabela
        rows = spark.table(dst).count()
        status, error = "SUCCESS", None
        print(f"{t['table']}: {rows} linhas")
    except Exception as e:                               # registra e segue (as próximas podem depender desta)
        status, error = "FAILED", str(e)[:1000]
        failures.append(t["table"])
        print(f"{t['table']}: FALHOU -> {error[:200]}")

    log_row = [(t["table"], rows, status, error, started_at, datetime.now())]
    spark.createDataFrame(log_row, LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TABLE)

# Job deve terminar com erro se algo falhou (senão o orquestrador acha que deu tudo certo)
if failures:
    raise Exception(f"Gold falhou em: {failures}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validações rápidas
# MAGIC Os testes completos (chaves, órfãos, reconciliação) ficam em `quality/04_data_quality`.

# COMMAND ----------

# Última execução: todas SUCCESS?
display(spark.sql(f"""
    SELECT * FROM {CATALOG}.gold.gold_log
    ORDER BY started_at DESC
    LIMIT 12
"""))

# COMMAND ----------

# Evolução mensal de vendas
display(spark.sql(f"""
    SELECT year_month, orders, gmv, avg_ticket, avg_review_score, late_rate
    FROM {CATALOG}.gold.agg_sales_monthly
    ORDER BY year_month
"""))

# COMMAND ----------

# Top 10 categorias por receita
display(spark.sql(f"""
    SELECT revenue_rank, category_name, revenue, revenue_share, avg_review_score
    FROM {CATALOG}.gold.agg_category_performance
    ORDER BY revenue_rank
    LIMIT 10
"""))

# COMMAND ----------

# Atraso derruba a nota? (pergunta clássica do Olist)
display(spark.sql(f"""
    SELECT is_late, COUNT(*) AS pedidos, ROUND(AVG(review_score), 2) AS nota_media
    FROM {CATALOG}.gold.fact_orders
    WHERE is_late IS NOT NULL
    GROUP BY is_late
"""))

# COMMAND ----------

# Segmentos RFM
display(spark.sql(f"""
    SELECT segment, COUNT(*) AS clientes, ROUND(AVG(monetary), 2) AS gasto_medio
    FROM {CATALOG}.gold.customer_rfm
    GROUP BY segment
    ORDER BY clientes DESC
"""))
