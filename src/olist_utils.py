"""Funções reutilizáveis do pipeline Olist.

Ficam fora dos notebooks para poderem ser testadas (ver tests/).
Os notebooks importam daqui: from olist_utils import ...
"""
from pyspark.sql import functions as F                   # F.col, F.trim, F.when... (F = apelido)
from pyspark.sql.window import Window                    # janela: usada no dedup

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"                        # formato das datas no Olist (ex: 2017-10-02 10:56:33)

# Letras com acento e sua versão sem acento (mesma posição = troca)
ACCENTS    = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
NO_ACCENTS = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


# ===== Limpeza =====

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


# ===== Casts com try_: valor inválido vira NULL em vez de quebrar o job =====

def to_int(c):    return F.expr(f"try_cast({c} AS INT)")              # texto -> inteiro
def to_double(c): return F.expr(f"try_cast({c} AS DOUBLE)")           # texto -> decimal flutuante (lat/lng)
def to_dec(c):    return F.expr(f"try_cast({c} AS DECIMAL(10,2))")    # texto -> dinheiro (2 casas, exato)
def to_ts(c):     return F.expr(f"try_to_timestamp({c}, '{TS_FORMAT}')")  # texto -> data/hora


# ===== Padronizações de texto =====

def norm_city(c):  return F.translate(F.lower(F.col(c)), ACCENTS, NO_ACCENTS)  # "São Paulo" -> "sao paulo"
def norm_state(c): return F.upper(F.col(c))                                    # "sp" -> "SP"
def norm_zip(c):   return F.lpad(F.col(c), 5, "0")                             # "1037" -> "01037"


# ===== Deduplicação =====

def dedup(df, keys):
    """Mantém 1 linha por chave: a mais recente da Bronze."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingestion_timestamp").desc())  # grupo = chave; mais nova primeiro
    return (
        df.withColumn("_rn", F.row_number().over(w))     # numera 1, 2, 3... dentro de cada chave
        .filter(F.col("_rn") == 1)                       # fica só a 1ª (mais recente)
        .drop("_rn")                                     # remove a coluna auxiliar
    )


# ===== Regras da Gold =====

def region_case(state_col):
    """UF -> região do Brasil (IBGE). Devolve um CASE em SQL."""
    return f"""
    CASE
        WHEN {state_col} IN ('AC','AP','AM','PA','RO','RR','TO')                THEN 'Norte'
        WHEN {state_col} IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE')      THEN 'Nordeste'
        WHEN {state_col} IN ('DF','GO','MT','MS')                               THEN 'Centro-Oeste'
        WHEN {state_col} IN ('ES','MG','RJ','SP')                               THEN 'Sudeste'
        WHEN {state_col} IN ('PR','RS','SC')                                    THEN 'Sul'
        ELSE 'Desconhecida'
    END"""
