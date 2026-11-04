"""Testes unitários das funções de src/olist_utils.py.

Cada teste monta um DataFrame pequeno, aplica 1 função e confere o resultado.
Rodam no Databricks pelo notebook tests/run_unit_tests (1ª etapa do Job).
"""
from datetime import datetime
from decimal import Decimal

from olist_utils import (
    clean_strings, dedup, fix_header, norm_city, norm_state, norm_zip,
    region_case, to_dec, to_int, to_ts,
)


def test_fix_header_remove_bom(spark):
    df = spark.createDataFrame([("1",)], ["﻿order_id"])
    assert fix_header(df).columns == ["order_id"]


def test_clean_strings_trim_e_vazio_vira_null(spark):
    df = spark.createDataFrame([("  sp  ",), ("",), ("   ",)], ["state"])
    result = [r.state for r in clean_strings(df).collect()]
    assert result == ["sp", None, None]


def test_casts_invalidos_viram_null(spark):
    df = spark.createDataFrame([("10", "19.90", "2017-10-02 10:56:33"),
                                ("abc", "xyz", "data errada")], ["qty", "price", "ts"])
    rows = df.select(to_int("qty").alias("qty"),
                     to_dec("price").alias("price"),
                     to_ts("ts").alias("ts")).collect()
    assert (rows[0].qty, rows[0].price, rows[0].ts) == (10, Decimal("19.90"), datetime(2017, 10, 2, 10, 56, 33))
    assert (rows[1].qty, rows[1].price, rows[1].ts) == (None, None, None)


def test_norm_city_minuscula_sem_acento(spark):
    df = spark.createDataFrame([("São Paulo",), ("BRASÍLIA",)], ["city"])
    assert [r.c for r in df.select(norm_city("city").alias("c")).collect()] == ["sao paulo", "brasilia"]


def test_norm_state_e_norm_zip(spark):
    df = spark.createDataFrame([("sp", "1037")], ["state", "zip"])
    row = df.select(norm_state("state").alias("uf"), norm_zip("zip").alias("cep")).first()
    assert (row.uf, row.cep) == ("SP", "01037")       # CEP com zero à esquerda volta a ter 5 dígitos


def test_dedup_fica_a_linha_mais_recente(spark):
    df = spark.createDataFrame([
        ("p1", "antigo", datetime(2026, 1, 1)),
        ("p1", "novo",   datetime(2026, 2, 1)),
        ("p2", "unico",  datetime(2026, 1, 1)),
    ], ["order_id", "status", "_ingestion_timestamp"])
    result = {r.order_id: r.status for r in dedup(df, ["order_id"]).collect()}
    assert result == {"p1": "novo", "p2": "unico"}    # 1 linha por chave, a mais nova


def test_region_case_mapeia_uf_para_regiao(spark):
    df = spark.createDataFrame([("SP",), ("BA",), ("RS",), ("AM",), ("DF",), ("XX",)], ["uf"])
    df.createOrReplaceTempView("ufs")
    result = {r.uf: r.regiao for r in spark.sql(f"SELECT uf, {region_case('uf')} AS regiao FROM ufs").collect()}
    assert result == {"SP": "Sudeste", "BA": "Nordeste", "RS": "Sul",
                      "AM": "Norte", "DF": "Centro-Oeste", "XX": "Desconhecida"}
