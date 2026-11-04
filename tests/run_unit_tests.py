# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Testes unitários
# MAGIC Roda os testes de `tests/test_olist_utils.py` com **pytest**.
# MAGIC É a 1ª etapa do Job: se uma função de limpeza quebrar, o pipeline nem começa.

# COMMAND ----------

# MAGIC %pip install pytest -q

# COMMAND ----------

import sys
import pytest

sys.dont_write_bytecode = True                           # não cria __pycache__ dentro do repositório

# "." = pasta tests/ | -v = mostra cada teste | sem cache = não grava .pytest_cache no repositório
exit_code = pytest.main([".", "-v", "-p", "no:cacheprovider"])

if exit_code != 0:                                       # falhou: o Job para aqui
    raise Exception(f"Testes unitários falharam (pytest exit code {exit_code})")
print("Todos os testes passaram.")
