"""Configuração compartilhada do pytest."""
import os
import sys

import pytest

# Deixa "from olist_utils import ..." funcionar (pasta src/ do repositório)
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    """Sessão Spark: no Databricks reaproveita a do notebook; fora dele cria uma local."""
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
