"""
Utilitários para trabalhar com PySpark
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Optional, Dict, Any
import logging


def get_spark_session(
    app_name: str = "ECD Pipeline",
    master: str = "local[*]",
    config: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Cria ou retorna sessão Spark existente

    Args:
        app_name: Nome da aplicação
        master: Master URL (local[*] para modo local)
        config: Configurações adicionais do Spark

    Returns:
        SparkSession configurada
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    # Configurações padrão
    default_config = {
        "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }

    # Merge com configurações customizadas
    if config:
        default_config.update(config)

    for key, value in default_config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def save_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_by: Optional[list] = None,
    logger: Optional[logging.Logger] = None
):
    """
    Salva DataFrame como tabela Spark/Hive

    Args:
        df: DataFrame a ser salvo
        table_name: Nome da tabela
        mode: Modo de salvamento (overwrite, append, etc)
        partition_by: Colunas para particionamento
        logger: Logger para mensagens
    """
    if logger:
        count = df.count()
        logger.info(f"Salvando {count:,} registros na tabela {table_name}")

    writer = df.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.saveAsTable(table_name)

    if logger:
        logger.info(f"Tabela {table_name} salva com sucesso")


def execute_query(
    spark: SparkSession,
    query: str,
    logger: Optional[logging.Logger] = None
) -> DataFrame:
    """
    Executa query SQL e retorna DataFrame

    Args:
        spark: Sessão Spark
        query: Query SQL
        logger: Logger para mensagens

    Returns:
        DataFrame com resultado da query
    """
    if logger:
        logger.debug(f"Executando query: {query[:100]}...")

    df = spark.sql(query)

    if logger:
        count = df.count()
        logger.info(f"Query retornou {count:,} registros")

    return df


def add_classification_columns(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas padrão de classificação a um DataFrame

    Args:
        df: DataFrame original

    Returns:
        DataFrame com colunas de classificação
    """
    return df.withColumn("classificacao_nivel1", F.lit(None).cast(StringType())) \
             .withColumn("classificacao_nivel2", F.lit(None).cast(StringType())) \
             .withColumn("classificacao_nivel3", F.lit(None).cast(StringType())) \
             .withColumn("nome_nivel1", F.lit(None).cast(StringType())) \
             .withColumn("nome_nivel2", F.lit(None).cast(StringType())) \
             .withColumn("nome_nivel3", F.lit(None).cast(StringType())) \
             .withColumn("metodo_classificacao", F.lit(None).cast(StringType())) \
             .withColumn("confianca_classificacao", F.lit(None).cast(DoubleType()))


def validate_required_columns(
    df: DataFrame,
    required_columns: list,
    df_name: str = "DataFrame"
) -> bool:
    """
    Valida se DataFrame possui colunas obrigatórias

    Args:
        df: DataFrame a validar
        required_columns: Lista de colunas obrigatórias
        df_name: Nome do DataFrame (para mensagens de erro)

    Returns:
        True se todas colunas existem, False caso contrário

    Raises:
        ValueError se alguma coluna estiver faltando
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"{df_name} não possui as colunas obrigatórias: {missing}"
        )
    return True


def clean_text_column(df: DataFrame, col_name: str) -> DataFrame:
    """
    Limpa e normaliza coluna de texto

    Args:
        df: DataFrame
        col_name: Nome da coluna a limpar

    Returns:
        DataFrame com coluna limpa
    """
    return df.withColumn(
        col_name,
        F.lower(F.trim(F.regexp_replace(F.col(col_name), r'\s+', ' ')))
    )


def calculate_balance_sum(df: DataFrame, group_cols: list) -> DataFrame:
    """
    Calcula soma de saldos por grupo

    Args:
        df: DataFrame com coluna 'vl_saldo_fin'
        group_cols: Colunas para agrupamento

    Returns:
        DataFrame agregado
    """
    return df.groupBy(group_cols) \
             .agg(F.sum('vl_saldo_fin').alias('saldo_total'))


def apply_natureza_multiplier(df: DataFrame) -> DataFrame:
    """
    Aplica multiplicador baseado na natureza da conta
    - Natureza Ativa (01): saldo positivo
    - Natureza Passiva (02): saldo negativo
    - Natureza PL (04): saldo negativo

    Args:
        df: DataFrame com colunas 'cd_natureza' e 'vl_saldo_fin'

    Returns:
        DataFrame com coluna 'saldo_ajustado'
    """
    return df.withColumn(
        'saldo_ajustado',
        F.when(F.col('cd_natureza') == '01', F.col('vl_saldo_fin'))
         .when(F.col('cd_natureza').isin(['02', '04']), -F.col('vl_saldo_fin'))
         .otherwise(F.col('vl_saldo_fin'))
    )
