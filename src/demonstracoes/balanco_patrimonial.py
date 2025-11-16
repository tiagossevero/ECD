"""
Gerador de Balanço Patrimonial
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional, List
import logging

from .estrutura_contas import EstruturaContas


class GeradorBalancoPatrimonial:
    """
    Gera Balanço Patrimonial a partir de contas classificadas

    Estrutura do BP:
    ATIVO                              PASSIVO + PATRIMÔNIO LÍQUIDO
      Ativo Circulante                   Passivo Circulante
      Ativo Não Circulante                Passivo Não Circulante
                                          Patrimônio Líquido
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: Optional[logging.Logger] = None
    ):
        self.spark = spark
        self.logger = logger or logging.getLogger(__name__)
        self.estrutura = EstruturaContas()

    def gerar(
        self,
        df_saldos: DataFrame,
        agrupar_por: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Gera Balanço Patrimonial

        Args:
            df_saldos: DataFrame com saldos classificados
                       Colunas: cnpj, ano, cd_cta, classificacao_nivel1, classificacao_nivel2,
                               classificacao_nivel3, vl_saldo_fin
            agrupar_por: Colunas adicionais para agrupamento (ex: ['cnae', 'porte'])

        Returns:
            DataFrame com BP estruturado
        """
        self.logger.info("Gerando Balanço Patrimonial...")

        # Filtrar apenas contas do BP (classificacao_nivel1 in 1, 2, 3)
        df_bp = df_saldos.filter(
            F.col('classificacao_nivel1').isin(['1', '2', '3'])
        )

        # Definir colunas de agrupamento
        group_cols = ['cnpj', 'ano']
        if agrupar_por:
            group_cols.extend(agrupar_por)

        # PARTE 1: Consolidar por níveis
        df_nivel3 = self._consolidar_nivel(df_bp, group_cols, 'classificacao_nivel3', 3)
        df_nivel2 = self._consolidar_nivel(df_bp, group_cols, 'classificacao_nivel2', 2)
        df_nivel1 = self._consolidar_nivel(df_bp, group_cols, 'classificacao_nivel1', 1)

        # PARTE 2: Unir todos os níveis
        df_bp_consolidado = df_nivel1.unionByName(df_nivel2).unionByName(df_nivel3)

        # PARTE 3: Adicionar nomes legíveis
        df_bp_consolidado = self._adicionar_nomes_contas(df_bp_consolidado)

        # PARTE 4: Calcular totalizadores principais
        df_bp_final = self._calcular_totalizadores(df_bp_consolidado, group_cols)

        # PARTE 5: Ordenar por estrutura hierárquica
        df_bp_final = df_bp_final.orderBy('cnpj', 'ano', 'codigo_conta')

        total_empresas = df_bp_final.select('cnpj').distinct().count()
        self.logger.info(f"BP gerado para {total_empresas:,} empresas")

        return df_bp_final

    def _consolidar_nivel(
        self,
        df: DataFrame,
        group_cols: List[str],
        classificacao_col: str,
        nivel: int
    ) -> DataFrame:
        """
        Consolida saldos por nível de classificação

        Args:
            df: DataFrame com saldos
            group_cols: Colunas para agrupamento
            classificacao_col: Coluna de classificação (nivel1, nivel2, nivel3)
            nivel: Nível da hierarquia

        Returns:
            DataFrame consolidado
        """
        df_consolidado = df.filter(F.col(classificacao_col).isNotNull()) \
                           .groupBy(group_cols + [classificacao_col]) \
                           .agg(F.sum('vl_saldo_fin').alias('saldo')) \
                           .withColumnRenamed(classificacao_col, 'codigo_conta') \
                           .withColumn('nivel', F.lit(nivel))

        return df_consolidado

    def _adicionar_nomes_contas(self, df: DataFrame) -> DataFrame:
        """
        Adiciona nomes legíveis das contas baseado na estrutura

        Args:
            df: DataFrame com código das contas

        Returns:
            DataFrame com coluna 'nome_conta' adicionada
        """
        def get_nome_conta(codigo: str) -> str:
            conta = self.estrutura.get_conta(codigo)
            return conta.nome if conta else codigo

        get_nome_udf = F.udf(get_nome_conta)

        return df.withColumn('nome_conta', get_nome_udf(F.col('codigo_conta')))

    def _calcular_totalizadores(
        self,
        df: DataFrame,
        group_cols: List[str]
    ) -> DataFrame:
        """
        Calcula valores totalizadores principais do BP

        Args:
            df: DataFrame consolidado
            group_cols: Colunas de agrupamento

        Returns:
            DataFrame com totalizadores
        """
        # Calcular totais principais por empresa/ano
        df_totais = df.groupBy(group_cols + ['codigo_conta']) \
                      .agg(F.sum('saldo').alias('saldo'))

        # Adicionar colunas calculadas
        window_spec = Window.partitionBy(group_cols)

        # Total do Ativo
        df_totais = df_totais.withColumn(
            'ativo_total',
            F.sum(
                F.when(F.col('codigo_conta') == '1', F.col('saldo')).otherwise(0)
            ).over(window_spec)
        )

        # Total do Passivo
        df_totais = df_totais.withColumn(
            'passivo_total',
            F.sum(
                F.when(F.col('codigo_conta') == '2', F.col('saldo')).otherwise(0)
            ).over(window_spec)
        )

        # Total do PL
        df_totais = df_totais.withColumn(
            'patrimonio_liquido_total',
            F.sum(
                F.when(F.col('codigo_conta') == '3', F.col('saldo')).otherwise(0)
            ).over(window_spec)
        )

        # Passivo + PL (deve igualar Ativo)
        df_totais = df_totais.withColumn(
            'passivo_mais_pl',
            F.col('passivo_total') + F.col('patrimonio_liquido_total')
        )

        # Diferença (para validação)
        df_totais = df_totais.withColumn(
            'diferenca_balanco',
            F.col('ativo_total') - F.col('passivo_mais_pl')
        )

        return df_totais

    def gerar_bp_analitico(
        self,
        df_saldos: DataFrame,
        cnpj: str,
        ano: int
    ) -> DataFrame:
        """
        Gera BP analítico (detalhado) para uma empresa específica

        Args:
            df_saldos: DataFrame com saldos classificados
            cnpj: CNPJ da empresa
            ano: Ano de referência

        Returns:
            DataFrame com BP analítico (todas as contas)
        """
        self.logger.info(f"Gerando BP analítico para CNPJ {cnpj}, ano {ano}")

        df_empresa = df_saldos.filter(
            (F.col('cnpj') == cnpj) &
            (F.col('ano') == ano) &
            F.col('classificacao_nivel1').isin(['1', '2', '3'])
        )

        # Selecionar e ordenar colunas
        df_analitico = df_empresa.select(
            'cd_cta',
            'desc_cta',
            'classificacao_nivel1',
            'classificacao_nivel2',
            'classificacao_nivel3',
            'nome_nivel1',
            'nome_nivel2',
            'nome_nivel3',
            'vl_saldo_fin'
        ).orderBy('classificacao_nivel1', 'classificacao_nivel2', 'classificacao_nivel3', 'cd_cta')

        return df_analitico

    def gerar_bp_comparativo(
        self,
        df_saldos: DataFrame,
        cnpj: str,
        anos: List[int]
    ) -> DataFrame:
        """
        Gera BP comparativo (múltiplos anos)

        Args:
            df_saldos: DataFrame com saldos classificados
            cnpj: CNPJ da empresa
            anos: Lista de anos para comparação

        Returns:
            DataFrame com BP comparativo
        """
        self.logger.info(f"Gerando BP comparativo para CNPJ {cnpj}, anos {anos}")

        df_empresa = df_saldos.filter(
            (F.col('cnpj') == cnpj) &
            (F.col('ano').isin(anos)) &
            F.col('classificacao_nivel1').isin(['1', '2', '3'])
        )

        # Consolidar por níveis e pivotar por ano
        df_consolidado = df_empresa.groupBy('classificacao_nivel2', 'ano') \
                                    .agg(F.sum('vl_saldo_fin').alias('saldo'))

        df_comparativo = df_consolidado.groupBy('classificacao_nivel2') \
                                        .pivot('ano') \
                                        .sum('saldo')

        # Adicionar nomes
        df_comparativo = self._adicionar_nomes_contas(
            df_comparativo.withColumnRenamed('classificacao_nivel2', 'codigo_conta')
        )

        # Calcular variações
        if len(anos) == 2:
            anos_sorted = sorted(anos)
            df_comparativo = df_comparativo.withColumn(
                'variacao_absoluta',
                F.col(str(anos_sorted[1])) - F.col(str(anos_sorted[0]))
            ).withColumn(
                'variacao_percentual',
                F.when(
                    F.col(str(anos_sorted[0])) != 0,
                    ((F.col(str(anos_sorted[1])) - F.col(str(anos_sorted[0]))) /
                     F.abs(F.col(str(anos_sorted[0])))) * 100
                ).otherwise(None)
            )

        return df_comparativo
