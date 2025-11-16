"""
Indicadores Financeiros
Calculados a partir do Balanço Patrimonial
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional
import logging


class IndicadoresFinanceiros:
    """
    Calcula indicadores financeiros a partir do BP

    Categorias:
    1. Liquidez
    2. Endividamento
    3. Estrutura de Capital
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def calcular_todos(self, df_bp: DataFrame) -> DataFrame:
        """
        Calcula todos os indicadores financeiros

        Args:
            df_bp: DataFrame do Balanço Patrimonial
                   Esperado formato pivotado com colunas para cada grupo de contas

        Returns:
            DataFrame com indicadores calculados
        """
        self.logger.info("Calculando indicadores financeiros...")

        # Liquidez
        df = self._calcular_liquidez(df_bp)

        # Endividamento
        df = self._calcular_endividamento(df)

        # Estrutura de Capital
        df = self._calcular_estrutura_capital(df)

        return df

    def _calcular_liquidez(self, df: DataFrame) -> DataFrame:
        """
        Calcula indicadores de liquidez

        1. Liquidez Corrente = Ativo Circulante / Passivo Circulante
        2. Liquidez Seca = (AC - Estoques) / PC
        3. Liquidez Imediata = Disponibilidades / PC
        4. Liquidez Geral = (AC + ANC) / (PC + PNC)
        """
        self.logger.debug("Calculando indicadores de liquidez...")

        # Liquidez Corrente
        df = df.withColumn(
            'liquidez_corrente',
            F.when(
                F.col('passivo_circulante') != 0,
                F.col('ativo_circulante') / F.col('passivo_circulante')
            ).otherwise(None)
        )

        # Liquidez Seca (assumindo que temos coluna estoques)
        if 'estoques' in df.columns:
            df = df.withColumn(
                'liquidez_seca',
                F.when(
                    F.col('passivo_circulante') != 0,
                    (F.col('ativo_circulante') - F.col('estoques')) / F.col('passivo_circulante')
                ).otherwise(None)
            )

        # Liquidez Imediata
        if 'disponibilidades' in df.columns:
            df = df.withColumn(
                'liquidez_imediata',
                F.when(
                    F.col('passivo_circulante') != 0,
                    F.col('disponibilidades') / F.col('passivo_circulante')
                ).otherwise(None)
            )

        # Liquidez Geral
        df = df.withColumn(
            'liquidez_geral',
            F.when(
                (F.col('passivo_circulante') + F.col('passivo_nao_circulante')) != 0,
                (F.col('ativo_circulante') + F.col('ativo_nao_circulante')) /
                (F.col('passivo_circulante') + F.col('passivo_nao_circulante'))
            ).otherwise(None)
        )

        return df

    def _calcular_endividamento(self, df: DataFrame) -> DataFrame:
        """
        Calcula indicadores de endividamento

        1. Endividamento Geral = Passivo Total / Ativo Total
        2. Composição do Endividamento = PC / Passivo Total
        3. Imobilização do PL = Ativo Não Circulante / PL
        4. Imobilização de Recursos Não Correntes = ANC / (PL + PNC)
        """
        self.logger.debug("Calculando indicadores de endividamento...")

        # Endividamento Geral (ou Grau de Endividamento)
        df = df.withColumn(
            'endividamento_geral',
            F.when(
                F.col('ativo_total') != 0,
                F.col('passivo_total') / F.col('ativo_total')
            ).otherwise(None)
        )

        # Percentual de endividamento
        df = df.withColumn(
            'endividamento_pct',
            F.col('endividamento_geral') * 100
        )

        # Composição do Endividamento
        df = df.withColumn(
            'composicao_endividamento',
            F.when(
                F.col('passivo_total') != 0,
                F.col('passivo_circulante') / F.col('passivo_total')
            ).otherwise(None)
        )

        # Imobilização do PL
        df = df.withColumn(
            'imobilizacao_pl',
            F.when(
                F.col('patrimonio_liquido_total') != 0,
                F.col('ativo_nao_circulante') / F.col('patrimonio_liquido_total')
            ).otherwise(None)
        )

        # Imobilização de Recursos Não Correntes
        df = df.withColumn(
            'imobilizacao_recursos_nao_correntes',
            F.when(
                (F.col('patrimonio_liquido_total') + F.col('passivo_nao_circulante')) != 0,
                F.col('ativo_nao_circulante') /
                (F.col('patrimonio_liquido_total') + F.col('passivo_nao_circulante'))
            ).otherwise(None)
        )

        # Grau de Alavancagem Financeira = Passivo / PL
        df = df.withColumn(
            'alavancagem_financeira',
            F.when(
                F.col('patrimonio_liquido_total') != 0,
                F.col('passivo_total') / F.col('patrimonio_liquido_total')
            ).otherwise(None)
        )

        return df

    def _calcular_estrutura_capital(self, df: DataFrame) -> DataFrame:
        """
        Calcula indicadores de estrutura de capital

        1. Participação de Capitais de Terceiros = Passivo / PL
        2. Participação PL = PL / (Passivo + PL)
        3. Capital de Giro = AC - PC
        4. Capital de Giro Líquido sobre Ativo = (AC - PC) / Ativo Total
        """
        self.logger.debug("Calculando indicadores de estrutura de capital...")

        # Participação de Capitais de Terceiros (PCT)
        df = df.withColumn(
            'part_capital_terceiros',
            F.when(
                F.col('patrimonio_liquido_total') != 0,
                F.col('passivo_total') / F.col('patrimonio_liquido_total')
            ).otherwise(None)
        )

        # Participação do PL
        df = df.withColumn(
            'part_patrimonio_liquido',
            F.when(
                (F.col('passivo_total') + F.col('patrimonio_liquido_total')) != 0,
                F.col('patrimonio_liquido_total') /
                (F.col('passivo_total') + F.col('patrimonio_liquido_total'))
            ).otherwise(None)
        )

        # Capital de Giro
        df = df.withColumn(
            'capital_giro',
            F.col('ativo_circulante') - F.col('passivo_circulante')
        )

        # Capital de Giro Líquido sobre Ativo
        df = df.withColumn(
            'capital_giro_sobre_ativo',
            F.when(
                F.col('ativo_total') != 0,
                F.col('capital_giro') / F.col('ativo_total')
            ).otherwise(None)
        )

        # Índice de Solvência = Ativo Total / Passivo Total
        df = df.withColumn(
            'indice_solvencia',
            F.when(
                F.col('passivo_total') != 0,
                F.col('ativo_total') / F.col('passivo_total')
            ).otherwise(None)
        )

        return df

    def classificar_situacao_financeira(self, df: DataFrame) -> DataFrame:
        """
        Classifica situação financeira da empresa baseada nos indicadores

        Args:
            df: DataFrame com indicadores calculados

        Returns:
            DataFrame com classificação de risco
        """
        self.logger.info("Classificando situação financeira...")

        # Score de liquidez (0-100)
        df = df.withColumn(
            'score_liquidez',
            F.when(F.col('liquidez_corrente') >= 1.5, 100)
             .when(F.col('liquidez_corrente') >= 1.0, 70)
             .when(F.col('liquidez_corrente') >= 0.5, 40)
             .otherwise(0)
        )

        # Score de endividamento (0-100)
        df = df.withColumn(
            'score_endividamento',
            F.when(F.col('endividamento_geral') <= 0.3, 100)
             .when(F.col('endividamento_geral') <= 0.5, 70)
             .when(F.col('endividamento_geral') <= 0.7, 40)
             .otherwise(0)
        )

        # Score geral (média dos scores)
        df = df.withColumn(
            'score_financeiro',
            (F.col('score_liquidez') + F.col('score_endividamento')) / 2
        )

        # Classificação
        df = df.withColumn(
            'classificacao_financeira',
            F.when(F.col('score_financeiro') >= 80, 'Excelente')
             .when(F.col('score_financeiro') >= 60, 'Boa')
             .when(F.col('score_financeiro') >= 40, 'Regular')
             .when(F.col('score_financeiro') >= 20, 'Ruim')
             .otherwise('Crítica')
        )

        return df
