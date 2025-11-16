"""
Indicadores de Rentabilidade
Calculados a partir da DRE e BP
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional
import logging


class IndicadoresRentabilidade:
    """
    Calcula indicadores de rentabilidade

    Categorias:
    1. Margens (Bruta, Operacional, Líquida)
    2. Retorno sobre Investimento (ROA, ROE, ROI)
    3. Giro e Prazos
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def calcular_todos(
        self,
        df_dre: DataFrame,
        df_bp: DataFrame
    ) -> DataFrame:
        """
        Calcula todos os indicadores de rentabilidade

        Args:
            df_dre: DataFrame da DRE com indicadores
            df_bp: DataFrame do BP com totalizadores

        Returns:
            DataFrame com indicadores de rentabilidade
        """
        self.logger.info("Calculando indicadores de rentabilidade...")

        # Join DRE e BP
        df = df_dre.join(
            df_bp.select('cnpj', 'ano', 'ativo_total', 'patrimonio_liquido_total'),
            on=['cnpj', 'ano'],
            how='left'
        )

        # Margens (já calculadas na DRE, mas vamos garantir)
        df = self._calcular_margens(df)

        # Retorno
        df = self._calcular_retorno(df)

        # Giro
        df = self._calcular_giro(df)

        return df

    def _calcular_margens(self, df: DataFrame) -> DataFrame:
        """
        Calcula margens de rentabilidade

        1. Margem Bruta = Lucro Bruto / Receita Líquida
        2. Margem Operacional = Lucro Operacional / Receita Líquida
        3. Margem Líquida = Lucro Líquido / Receita Líquida
        4. Margem EBITDA = EBITDA / Receita Líquida
        """
        self.logger.debug("Calculando margens...")

        # Margem Bruta
        if 'margem_bruta_pct' not in df.columns:
            df = df.withColumn(
                'margem_bruta_pct',
                F.when(
                    F.col('receita_liquida') != 0,
                    (F.col('lucro_bruto') / F.col('receita_liquida')) * 100
                ).otherwise(0)
            )

        # Margem Operacional
        if 'margem_operacional_pct' not in df.columns:
            df = df.withColumn(
                'margem_operacional_pct',
                F.when(
                    F.col('receita_liquida') != 0,
                    (F.col('lucro_operacional') / F.col('receita_liquida')) * 100
                ).otherwise(0)
            )

        # Margem Líquida
        if 'margem_liquida_pct' not in df.columns:
            df = df.withColumn(
                'margem_liquida_pct',
                F.when(
                    F.col('receita_liquida') != 0,
                    (F.col('lucro_liquido') / F.col('receita_liquida')) * 100
                ).otherwise(0)
            )

        # Margem EBITDA (se tiver EBITDA)
        if 'ebit' in df.columns:
            # EBITDA simplificado = EBIT (não temos depreciação/amortização por enquanto)
            df = df.withColumn(
                'ebitda',
                F.col('ebit')  # Simplificado
            ).withColumn(
                'margem_ebitda_pct',
                F.when(
                    F.col('receita_liquida') != 0,
                    (F.col('ebitda') / F.col('receita_liquida')) * 100
                ).otherwise(0)
            )

        return df

    def _calcular_retorno(self, df: DataFrame) -> DataFrame:
        """
        Calcula indicadores de retorno

        1. ROA (Return on Assets) = Lucro Líquido / Ativo Total
        2. ROE (Return on Equity) = Lucro Líquido / Patrimônio Líquido
        3. ROI (Return on Investment) = Lucro Líquido / (Ativo - Passivo Circulante)
        """
        self.logger.debug("Calculando indicadores de retorno...")

        # ROA - Retorno sobre Ativos
        df = df.withColumn(
            'roa',
            F.when(
                F.col('ativo_total') != 0,
                (F.col('lucro_liquido') / F.col('ativo_total')) * 100
            ).otherwise(None)
        )

        # ROE - Retorno sobre Patrimônio Líquido
        df = df.withColumn(
            'roe',
            F.when(
                F.col('patrimonio_liquido_total') != 0,
                (F.col('lucro_liquido') / F.col('patrimonio_liquido_total')) * 100
            ).otherwise(None)
        )

        # ROIC - Retorno sobre Capital Investido (simplificado)
        # ROIC = EBIT / (Ativo Total - Passivo Circulante)
        if 'ebit' in df.columns and 'passivo_circulante' in df.columns:
            df = df.withColumn(
                'roic',
                F.when(
                    (F.col('ativo_total') - F.col('passivo_circulante')) != 0,
                    (F.col('ebit') / (F.col('ativo_total') - F.col('passivo_circulante'))) * 100
                ).otherwise(None)
            )

        return df

    def _calcular_giro(self, df: DataFrame) -> DataFrame:
        """
        Calcula indicadores de giro

        1. Giro do Ativo = Receita Líquida / Ativo Total
        2. Giro do PL = Receita Líquida / PL
        """
        self.logger.debug("Calculando indicadores de giro...")

        # Giro do Ativo
        df = df.withColumn(
            'giro_ativo',
            F.when(
                F.col('ativo_total') != 0,
                F.col('receita_liquida') / F.col('ativo_total')
            ).otherwise(None)
        )

        # Giro do PL
        df = df.withColumn(
            'giro_pl',
            F.when(
                F.col('patrimonio_liquido_total') != 0,
                F.col('receita_liquida') / F.col('patrimonio_liquido_total')
            ).otherwise(None)
        )

        return df

    def calcular_dupont(self, df: DataFrame) -> DataFrame:
        """
        Análise DuPont: ROE = Margem Líquida × Giro do Ativo × Alavancagem

        ROE = (Lucro Líquido / Receita) × (Receita / Ativo) × (Ativo / PL)
        """
        self.logger.info("Calculando análise DuPont...")

        # Margem Líquida (já calculada)
        # Giro do Ativo (já calculado)

        # Multiplicador de Alavancagem = Ativo / PL
        df = df.withColumn(
            'multiplicador_alavancagem',
            F.when(
                F.col('patrimonio_liquido_total') != 0,
                F.col('ativo_total') / F.col('patrimonio_liquido_total')
            ).otherwise(None)
        )

        # ROE pela fórmula DuPont
        df = df.withColumn(
            'roe_dupont',
            (F.col('margem_liquida_pct') / 100) *
            F.col('giro_ativo') *
            F.col('multiplicador_alavancagem') *
            100
        )

        return df

    def classificar_rentabilidade(self, df: DataFrame) -> DataFrame:
        """
        Classifica empresa quanto à rentabilidade

        Args:
            df: DataFrame com indicadores de rentabilidade

        Returns:
            DataFrame com classificação
        """
        self.logger.info("Classificando rentabilidade...")

        # Score ROE
        df = df.withColumn(
            'score_roe',
            F.when(F.col('roe') >= 20, 100)
             .when(F.col('roe') >= 15, 80)
             .when(F.col('roe') >= 10, 60)
             .when(F.col('roe') >= 5, 40)
             .when(F.col('roe') >= 0, 20)
             .otherwise(0)
        )

        # Score Margem Líquida
        df = df.withColumn(
            'score_margem',
            F.when(F.col('margem_liquida_pct') >= 20, 100)
             .when(F.col('margem_liquida_pct') >= 10, 70)
             .when(F.col('margem_liquida_pct') >= 5, 50)
             .when(F.col('margem_liquida_pct') >= 0, 30)
             .otherwise(0)
        )

        # Score geral de rentabilidade
        df = df.withColumn(
            'score_rentabilidade',
            (F.col('score_roe') + F.col('score_margem')) / 2
        )

        # Classificação
        df = df.withColumn(
            'classificacao_rentabilidade',
            F.when(F.col('score_rentabilidade') >= 80, 'Excelente')
             .when(F.col('score_rentabilidade') >= 60, 'Boa')
             .when(F.col('score_rentabilidade') >= 40, 'Regular')
             .when(F.col('score_rentabilidade') >= 20, 'Ruim')
             .otherwise('Crítica')
        )

        return df

    def calcular_ebitda(
        self,
        df: DataFrame,
        deprec_amort_col: Optional[str] = None
    ) -> DataFrame:
        """
        Calcula EBITDA

        EBITDA = EBIT + Depreciação + Amortização

        Args:
            df: DataFrame com EBIT
            deprec_amort_col: Nome da coluna com depreciação/amortização (opcional)

        Returns:
            DataFrame com EBITDA
        """
        if deprec_amort_col and deprec_amort_col in df.columns:
            df = df.withColumn(
                'ebitda',
                F.col('ebit') + F.col(deprec_amort_col)
            )
        else:
            # Se não tiver depreciação, EBITDA = EBIT (simplificado)
            df = df.withColumn(
                'ebitda',
                F.col('ebit')
            )

        return df
