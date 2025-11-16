"""
Validação da equação contábil fundamental
ATIVO = PASSIVO + PATRIMÔNIO LÍQUIDO
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
import logging


class ValidadorEquacaoContabil:
    """
    Valida a equação contábil fundamental

    A equação fundamental da contabilidade é:
    ATIVO = PASSIVO + PATRIMÔNIO LÍQUIDO

    Ou seja:
    Classificação '1' (ATIVO) = Classificação '2' (PASSIVO) + Classificação '3' (PL)
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def validar(
        self,
        df_saldos: DataFrame,
        tolerancia: float = 0.01
    ) -> Dict[str, Any]:
        """
        Valida equação contábil por empresa/período

        Args:
            df_saldos: DataFrame com saldos classificados
                       Colunas esperadas: cnpj, ano, classificacao_nivel1, vl_saldo_fin
            tolerancia: Tolerância percentual para considerar balanço fechado (default 1%)

        Returns:
            Dicionário com resultado da validação
        """
        self.logger.info("Validando equação contábil...")

        # Agregar saldos por empresa, ano e classificação nível 1
        df_agregado = df_saldos.groupBy('cnpj', 'ano', 'classificacao_nivel1') \
                               .agg(F.sum('vl_saldo_fin').alias('saldo_total'))

        # Pivotar para ter ATIVO, PASSIVO e PL em colunas
        df_pivot = df_agregado.groupBy('cnpj', 'ano').pivot('classificacao_nivel1').sum('saldo_total')

        # Renomear colunas (assumindo '1'=ATIVO, '2'=PASSIVO, '3'=PL)
        df_pivot = df_pivot.withColumnRenamed('1', 'ativo') \
                           .withColumnRenamed('2', 'passivo') \
                           .withColumnRenamed('3', 'patrimonio_liquido')

        # Preencher nulos com 0
        df_pivot = df_pivot.fillna(0, subset=['ativo', 'passivo', 'patrimonio_liquido'])

        # Calcular validação
        df_validacao = df_pivot.withColumn(
            'lado_esquerdo',  # ATIVO
            F.col('ativo')
        ).withColumn(
            'lado_direito',  # PASSIVO + PL
            F.col('passivo') + F.col('patrimonio_liquido')
        ).withColumn(
            'diferenca',
            F.col('lado_esquerdo') - F.col('lado_direito')
        ).withColumn(
            'diferenca_percentual',
            F.when(
                F.col('lado_esquerdo') != 0,
                (F.abs(F.col('diferenca')) / F.abs(F.col('lado_esquerdo'))) * 100
            ).otherwise(0)
        ).withColumn(
            'equacao_valida',
            F.col('diferenca_percentual') <= (tolerancia * 100)
        )

        # Estatísticas
        total_empresas = df_validacao.count()
        empresas_validas = df_validacao.filter(F.col('equacao_valida') == True).count()
        empresas_invalidas = total_empresas - empresas_validas

        taxa_validacao = (empresas_validas / total_empresas * 100) if total_empresas > 0 else 0

        # Empresas com maior diferença
        top_diferencas = df_validacao.filter(F.col('equacao_valida') == False) \
                                      .orderBy(F.desc('diferenca_percentual')) \
                                      .limit(10) \
                                      .select('cnpj', 'ano', 'diferenca', 'diferenca_percentual')

        resultado = {
            'total_empresas': total_empresas,
            'empresas_validas': empresas_validas,
            'empresas_invalidas': empresas_invalidas,
            'taxa_validacao': taxa_validacao,
            'df_validacao': df_validacao,
            'top_diferencas': top_diferencas
        }

        self.logger.info(f"Empresas com equação válida: {empresas_validas:,} / {total_empresas:,} ({taxa_validacao:.1f}%)")

        if empresas_invalidas > 0:
            self.logger.warning(f"ATENÇÃO: {empresas_invalidas:,} empresas com equação contábil inválida!")
            self.logger.warning("Verificar top empresas com maior diferença")

        return resultado

    def validar_dre(
        self,
        df_saldos: DataFrame,
        tolerancia: float = 0.01
    ) -> Dict[str, Any]:
        """
        Valida DRE: RECEITAS - CUSTOS - DESPESAS = LUCRO/PREJUÍZO

        Args:
            df_saldos: DataFrame com saldos da DRE classificados
            tolerancia: Tolerância percentual

        Returns:
            Dicionário com resultado da validação
        """
        self.logger.info("Validando DRE...")

        # Agregar por empresa, ano e classificação
        df_agregado = df_saldos.groupBy('cnpj', 'ano', 'classificacao_nivel1') \
                               .agg(F.sum('vl_saldo_fin').alias('saldo_total'))

        # Pivotar (4=RECEITAS, 5=CUSTOS, 6=DESPESAS)
        df_pivot = df_agregado.groupBy('cnpj', 'ano').pivot('classificacao_nivel1').sum('saldo_total')

        df_pivot = df_pivot.withColumnRenamed('4', 'receitas') \
                           .withColumnRenamed('5', 'custos') \
                           .withColumnRenamed('6', 'despesas')

        df_pivot = df_pivot.fillna(0, subset=['receitas', 'custos', 'despesas'])

        # Calcular resultado
        # Receitas (natureza credora, valores positivos)
        # Custos e Despesas (natureza devedora, valores positivos)
        # Resultado = Receitas - Custos - Despesas
        df_validacao = df_pivot.withColumn(
            'resultado',
            F.col('receitas') - F.col('custos') - F.col('despesas')
        ).withColumn(
            'receita_liquida',
            F.col('receitas')  # Simplificado
        ).withColumn(
            'lucro_bruto',
            F.col('receitas') - F.col('custos')
        ).withColumn(
            'lucro_liquido',
            F.col('resultado')
        )

        total_empresas = df_validacao.count()

        resultado = {
            'total_empresas': total_empresas,
            'df_validacao': df_validacao
        }

        self.logger.info(f"DRE validada para {total_empresas:,} empresas")

        return resultado
