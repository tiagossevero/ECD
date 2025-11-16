"""
Gerador de Demonstração do Resultado do Exercício (DRE)
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional, List
import logging

from .estrutura_contas import EstruturaContas


class GeradorDRE:
    """
    Gera DRE a partir de contas classificadas

    Estrutura da DRE:
    (+) Receita Bruta
    (-) Deduções
    (=) Receita Líquida
    (-) CMV/CSP
    (=) Lucro Bruto
    (-) Despesas Operacionais
    (=) Lucro Operacional
    (+/-) Receitas/Despesas Financeiras
    (=) Lucro Antes de IR
    (-) IR/CSLL
    (=) Lucro Líquido
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
        Gera DRE

        Args:
            df_saldos: DataFrame com saldos classificados
                       Colunas: cnpj, ano, cd_cta, classificacao_nivel1, classificacao_nivel2,
                               classificacao_nivel3, vl_saldo_fin
            agrupar_por: Colunas adicionais para agrupamento

        Returns:
            DataFrame com DRE estruturada
        """
        self.logger.info("Gerando DRE...")

        # Filtrar apenas contas da DRE (classificacao_nivel1 in 4, 5, 6)
        df_dre = df_saldos.filter(
            F.col('classificacao_nivel1').isin(['4', '5', '6'])
        )

        # Definir colunas de agrupamento
        group_cols = ['cnpj', 'ano']
        if agrupar_por:
            group_cols.extend(agrupar_por)

        # PARTE 1: Consolidar por níveis
        df_nivel2 = self._consolidar_nivel(df_dre, group_cols, 'classificacao_nivel2', 2)
        df_nivel1 = self._consolidar_nivel(df_dre, group_cols, 'classificacao_nivel1', 1)

        # PARTE 2: Unir níveis
        df_dre_consolidado = df_nivel1.unionByName(df_nivel2)

        # PARTE 3: Adicionar nomes
        df_dre_consolidado = self._adicionar_nomes_contas(df_dre_consolidado)

        # PARTE 4: Calcular indicadores da DRE
        df_dre_final = self._calcular_indicadores_dre(df_dre_consolidado, group_cols)

        # PARTE 5: Ordenar
        df_dre_final = df_dre_final.orderBy('cnpj', 'ano', 'codigo_conta')

        total_empresas = df_dre_final.select('cnpj').distinct().count()
        self.logger.info(f"DRE gerada para {total_empresas:,} empresas")

        return df_dre_final

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
            classificacao_col: Coluna de classificação
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
        """Adiciona nomes legíveis das contas"""

        def get_nome_conta(codigo: str) -> str:
            conta = self.estrutura.get_conta(codigo)
            return conta.nome if conta else codigo

        get_nome_udf = F.udf(get_nome_conta)

        return df.withColumn('nome_conta', get_nome_udf(F.col('codigo_conta')))

    def _calcular_indicadores_dre(
        self,
        df: DataFrame,
        group_cols: List[str]
    ) -> DataFrame:
        """
        Calcula indicadores principais da DRE

        Args:
            df: DataFrame consolidado
            group_cols: Colunas de agrupamento

        Returns:
            DataFrame com indicadores
        """
        # Agregar valores principais
        df_agregado = df.groupBy(group_cols) \
                        .pivot('codigo_conta') \
                        .sum('saldo')

        # Renomear colunas (pode não existir todas)
        colunas_esperadas = {
            '4': 'receitas',
            '4.01': 'receita_bruta',
            '4.02': 'deducoes_receita',
            '4.03': 'receitas_financeiras',
            '4.04': 'outras_receitas',
            '5': 'custos',
            '5.01': 'cmv',
            '5.02': 'csp',
            '6': 'despesas',
            '6.01': 'despesas_operacionais',
            '6.01.01': 'despesas_administrativas',
            '6.01.02': 'despesas_vendas',
            '6.01.03': 'despesas_financeiras',
            '6.02': 'outras_despesas'
        }

        # Renomear colunas existentes
        for cod, nome in colunas_esperadas.items():
            if cod in df_agregado.columns:
                df_agregado = df_agregado.withColumnRenamed(cod, nome)

        # Preencher nulos com 0
        for nome in colunas_esperadas.values():
            if nome in df_agregado.columns:
                df_agregado = df_agregado.fillna(0, subset=[nome])
            else:
                df_agregado = df_agregado.withColumn(nome, F.lit(0))

        # Calcular indicadores
        # Receita Líquida = Receita Bruta - Deduções
        df_agregado = df_agregado.withColumn(
            'receita_liquida',
            F.col('receita_bruta') - F.col('deducoes_receita')
        )

        # Lucro Bruto = Receita Líquida - Custos (CMV/CSP)
        df_agregado = df_agregado.withColumn(
            'lucro_bruto',
            F.col('receita_liquida') - F.col('custos')
        )

        # Margem Bruta %
        df_agregado = df_agregado.withColumn(
            'margem_bruta_pct',
            F.when(
                F.col('receita_liquida') != 0,
                (F.col('lucro_bruto') / F.col('receita_liquida')) * 100
            ).otherwise(0)
        )

        # Lucro Operacional = Lucro Bruto - Despesas Operacionais
        df_agregado = df_agregado.withColumn(
            'lucro_operacional',
            F.col('lucro_bruto') - F.col('despesas_operacionais')
        )

        # Margem Operacional %
        df_agregado = df_agregado.withColumn(
            'margem_operacional_pct',
            F.when(
                F.col('receita_liquida') != 0,
                (F.col('lucro_operacional') / F.col('receita_liquida')) * 100
            ).otherwise(0)
        )

        # EBIT = Lucro Operacional + Despesas Financeiras
        df_agregado = df_agregado.withColumn(
            'ebit',
            F.col('lucro_operacional') + F.col('despesas_financeiras')
        )

        # Resultado Financeiro = Receitas Financeiras - Despesas Financeiras
        df_agregado = df_agregado.withColumn(
            'resultado_financeiro',
            F.col('receitas_financeiras') - F.col('despesas_financeiras')
        )

        # Lucro Antes de IR = Lucro Operacional + Resultado Financeiro + Outras Receitas - Outras Despesas
        df_agregado = df_agregado.withColumn(
            'lucro_antes_ir',
            F.col('lucro_operacional') + F.col('resultado_financeiro') +
            F.col('outras_receitas') - F.col('outras_despesas')
        )

        # Lucro Líquido (simplificado, sem IR/CSLL por enquanto)
        df_agregado = df_agregado.withColumn(
            'lucro_liquido',
            F.col('lucro_antes_ir')  # Simplificado
        )

        # Margem Líquida %
        df_agregado = df_agregado.withColumn(
            'margem_liquida_pct',
            F.when(
                F.col('receita_liquida') != 0,
                (F.col('lucro_liquido') / F.col('receita_liquida')) * 100
            ).otherwise(0)
        )

        return df_agregado

    def gerar_dre_analitica(
        self,
        df_saldos: DataFrame,
        cnpj: str,
        ano: int
    ) -> DataFrame:
        """
        Gera DRE analítica (detalhada) para uma empresa específica

        Args:
            df_saldos: DataFrame com saldos classificados
            cnpj: CNPJ da empresa
            ano: Ano de referência

        Returns:
            DataFrame com DRE analítica
        """
        self.logger.info(f"Gerando DRE analítica para CNPJ {cnpj}, ano {ano}")

        df_empresa = df_saldos.filter(
            (F.col('cnpj') == cnpj) &
            (F.col('ano') == ano) &
            F.col('classificacao_nivel1').isin(['4', '5', '6'])
        )

        df_analitica = df_empresa.select(
            'cd_cta',
            'desc_cta',
            'classificacao_nivel1',
            'classificacao_nivel2',
            'nome_nivel1',
            'nome_nivel2',
            'vl_saldo_fin'
        ).orderBy('classificacao_nivel1', 'classificacao_nivel2', 'cd_cta')

        return df_analitica

    def gerar_dre_comparativa(
        self,
        df_saldos: DataFrame,
        cnpj: str,
        anos: List[int]
    ) -> DataFrame:
        """
        Gera DRE comparativa (múltiplos anos)

        Args:
            df_saldos: DataFrame com saldos classificados
            cnpj: CNPJ da empresa
            anos: Lista de anos para comparação

        Returns:
            DataFrame com DRE comparativa
        """
        self.logger.info(f"Gerando DRE comparativa para CNPJ {cnpj}, anos {anos}")

        df_empresa = df_saldos.filter(
            (F.col('cnpj') == cnpj) &
            (F.col('ano').isin(anos)) &
            F.col('classificacao_nivel1').isin(['4', '5', '6'])
        )

        # Consolidar e pivotar
        df_consolidado = df_empresa.groupBy('classificacao_nivel2', 'ano') \
                                    .agg(F.sum('vl_saldo_fin').alias('saldo'))

        df_comparativa = df_consolidado.groupBy('classificacao_nivel2') \
                                        .pivot('ano') \
                                        .sum('saldo')

        # Adicionar nomes
        df_comparativa = self._adicionar_nomes_contas(
            df_comparativa.withColumnRenamed('classificacao_nivel2', 'codigo_conta')
        )

        # Calcular variações
        if len(anos) == 2:
            anos_sorted = sorted(anos)
            df_comparativa = df_comparativa.withColumn(
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

        return df_comparativa
