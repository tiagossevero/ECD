"""
Classificador de contas baseado em regras hierárquicas
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import Optional
import logging

from ..demonstracoes.estrutura_contas import EstruturaContas


class ClassificadorRegras:
    """
    Classifica contas contábeis usando regras hierárquicas baseadas em:
    - Natureza da conta (cd_natureza)
    - Código da conta
    - Palavras-chave na descrição
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.estrutura = EstruturaContas()

    def classificar(self, df: DataFrame) -> DataFrame:
        """
        Classifica contas do DataFrame usando regras

        Args:
            df: DataFrame com colunas 'cd_cta', 'desc_cta', 'cd_natureza'

        Returns:
            DataFrame com colunas de classificação adicionadas
        """
        self.logger.info("Iniciando classificação por regras...")

        # Passo 1: Classificar por natureza (nível 1)
        df = self._classificar_nivel1_por_natureza(df)

        # Passo 2: Classificar por código da conta (nível 2)
        df = self._classificar_nivel2_por_codigo(df)

        # Passo 3: Classificar por keywords (nível 3)
        df = self._classificar_nivel3_por_keywords(df)

        # Adicionar nomes das classificações
        df = self._adicionar_nomes_classificacao(df)

        # Marcar método de classificação
        df = df.withColumn(
            'metodo_classificacao',
            F.when(
                F.col('classificacao_nivel3').isNotNull(),
                F.lit('regras')
            ).otherwise(F.col('metodo_classificacao'))
        ).withColumn(
            'confianca_classificacao',
            F.when(
                F.col('classificacao_nivel3').isNotNull(),
                F.lit(1.0)
            ).otherwise(F.col('confianca_classificacao'))
        )

        count_classificadas = df.filter(F.col('classificacao_nivel1').isNotNull()).count()
        count_total = df.count()
        taxa = (count_classificadas / count_total * 100) if count_total > 0 else 0

        self.logger.info(
            f"Classificação por regras concluída: "
            f"{count_classificadas:,} de {count_total:,} contas ({taxa:.1f}%)"
        )

        return df

    def _classificar_nivel1_por_natureza(self, df: DataFrame) -> DataFrame:
        """
        Classifica nível 1 baseado na natureza da conta
        01 (Ativa) -> 1 (ATIVO)
        02 (Passiva) -> 2 (PASSIVO)
        04 (PL) -> 3 (PATRIMÔNIO LÍQUIDO)
        07 (Receita) -> 4 (RECEITAS)
        08 (Despesa) -> 5 ou 6 (CUSTOS/DESPESAS)
        """
        return df.withColumn(
            'classificacao_nivel1',
            F.when(F.col('cd_natureza') == '01', F.lit('1'))  # ATIVO
             .when(F.col('cd_natureza') == '02', F.lit('2'))  # PASSIVO
             .when(F.col('cd_natureza') == '04', F.lit('3'))  # PL
             .when(F.col('cd_natureza') == '07', F.lit('4'))  # RECEITAS
             .when(F.col('cd_natureza') == '08', F.lit('6'))  # DESPESAS (padrão)
             .otherwise(F.col('classificacao_nivel1'))
        )

    def _classificar_nivel2_por_codigo(self, df: DataFrame) -> DataFrame:
        """
        Classifica nível 2 baseado em padrões do código da conta
        Assume estrutura hierárquica: 1.01.xxx = Ativo Circulante
        """
        # Para ATIVO (1)
        df = df.withColumn(
            'classificacao_nivel2',
            F.when(
                (F.col('classificacao_nivel1') == '1') &
                (F.col('cd_cta').rlike(r'^1\.01\.') |
                 F.lower(F.col('desc_cta')).rlike(r'circulante|curto prazo|cp')),
                F.lit('1.01')
            ).when(
                (F.col('classificacao_nivel1') == '1'),
                F.lit('1.02')  # Padrão: Não Circulante
            # Para PASSIVO (2)
            ).when(
                (F.col('classificacao_nivel1') == '2') &
                (F.col('cd_cta').rlike(r'^2\.01\.') |
                 F.lower(F.col('desc_cta')).rlike(r'circulante|curto prazo|cp')),
                F.lit('2.01')
            ).when(
                (F.col('classificacao_nivel1') == '2'),
                F.lit('2.02')  # Padrão: Não Circulante
            # Para PL (3) - não tem nível 2 na estrutura simplificada
            ).when(
                F.col('classificacao_nivel1') == '3',
                F.lit('3')
            # Para RECEITAS (4)
            ).when(
                F.col('classificacao_nivel1') == '4',
                F.lit('4.01')  # Padrão: Receita Bruta
            # Para CUSTOS (5)
            ).when(
                F.col('classificacao_nivel1') == '5',
                F.lit('5.01')  # Padrão: CMV
            # Para DESPESAS (6)
            ).when(
                F.col('classificacao_nivel1') == '6',
                F.lit('6.01')  # Padrão: Despesas Operacionais
            ).otherwise(F.col('classificacao_nivel2'))
        )

        return df

    def _classificar_nivel3_por_keywords(self, df: DataFrame) -> DataFrame:
        """
        Classifica nível 3 usando palavras-chave da descrição
        """
        # Registrar UDF para busca de keywords
        def match_keywords_udf(descricao: str, nivel2: str) -> Optional[str]:
            if not descricao or not nivel2:
                return None

            descricao_lower = descricao.lower()

            # Buscar contas filhas do nível 2
            contas_nivel3 = [
                c for c in self.estrutura.todas_contas.values()
                if c.parent == nivel2 and c.nivel == 3
            ]

            # Encontrar melhor match
            best_match = None
            best_score = 0

            for conta in contas_nivel3:
                score = sum(1 for kw in conta.keywords if kw in descricao_lower)
                if score > best_score:
                    best_score = score
                    best_match = conta.codigo

            return best_match

        # Criar UDF Spark
        match_udf = F.udf(match_keywords_udf, StringType())

        df = df.withColumn(
            'classificacao_nivel3',
            match_udf(F.col('desc_cta'), F.col('classificacao_nivel2'))
        )

        return df

    def _adicionar_nomes_classificacao(self, df: DataFrame) -> DataFrame:
        """Adiciona nomes legíveis às classificações"""

        def get_nome_nivel1(codigo: str) -> Optional[str]:
            conta = self.estrutura.get_conta(codigo)
            return conta.nome if conta else None

        def get_nome_nivel2(codigo: str) -> Optional[str]:
            conta = self.estrutura.get_conta(codigo)
            return conta.nome if conta else None

        def get_nome_nivel3(codigo: str) -> Optional[str]:
            conta = self.estrutura.get_conta(codigo)
            return conta.nome if conta else None

        nome_nivel1_udf = F.udf(get_nome_nivel1, StringType())
        nome_nivel2_udf = F.udf(get_nome_nivel2, StringType())
        nome_nivel3_udf = F.udf(get_nome_nivel3, StringType())

        return df.withColumn('nome_nivel1', nome_nivel1_udf(F.col('classificacao_nivel1'))) \
                 .withColumn('nome_nivel2', nome_nivel2_udf(F.col('classificacao_nivel2'))) \
                 .withColumn('nome_nivel3', nome_nivel3_udf(F.col('classificacao_nivel3')))
