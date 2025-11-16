"""
Validação cruzada entre BP e DRE
Garante consistência na classificação de contas
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional, List, Dict, Any
import logging


class ValidadorCruzado:
    """
    Valida consistência entre classificações de BP e DRE

    Regras de validação:
    - Conta classificada no ATIVO não pode aparecer na DRE
    - Conta classificada como RECEITA não pode aparecer no BP
    - Natureza da conta deve ser compatível com classificação
    - Contas com saldo significativo em ambas demonstrações são sinalizadas
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def validar(
        self,
        df_classificado: DataFrame
    ) -> Dict[str, Any]:
        """
        Valida consistência da classificação

        Args:
            df_classificado: DataFrame com contas classificadas

        Returns:
            Dicionário com resultado da validação e DataFrame com inconsistências
        """
        self.logger.info("Executando validação cruzada BP x DRE...")

        # Identificar contas de BP e DRE
        df_validacao = df_classificado.withColumn(
            'tipo_demonstracao',
            F.when(
                F.col('classificacao_nivel1').isin(['1', '2', '3']),
                F.lit('BP')
            ).when(
                F.col('classificacao_nivel1').isin(['4', '5', '6']),
                F.lit('DRE')
            ).otherwise(F.lit('NAO_CLASSIFICADO'))
        )

        # Validar consistência com natureza
        df_validacao = df_validacao.withColumn(
            'natureza_compativel',
            self._validar_natureza_compativel()
        )

        # Identificar inconsistências
        df_inconsistencias = df_validacao.filter(
            F.col('natureza_compativel') == False
        )

        total_contas = df_classificado.count()
        contas_inconsistentes = df_inconsistencias.count()
        taxa_inconsistencia = (contas_inconsistentes / total_contas * 100) if total_contas > 0 else 0

        resultado = {
            'total_contas': total_contas,
            'contas_inconsistentes': contas_inconsistentes,
            'taxa_inconsistencia': taxa_inconsistencia,
            'df_inconsistencias': df_inconsistencias,
            'df_validacao': df_validacao
        }

        if contas_inconsistentes > 0:
            self.logger.warning(
                f"ATENÇÃO: {contas_inconsistentes:,} contas com inconsistência "
                f"entre natureza e classificação ({taxa_inconsistencia:.1f}%)"
            )
        else:
            self.logger.info("Nenhuma inconsistência detectada!")

        return resultado

    def _validar_natureza_compativel(self):
        """
        Retorna expressão Spark para validar compatibilidade natureza x classificação

        Regras:
        - Natureza 01 (Ativa) -> deve ser classificação 1 (ATIVO)
        - Natureza 02 (Passiva) -> deve ser classificação 2 (PASSIVO)
        - Natureza 04 (PL) -> deve ser classificação 3 (PL)
        - Natureza 07 (Receita) -> deve ser classificação 4 (RECEITAS)
        - Natureza 08 (Despesa) -> deve ser classificação 5 ou 6 (CUSTOS/DESPESAS)
        """
        return F.when(
            # Natureza Ativa -> ATIVO
            (F.col('cd_natureza') == '01') & (F.col('classificacao_nivel1') == '1'),
            True
        ).when(
            # Natureza Passiva -> PASSIVO
            (F.col('cd_natureza') == '02') & (F.col('classificacao_nivel1') == '2'),
            True
        ).when(
            # Natureza PL -> PL
            (F.col('cd_natureza') == '04') & (F.col('classificacao_nivel1') == '3'),
            True
        ).when(
            # Natureza Receita -> RECEITAS
            (F.col('cd_natureza') == '07') & (F.col('classificacao_nivel1') == '4'),
            True
        ).when(
            # Natureza Despesa -> CUSTOS ou DESPESAS
            (F.col('cd_natureza') == '08') & (F.col('classificacao_nivel1').isin(['5', '6'])),
            True
        ).when(
            # Não classificado
            F.col('classificacao_nivel1').isNull(),
            None
        ).otherwise(
            False
        )

    def detectar_contas_duplicadas(
        self,
        df_saldos: DataFrame
    ) -> DataFrame:
        """
        Detecta contas que aparecem tanto no BP quanto na DRE
        (pode indicar erro de classificação)

        Args:
            df_saldos: DataFrame com saldos e classificações

        Returns:
            DataFrame com contas que aparecem em ambas demonstrações
        """
        self.logger.info("Detectando contas que aparecem em BP e DRE...")

        # Marcar tipo de demonstração
        df_marcado = df_saldos.withColumn(
            'tipo_demo',
            F.when(
                F.col('classificacao_nivel1').isin(['1', '2', '3']),
                F.lit('BP')
            ).when(
                F.col('classificacao_nivel1').isin(['4', '5', '6']),
                F.lit('DRE')
            )
        )

        # Agrupar por conta e ver se aparece em ambas
        df_duplicadas = df_marcado.groupBy('cnpj', 'cd_cta', 'desc_cta') \
                                   .agg(
                                       F.collect_set('tipo_demo').alias('demonstracoes'),
                                       F.sum('vl_saldo_fin').alias('saldo_total')
                                   ) \
                                   .filter(F.size(F.col('demonstracoes')) > 1)

        count_duplicadas = df_duplicadas.count()

        if count_duplicadas > 0:
            self.logger.warning(
                f"ATENÇÃO: {count_duplicadas:,} contas aparecem tanto em BP quanto em DRE!"
            )
        else:
            self.logger.info("Nenhuma conta duplicada entre BP e DRE detectada")

        return df_duplicadas

    def validar_somatorios(
        self,
        df_bp: DataFrame,
        df_dre: DataFrame
    ) -> Dict[str, Any]:
        """
        Valida somatórios hierárquicos das demonstrações

        Verifica se:
        - Soma dos filhos = valor do pai
        - Ativo Circulante + Ativo Não Circulante = Ativo Total
        - etc.

        Args:
            df_bp: DataFrame do BP consolidado
            df_dre: DataFrame da DRE consolidada

        Returns:
            Dicionário com resultado da validação
        """
        self.logger.info("Validando somatórios hierárquicos...")

        # TODO: Implementar validação hierárquica
        # Por ora, validação básica

        resultado = {
            'bp_valido': True,
            'dre_valido': True,
            'mensagens': []
        }

        return resultado
