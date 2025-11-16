"""
Pipeline híbrido de classificação: Regras + ML + Validação Cruzada
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
from datetime import datetime
import logging

from .regras import ClassificadorRegras
from .ml_classifier import ClassificadorML
from ..utils.spark_helper import add_classification_columns, validate_required_columns
from ..utils.logger import setup_logger, log_execution_time


class PipelineClassificacao:
    """
    Pipeline completo de classificação de contas contábeis

    Estratégia:
    1. Classificação por regras (rápida, determinística, ~70% cobertura)
    2. Classificação por ML (para casos não cobertos por regras)
    3. Validação cruzada (BP x DRE)
    4. Revisão manual (contas não classificadas ou baixa confiança)
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Args:
            spark: Sessão Spark
            config: Configurações do pipeline
            logger: Logger
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logger or setup_logger('pipeline_classificacao')

        # Inicializar classificadores
        self.classificador_regras = ClassificadorRegras(logger=self.logger)
        self.classificador_ml = ClassificadorML(
            spark=spark,
            model_path=self.config.get('ml_model_path'),
            logger=self.logger
        )

    def classificar(
        self,
        df_contas: DataFrame,
        usar_ml: bool = True,
        confidence_threshold: float = 0.7
    ) -> DataFrame:
        """
        Executa pipeline completo de classificação

        Args:
            df_contas: DataFrame com contas a classificar (cd_cta, desc_cta, cd_natureza)
            usar_ml: Se deve usar classificação ML além de regras
            confidence_threshold: Threshold de confiança para aceitar classificação ML

        Returns:
            DataFrame com contas classificadas
        """
        start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("INICIANDO PIPELINE DE CLASSIFICAÇÃO")
        self.logger.info("=" * 80)

        # Validar colunas obrigatórias
        validate_required_columns(
            df_contas,
            ['cd_cta', 'desc_cta', 'cd_natureza'],
            'df_contas'
        )

        total_contas = df_contas.count()
        self.logger.info(f"Total de contas a classificar: {total_contas:,}")

        # Adicionar colunas de classificação
        df = add_classification_columns(df_contas)

        # ETAPA 1: Classificação por Regras
        self.logger.info("\n" + "=" * 80)
        self.logger.info("ETAPA 1: Classificação por Regras Hierárquicas")
        self.logger.info("=" * 80)

        df = self.classificador_regras.classificar(df)

        classificadas_regras = df.filter(
            F.col('metodo_classificacao') == 'regras'
        ).count()
        taxa_regras = (classificadas_regras / total_contas * 100) if total_contas > 0 else 0

        self.logger.info(
            f"Classificadas por regras: {classificadas_regras:,} ({taxa_regras:.1f}%)"
        )

        # ETAPA 2: Classificação por ML (se habilitado e houver contas não classificadas)
        nao_classificadas = df.filter(F.col('metodo_classificacao').isNull()).count()

        if usar_ml and nao_classificadas > 0:
            self.logger.info("\n" + "=" * 80)
            self.logger.info("ETAPA 2: Classificação por Machine Learning")
            self.logger.info("=" * 80)
            self.logger.info(f"Contas restantes para ML: {nao_classificadas:,}")

            try:
                df = self.classificador_ml.classificar(df, confidence_threshold)

                classificadas_ml = df.filter(
                    F.col('metodo_classificacao') == 'ml'
                ).count()
                taxa_ml = (classificadas_ml / total_contas * 100) if total_contas > 0 else 0

                self.logger.info(
                    f"Classificadas por ML: {classificadas_ml:,} ({taxa_ml:.1f}%)"
                )
            except Exception as e:
                self.logger.warning(f"Erro na classificação ML: {str(e)}")
                self.logger.warning("Continuando sem classificação ML...")

        # ETAPA 3: Estatísticas Finais
        self.logger.info("\n" + "=" * 80)
        self.logger.info("ESTATÍSTICAS FINAIS")
        self.logger.info("=" * 80)

        stats = self._calcular_estatisticas(df, total_contas)

        for key, value in stats.items():
            self.logger.info(f"{key}: {value}")

        log_execution_time(self.logger, start_time, "Pipeline de classificação")

        return df

    def treinar_modelo_ml(
        self,
        df_treino: DataFrame,
        df_validacao: Optional[DataFrame] = None,
        salvar_modelo: bool = True
    ) -> Dict[str, float]:
        """
        Treina modelo de ML usando contas já classificadas

        Args:
            df_treino: DataFrame com contas classificadas (ground truth)
            df_validacao: DataFrame de validação (opcional)
            salvar_modelo: Se deve salvar modelo após treino

        Returns:
            Dicionário com métricas de treino
        """
        self.logger.info("\n" + "=" * 80)
        self.logger.info("TREINO DO MODELO DE MACHINE LEARNING")
        self.logger.info("=" * 80)

        start_time = datetime.now()

        metricas = self.classificador_ml.treinar(
            df_treino=df_treino,
            df_validacao=df_validacao
        )

        log_execution_time(self.logger, start_time, "Treino do modelo ML")

        return metricas

    def _calcular_estatisticas(
        self,
        df: DataFrame,
        total: int
    ) -> Dict[str, str]:
        """Calcula estatísticas da classificação"""

        classificadas = df.filter(F.col('metodo_classificacao').isNotNull()).count()
        nao_classificadas = total - classificadas

        por_regras = df.filter(F.col('metodo_classificacao') == 'regras').count()
        por_ml = df.filter(F.col('metodo_classificacao') == 'ml').count()

        stats = {
            'Total de contas': f'{total:,}',
            'Classificadas': f'{classificadas:,} ({classificadas/total*100:.1f}%)',
            'Não classificadas': f'{nao_classificadas:,} ({nao_classificadas/total*100:.1f}%)',
            '  - Por regras': f'{por_regras:,} ({por_regras/total*100:.1f}%)',
            '  - Por ML': f'{por_ml:,} ({por_ml/total*100:.1f}%)'
        }

        # Estatísticas por nível
        for nivel in [1, 2, 3]:
            col = f'classificacao_nivel{nivel}'
            count = df.filter(F.col(col).isNotNull()).count()
            stats[f'Nível {nivel} classificado'] = f'{count:,} ({count/total*100:.1f}%)'

        return stats

    def exportar_nao_classificadas(
        self,
        df: DataFrame,
        output_path: str
    ):
        """
        Exporta contas não classificadas para revisão manual

        Args:
            df: DataFrame classificado
            output_path: Caminho para salvar CSV
        """
        self.logger.info(f"Exportando contas não classificadas para {output_path}")

        df_nao_class = df.filter(F.col('metodo_classificacao').isNull()) \
                         .select('cd_cta', 'desc_cta', 'cd_natureza') \
                         .orderBy('cd_cta')

        count = df_nao_class.count()
        self.logger.info(f"Total de contas não classificadas: {count:,}")

        df_nao_class.coalesce(1).write.mode('overwrite').csv(
            output_path,
            header=True
        )

        self.logger.info(f"Exportação concluída: {output_path}")

    def gerar_relatorio_qualidade(
        self,
        df: DataFrame,
        df_ground_truth: Optional[DataFrame] = None
    ) -> DataFrame:
        """
        Gera relatório de qualidade da classificação

        Args:
            df: DataFrame classificado
            df_ground_truth: DataFrame com classificação correta (para comparação)

        Returns:
            DataFrame com métricas de qualidade
        """
        self.logger.info("Gerando relatório de qualidade...")

        # Estatísticas por método
        stats_metodo = df.groupBy('metodo_classificacao') \
                         .agg(F.count('*').alias('quantidade'),
                              F.avg('confianca_classificacao').alias('confianca_media'))

        # Estatísticas por nível de classificação
        stats_nivel1 = df.filter(F.col('classificacao_nivel1').isNotNull()) \
                         .groupBy('classificacao_nivel1', 'nome_nivel1') \
                         .agg(F.count('*').alias('quantidade'))

        # Se tiver ground truth, calcular acurácia
        if df_ground_truth is not None:
            self.logger.info("Calculando acurácia comparando com ground truth...")
            # Join e comparação
            df_comparacao = df.alias('pred').join(
                df_ground_truth.alias('truth'),
                F.col('pred.cd_cta') == F.col('truth.cd_cta'),
                'inner'
            )

            for nivel in [1, 2, 3]:
                corretas = df_comparacao.filter(
                    F.col(f'pred.classificacao_nivel{nivel}') ==
                    F.col(f'truth.classificacao_nivel{nivel}')
                ).count()

                total = df_comparacao.count()
                acuracia = (corretas / total * 100) if total > 0 else 0

                self.logger.info(f"Acurácia nível {nivel}: {acuracia:.2f}%")

        return stats_metodo
