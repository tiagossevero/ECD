"""
Classificador de contas usando Machine Learning (Random Forest + TF-IDF)
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from typing import Optional, Tuple
import logging


class ClassificadorML:
    """
    Classifica contas contábeis usando Machine Learning
    Utiliza TF-IDF para análise semântica das descrições + Random Forest
    """

    def __init__(
        self,
        spark: SparkSession,
        model_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        self.spark = spark
        self.model_path = model_path
        self.logger = logger or logging.getLogger(__name__)
        self.model: Optional[PipelineModel] = None
        self.label_cols = ['classificacao_nivel1', 'classificacao_nivel2', 'classificacao_nivel3']

    def treinar(
        self,
        df_treino: DataFrame,
        df_validacao: Optional[DataFrame] = None,
        test_size: float = 0.2
    ) -> dict:
        """
        Treina o modelo de classificação

        Args:
            df_treino: DataFrame com contas já classificadas (ground truth)
            df_validacao: DataFrame opcional para validação
            test_size: Proporção para split treino/teste se df_validacao não fornecido

        Returns:
            Dicionário com métricas de treino
        """
        self.logger.info("Iniciando treino do modelo ML...")

        # Preparar dados
        df_treino = self._preparar_dados_treino(df_treino)

        # Split treino/teste se validação não fornecida
        if df_validacao is None:
            df_treino, df_validacao = df_treino.randomSplit([1-test_size, test_size], seed=42)

        self.logger.info(f"Treino: {df_treino.count():,} registros")
        self.logger.info(f"Validação: {df_validacao.count():,} registros")

        # Treinar modelo para cada nível
        metricas = {}
        for nivel in [1, 2, 3]:
            self.logger.info(f"Treinando classificador nível {nivel}...")
            model, accuracy = self._treinar_nivel(
                df_treino,
                df_validacao,
                nivel
            )
            metricas[f'nivel_{nivel}_accuracy'] = accuracy

        # Salvar modelo se caminho especificado
        if self.model_path:
            self.model.write().overwrite().save(self.model_path)
            self.logger.info(f"Modelo salvo em {self.model_path}")

        return metricas

    def _preparar_dados_treino(self, df: DataFrame) -> DataFrame:
        """
        Prepara dados para treino:
        - Remove registros sem classificação
        - Limpa textos
        - Remove nulos
        """
        # Filtrar apenas registros com classificação válida
        df = df.filter(
            F.col('classificacao_nivel1').isNotNull() &
            F.col('desc_cta').isNotNull()
        )

        # Limpar descrição
        df = df.withColumn(
            'desc_cta_limpa',
            F.lower(F.trim(F.regexp_replace(F.col('desc_cta'), r'[^\w\s]', ' ')))
        )

        return df

    def _treinar_nivel(
        self,
        df_treino: DataFrame,
        df_validacao: DataFrame,
        nivel: int
    ) -> Tuple[PipelineModel, float]:
        """
        Treina modelo para um nível específico

        Args:
            df_treino: Dados de treino
            df_validacao: Dados de validação
            nivel: Nível a treinar (1, 2 ou 3)

        Returns:
            Tuple (modelo, acurácia)
        """
        label_col = f'classificacao_nivel{nivel}'

        # Filtrar registros com label válida para este nível
        df_treino_nivel = df_treino.filter(F.col(label_col).isNotNull())
        df_val_nivel = df_validacao.filter(F.col(label_col).isNotNull())

        # Tokenização
        tokenizer = Tokenizer(inputCol="desc_cta_limpa", outputCol="words")

        # TF (Term Frequency)
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)

        # IDF (Inverse Document Frequency)
        idf = IDF(inputCol="rawFeatures", outputCol="tfidf_features")

        # Indexar labels
        label_indexer = StringIndexer(
            inputCol=label_col,
            outputCol=f"label_{nivel}",
            handleInvalid="skip"
        )

        # Features adicionais: natureza, nível hierárquico
        # (se tiver essas colunas)
        feature_cols = ["tfidf_features"]

        # Assembler de features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features",
            handleInvalid="skip"
        )

        # Random Forest
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol=f"label_{nivel}",
            predictionCol=f"prediction_{nivel}",
            probabilityCol=f"probability_{nivel}",
            numTrees=100,
            maxDepth=10,
            seed=42
        )

        # Converter índice de volta para label
        label_converter = IndexToString(
            inputCol=f"prediction_{nivel}",
            outputCol=f"predicted_{label_col}",
            labels=label_indexer.labels
        )

        # Pipeline
        pipeline = Pipeline(stages=[
            tokenizer,
            hashingTF,
            idf,
            label_indexer,
            assembler,
            rf,
            label_converter
        ])

        # Treinar
        self.logger.info(f"Treinando Random Forest para nível {nivel}...")
        model = pipeline.fit(df_treino_nivel)

        # Avaliar
        predictions = model.transform(df_val_nivel)
        evaluator = MulticlassClassificationEvaluator(
            labelCol=f"label_{nivel}",
            predictionCol=f"prediction_{nivel}",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)

        self.logger.info(f"Acurácia nível {nivel}: {accuracy:.2%}")

        # Salvar modelo deste nível
        if nivel == 1:
            self.model = model

        return model, accuracy

    def classificar(
        self,
        df: DataFrame,
        confidence_threshold: float = 0.7
    ) -> DataFrame:
        """
        Classifica contas usando modelo treinado

        Args:
            df: DataFrame com contas a classificar
            confidence_threshold: Threshold mínimo de confiança

        Returns:
            DataFrame com classificações
        """
        if self.model is None:
            if self.model_path:
                self.logger.info(f"Carregando modelo de {self.model_path}")
                self.model = PipelineModel.load(self.model_path)
            else:
                raise ValueError("Modelo não treinado e caminho não especificado")

        self.logger.info("Classificando contas com ML...")

        # Preparar dados
        df = df.withColumn(
            'desc_cta_limpa',
            F.lower(F.trim(F.regexp_replace(F.col('desc_cta'), r'[^\w\s]', ' ')))
        )

        # Aplicar modelo
        df_predicted = self.model.transform(df)

        # Aplicar threshold de confiança
        # Pegar probabilidade máxima como confiança
        df_predicted = df_predicted.withColumn(
            'confianca_ml',
            F.udf(
                lambda prob: float(max(prob)) if prob else 0.0,
                'double'
            )(F.col('probability_1'))
        )

        # Usar classificação ML apenas se confiança >= threshold
        # E se não foi classificado por regras
        df_predicted = df_predicted.withColumn(
            'classificacao_nivel1',
            F.when(
                (F.col('classificacao_nivel1').isNull()) &
                (F.col('confianca_ml') >= confidence_threshold),
                F.col('predicted_classificacao_nivel1')
            ).otherwise(F.col('classificacao_nivel1'))
        ).withColumn(
            'metodo_classificacao',
            F.when(
                (F.col('metodo_classificacao').isNull()) &
                (F.col('confianca_ml') >= confidence_threshold),
                F.lit('ml')
            ).otherwise(F.col('metodo_classificacao'))
        ).withColumn(
            'confianca_classificacao',
            F.when(
                F.col('metodo_classificacao') == 'ml',
                F.col('confianca_ml')
            ).otherwise(F.col('confianca_classificacao'))
        )

        count_classificadas = df_predicted.filter(
            F.col('metodo_classificacao') == 'ml'
        ).count()

        self.logger.info(f"ML classificou {count_classificadas:,} contas adicionais")

        return df_predicted

    def avaliar_modelo(self, df_teste: DataFrame) -> dict:
        """
        Avalia modelo em dados de teste

        Args:
            df_teste: DataFrame de teste com ground truth

        Returns:
            Dicionário com métricas
        """
        if self.model is None:
            raise ValueError("Modelo não treinado")

        predictions = self.model.transform(df_teste)

        metricas = {}
        for nivel in [1, 2, 3]:
            label_col = f"label_{nivel}"
            pred_col = f"prediction_{nivel}"

            if label_col in predictions.columns:
                evaluator = MulticlassClassificationEvaluator(
                    labelCol=label_col,
                    predictionCol=pred_col,
                    metricName="accuracy"
                )
                accuracy = evaluator.evaluate(predictions)
                metricas[f'nivel_{nivel}_accuracy'] = accuracy

        return metricas
