"""
Pipeline Principal - ECD
Orquestra todo o fluxo: Classificação → BP/DRE → Indicadores → Validação
"""
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import TABELAS, ML_CONFIG
from src.utils.spark_helper import get_spark_session, save_table
from src.utils.logger import setup_logger
from src.classificacao.pipeline import PipelineClassificacao
from src.demonstracoes.balanco_patrimonial import GeradorBalancoPatrimonial
from src.demonstracoes.dre import GeradorDRE
from src.indicadores.financeiros import IndicadoresFinanceiros
from src.indicadores.rentabilidade import IndicadoresRentabilidade
from src.validacao.equacao_contabil import ValidadorEquacaoContabil
from src.validacao.validacao_cruzada import ValidadorCruzado


def executar_pipeline_completo(
    spark: SparkSession,
    modo: str = 'producao',
    usar_ml: bool = True,
    ano_filtro: int = None
):
    """
    Executa pipeline completo de classificação e geração de demonstrações

    Args:
        spark: Sessão Spark
        modo: 'producao' ou 'teste'
        usar_ml: Se deve usar classificação ML
        ano_filtro: Ano específico para processar (None = todos)
    """
    logger = setup_logger('main', log_file='logs/pipeline_main.log')

    logger.info("=" * 100)
    logger.info("INICIANDO PIPELINE COMPLETO ECD")
    logger.info("=" * 100)
    logger.info(f"Modo: {modo}")
    logger.info(f"Usar ML: {usar_ml}")
    logger.info(f"Ano filtro: {ano_filtro or 'Todos'}")

    start_time = datetime.now()

    try:
        # ======================================================================
        # ETAPA 1: CARREGAR DADOS
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 1: CARREGANDO DADOS")
        logger.info("=" * 100)

        # Carregar plano de contas
        df_contas = spark.table(TABELAS['ecd_contas'])
        logger.info(f"Plano de contas: {df_contas.count():,} registros")

        # Carregar saldos
        df_saldos = spark.table(TABELAS['ecd_i150'])

        if ano_filtro:
            df_saldos = df_saldos.filter(F.col('ano') == ano_filtro)

        logger.info(f"Saldos: {df_saldos.count():,} registros")

        # ======================================================================
        # ETAPA 2: CLASSIFICAÇÃO DE CONTAS
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 2: CLASSIFICAÇÃO DE CONTAS")
        logger.info("=" * 100)

        pipeline_class = PipelineClassificacao(
            spark=spark,
            config={'ml_model_path': 'models/classificador_ml'},
            logger=logger
        )

        df_contas_classificadas = pipeline_class.classificar(
            df_contas=df_contas,
            usar_ml=usar_ml,
            confidence_threshold=ML_CONFIG['confidence_threshold']
        )

        # Salvar contas classificadas
        save_table(
            df_contas_classificadas,
            TABELAS['ecd_classificadas'],
            mode='overwrite',
            logger=logger
        )

        # ======================================================================
        # ETAPA 3: JUNTAR SALDOS COM CLASSIFICAÇÃO
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 3: JUNTANDO SALDOS COM CLASSIFICAÇÃO")
        logger.info("=" * 100)

        df_saldos_classificados = df_saldos.join(
            df_contas_classificadas.select(
                'cd_cta',
                'classificacao_nivel1',
                'classificacao_nivel2',
                'classificacao_nivel3',
                'nome_nivel1',
                'nome_nivel2',
                'nome_nivel3',
                'metodo_classificacao',
                'confianca_classificacao'
            ),
            on='cd_cta',
            how='left'
        )

        logger.info(f"Saldos classificados: {df_saldos_classificados.count():,} registros")

        # ======================================================================
        # ETAPA 4: VALIDAÇÃO CRUZADA
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 4: VALIDAÇÃO CRUZADA")
        logger.info("=" * 100)

        validador_cruzado = ValidadorCruzado(logger=logger)
        resultado_validacao = validador_cruzado.validar(df_contas_classificadas)

        if resultado_validacao['contas_inconsistentes'] > 0:
            logger.warning(
                f"Encontradas {resultado_validacao['contas_inconsistentes']:,} "
                "inconsistências na classificação"
            )

        # ======================================================================
        # ETAPA 5: GERAR BALANÇO PATRIMONIAL
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 5: GERANDO BALANÇO PATRIMONIAL")
        logger.info("=" * 100)

        gerador_bp = GeradorBalancoPatrimonial(spark=spark, logger=logger)
        df_bp = gerador_bp.gerar(df_saldos_classificados)

        save_table(
            df_bp,
            TABELAS['ecd_bp'],
            mode='overwrite',
            partition_by=['ano'],
            logger=logger
        )

        # ======================================================================
        # ETAPA 6: GERAR DRE
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 6: GERANDO DRE")
        logger.info("=" * 100)

        gerador_dre = GeradorDRE(spark=spark, logger=logger)
        df_dre = gerador_dre.gerar(df_saldos_classificados)

        save_table(
            df_dre,
            TABELAS['ecd_dre'],
            mode='overwrite',
            partition_by=['ano'],
            logger=logger
        )

        # ======================================================================
        # ETAPA 7: VALIDAR EQUAÇÃO CONTÁBIL
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 7: VALIDANDO EQUAÇÃO CONTÁBIL")
        logger.info("=" * 100)

        validador_eq = ValidadorEquacaoContabil(logger=logger)
        resultado_eq = validador_eq.validar(df_saldos_classificados)

        logger.info(
            f"Empresas com equação válida: {resultado_eq['empresas_validas']:,} / "
            f"{resultado_eq['total_empresas']:,} ({resultado_eq['taxa_validacao']:.1f}%)"
        )

        # ======================================================================
        # ETAPA 8: CALCULAR INDICADORES FINANCEIROS
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 8: CALCULANDO INDICADORES FINANCEIROS")
        logger.info("=" * 100)

        # Preparar BP para cálculo de indicadores (pivotar)
        df_bp_pivot = df_bp.groupBy('cnpj', 'ano') \
                           .pivot('codigo_conta') \
                           .sum('saldo')

        # Renomear colunas
        colunas_bp = {
            '1': 'ativo_total',
            '1.01': 'ativo_circulante',
            '1.02': 'ativo_nao_circulante',
            '2': 'passivo_total',
            '2.01': 'passivo_circulante',
            '2.02': 'passivo_nao_circulante',
            '3': 'patrimonio_liquido_total'
        }

        for cod, nome in colunas_bp.items():
            if cod in df_bp_pivot.columns:
                df_bp_pivot = df_bp_pivot.withColumnRenamed(cod, nome)

        # Calcular indicadores financeiros
        calc_financeiros = IndicadoresFinanceiros(logger=logger)
        df_indicadores_fin = calc_financeiros.calcular_todos(df_bp_pivot)
        df_indicadores_fin = calc_financeiros.classificar_situacao_financeira(df_indicadores_fin)

        # ======================================================================
        # ETAPA 9: CALCULAR INDICADORES DE RENTABILIDADE
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("ETAPA 9: CALCULANDO INDICADORES DE RENTABILIDADE")
        logger.info("=" * 100)

        calc_rentabilidade = IndicadoresRentabilidade(logger=logger)
        df_indicadores = calc_rentabilidade.calcular_todos(df_dre, df_bp_pivot)
        df_indicadores = calc_rentabilidade.calcular_dupont(df_indicadores)
        df_indicadores = calc_rentabilidade.classificar_rentabilidade(df_indicadores)

        # Juntar indicadores financeiros
        df_indicadores_completos = df_indicadores.join(
            df_indicadores_fin,
            on=['cnpj', 'ano'],
            how='left'
        )

        save_table(
            df_indicadores_completos,
            TABELAS['ecd_indicadores'],
            mode='overwrite',
            partition_by=['ano'],
            logger=logger
        )

        # ======================================================================
        # ETAPA 10: RELATÓRIO FINAL
        # ======================================================================
        logger.info("\n" + "=" * 100)
        logger.info("RELATÓRIO FINAL")
        logger.info("=" * 100)

        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info(f"Tempo total de execução: {elapsed:.2f} segundos ({elapsed/60:.2f} minutos)")
        logger.info(f"Contas classificadas: {df_contas_classificadas.count():,}")
        logger.info(f"Empresas com BP: {df_bp.select('cnpj').distinct().count():,}")
        logger.info(f"Empresas com DRE: {df_dre.select('cnpj').distinct().count():,}")
        logger.info(f"Empresas com indicadores: {df_indicadores_completos.count():,}")

        logger.info("\n" + "=" * 100)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 100)

        return {
            'status': 'sucesso',
            'tempo_execucao': elapsed,
            'contas_classificadas': df_contas_classificadas.count(),
            'empresas_bp': df_bp.select('cnpj').distinct().count(),
            'empresas_dre': df_dre.select('cnpj').distinct().count()
        }

    except Exception as e:
        logger.error(f"ERRO NO PIPELINE: {str(e)}", exc_info=True)
        return {
            'status': 'erro',
            'mensagem': str(e)
        }


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Pipeline ECD - Classificação e Demonstrações')
    parser.add_argument('--modo', choices=['producao', 'teste'], default='producao',
                        help='Modo de execução')
    parser.add_argument('--no-ml', action='store_true',
                        help='Desabilitar classificação por ML')
    parser.add_argument('--ano', type=int,
                        help='Ano específico para processar')

    args = parser.parse_args()

    # Criar sessão Spark
    spark = get_spark_session(
        app_name=f"ECD Pipeline - {args.modo}",
        config={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "200"
        }
    )

    # Executar pipeline
    resultado = executar_pipeline_completo(
        spark=spark,
        modo=args.modo,
        usar_ml=not args.no_ml,
        ano_filtro=args.ano
    )

    # Finalizar
    spark.stop()

    if resultado['status'] == 'sucesso':
        print("\n✓ Pipeline executado com sucesso!")
        exit(0)
    else:
        print(f"\n✗ Pipeline falhou: {resultado.get('mensagem')}")
        exit(1)


if __name__ == '__main__':
    main()
