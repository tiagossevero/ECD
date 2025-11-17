"""
Script para Gerar Data Schemas de Todas as Tabelas do Projeto ECD
==================================================================

Este script coleta automaticamente:
1. DESCRIBE FORMATTED - Metadados completos da tabela
2. SELECT * FROM ... LIMIT 10 - Amostra de dados

Autor: Sistema ECD
Data: 2025-11-17
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Adicionar paths necessários (ajustar conforme seu ambiente)
sys.path.append("/home/tsevero/notebooks/SAT_BIG_DATA/data-pipeline/batch/poc")
sys.path.append("/home/tsevero/notebooks/SAT_BIG_DATA/data-pipeline/batch/plugins")
sys.path.append("/home/tsevero/notebooks/SAT_BIG_DATA/data-pipeline/batch/dags")

from pyspark.sql.functions import *
from utils import spark_utils_session as utils

# ==============================================================================
# CONFIGURAÇÃO DA SESSÃO SPARK
# ==============================================================================

def get_session(profile: str = 'efd_t2', dynamic_allocation_enabled: bool = True):
    """Gera DBASparkAppSession."""

    app_name = "ecd_data_schema_generator"

    spark_builder = (utils.DBASparkAppSession
                     .builder
                     .setAppName(app_name)
                     .usingProcessProfile(profile)
                    )

    if dynamic_allocation_enabled:
        spark_builder.autoResourceManagement()

    return spark_builder.build()

# Inicializar sessão
print("Inicializando sessão Spark...")
session = get_session(profile='efd_t2')
spark = session.sparkSession
print("✓ Sessão Spark inicializada")

# ==============================================================================
# DEFINIÇÃO DAS TABELAS
# ==============================================================================

TABELAS = {
    "ORIGINAIS_RI": [
        "usr_sat_ecd.ecd_ri050_plano_contas",
        "usr_sat_ecd.ecd_ri051_plano_contas_referencial",
        "usr_sat_ecd.ecd_ri150_saldos_periodicos_identificacao_periodo",
        "usr_sat_ecd.ecd_ri155_detalhe_saldos_periodicos",
    ],

    "ORIGINAIS_RJ": [
        "usr_sat_ecd.ecd_rj100_balanco_patrimonial",
        "usr_sat_ecd.ecd_rj150_demonstracao_resultado_exercicio",
    ],

    "ORIGINAIS_PROCESSADAS": [
        "teste.ecd_i150",
        "teste.ecd_i200",
    ],

    "PRODUCAO": [
        "teste.ecd_contas_classificadas_producao",
        "teste.ecd_balanco_patrimonial",
        "teste.ecd_dre",
        "teste.ecd_indicadores_financeiros",
    ],

    "STREAMLIT": [
        "teste.ecd_empresas_cadastro",
        "teste.ecd_score_risco_consolidado",
        "teste.ecd_saldos_contas_v2",
        "teste.ecd_plano_contas",
    ],

    "ML_DATASET": [
        "teste.ecd_ml_dataset",
        "teste.ecd_ml_train",
        "teste.ecd_ml_val",
        "teste.ecd_ml_test",
    ],

    "ML_PREDICOES": [
        "teste.ecd_ml_predictions_ALL",
        "teste.ecd_ml_predictions_rf_val",
        "teste.ecd_ml_predictions_lr_val",
        "teste.ecd_ml_predicoes",
    ],

    "ML_METRICAS": [
        "teste.ecd_ml_metricas",
        "teste.ecd_ml_performance_por_classe",
        "teste.ecd_ml_erros_rf",
        "teste.ml_label_mapping",
    ],

    "ML_EMPRESAS": [
        "teste.ecd_ml_stats_classificacao_empresa",
        "teste.ecd_ml_valores_balanco_empresa",
        "teste.ecd_ml_valores_dre_empresa",
        "teste.ecd_ml_empresas_consolidado",
    ],

    "ML_ANALISE": [
        "teste.ecd_ml_empresas_aptas_indicadores",
        "teste.ecd_ml_empresas_indices_padrao",
        "teste.ecd_ml_indices_padrao_decis",
        "teste.ecd_ml_candidatas_ajuste_manual",
        "teste.ecd_ml_sinteticas_por_heranca",
        "teste.ecd_ml_fallback_classificacoes",
    ],

    "INDICADORES": [
        "teste.ecd_indicadores_hibrido",
        "teste.ecd_indices_padrao_setoriais",
        "teste.ecd_empresas_classificacao_resumo",
        "teste.ecd_evolucao_scores",
        "teste.ecd_empresas_criticas",
    ],

    "VALIDACAO": [
        "teste.pc_referencia_completa",
        "teste.ecd_contas_classificadas_final",
        "teste.ecd_resumo_executivo",
        "teste.ecd_detalhamento_metodo",
        "teste.ecd_top_classificacoes",
        "teste.ecd_empresas_equacao_ok",
        "teste.ecd_amostra_ml",
        "teste.ecd_contas_nao_classificadas",
        "teste.ecd_stats_por_empresa",
    ],
}

# ==============================================================================
# FUNÇÕES DE COLETA
# ==============================================================================

def verificar_tabela_existe(tabela_nome):
    """Verifica se a tabela existe no banco."""
    try:
        spark.sql(f"DESCRIBE {tabela_nome}")
        return True
    except Exception as e:
        return False

def coletar_describe_formatted(tabela_nome):
    """Coleta DESCRIBE FORMATTED de uma tabela."""
    try:
        df = spark.sql(f"DESCRIBE FORMATTED {tabela_nome}")
        return df
    except Exception as e:
        print(f"  ✗ Erro ao coletar DESCRIBE FORMATTED: {str(e)}")
        return None

def coletar_sample_data(tabela_nome):
    """Coleta amostra de dados (10 linhas) de uma tabela."""
    try:
        df = spark.sql(f"SELECT * FROM {tabela_nome} LIMIT 10")
        return df
    except Exception as e:
        print(f"  ✗ Erro ao coletar sample: {str(e)}")
        return None

def salvar_dataframe_como_texto(df, arquivo_path):
    """Salva DataFrame como arquivo de texto."""
    try:
        # Converter para Pandas e salvar
        pdf = df.toPandas()

        with open(arquivo_path, 'w', encoding='utf-8') as f:
            f.write(pdf.to_string(index=False))

        return True
    except Exception as e:
        print(f"  ✗ Erro ao salvar arquivo: {str(e)}")
        return False

def salvar_dataframe_como_csv(df, arquivo_path):
    """Salva DataFrame como CSV."""
    try:
        pdf = df.toPandas()
        pdf.to_csv(arquivo_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        print(f"  ✗ Erro ao salvar CSV: {str(e)}")
        return False

# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================

def processar_tabela(tabela_nome, categoria, output_dir):
    """Processa uma tabela individual."""

    print(f"\n  📊 Processando: {tabela_nome}")

    # Verificar se tabela existe
    if not verificar_tabela_existe(tabela_nome):
        print(f"  ⚠️  Tabela não encontrada: {tabela_nome}")
        return False

    # Criar subdiretório para a categoria
    categoria_dir = output_dir / categoria
    categoria_dir.mkdir(parents=True, exist_ok=True)

    # Nome base do arquivo (remover schema)
    tabela_limpa = tabela_nome.replace('.', '_')

    # 1. DESCRIBE FORMATTED
    print(f"    → Coletando DESCRIBE FORMATTED...")
    df_describe = coletar_describe_formatted(tabela_nome)
    if df_describe:
        arquivo_describe = categoria_dir / f"{tabela_limpa}_DESCRIBE.txt"
        if salvar_dataframe_como_texto(df_describe, arquivo_describe):
            print(f"    ✓ DESCRIBE salvo: {arquivo_describe}")

    # 2. SELECT SAMPLE
    print(f"    → Coletando sample (10 linhas)...")
    df_sample = coletar_sample_data(tabela_nome)
    if df_sample:
        # Salvar como TXT
        arquivo_sample_txt = categoria_dir / f"{tabela_limpa}_SAMPLE.txt"
        if salvar_dataframe_como_texto(df_sample, arquivo_sample_txt):
            print(f"    ✓ Sample TXT salvo: {arquivo_sample_txt}")

        # Salvar como CSV
        arquivo_sample_csv = categoria_dir / f"{tabela_limpa}_SAMPLE.csv"
        if salvar_dataframe_como_csv(df_sample, arquivo_sample_csv):
            print(f"    ✓ Sample CSV salvo: {arquivo_sample_csv}")

    return True

def main():
    """Função principal."""

    print("=" * 80)
    print("  GERADOR DE DATA SCHEMAS - PROJETO ECD")
    print("=" * 80)
    print(f"  Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Criar diretório de saída
    output_dir = Path("/home/user/ECD/data-schemas")
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n📁 Diretório de saída: {output_dir}")

    # Contadores
    total_tabelas = sum(len(tabelas) for tabelas in TABELAS.values())
    processadas = 0
    sucesso = 0
    falhas = 0

    # Processar cada categoria
    for categoria, tabelas in TABELAS.items():
        print(f"\n{'=' * 80}")
        print(f"  📂 CATEGORIA: {categoria}")
        print(f"  Total de tabelas: {len(tabelas)}")
        print(f"{'=' * 80}")

        for tabela in tabelas:
            processadas += 1
            print(f"\n[{processadas}/{total_tabelas}]")

            if processar_tabela(tabela, categoria, output_dir):
                sucesso += 1
            else:
                falhas += 1

    # Relatório final
    print("\n" + "=" * 80)
    print("  RELATÓRIO FINAL")
    print("=" * 80)
    print(f"  Total de tabelas: {total_tabelas}")
    print(f"  ✓ Processadas com sucesso: {sucesso}")
    print(f"  ✗ Falhas: {falhas}")
    print(f"  📁 Arquivos salvos em: {output_dir}")
    print("=" * 80)

    # Criar arquivo de índice
    criar_indice(output_dir)

    print("\n✅ Processamento concluído!")

def criar_indice(output_dir):
    """Cria arquivo de índice com todas as tabelas processadas."""

    arquivo_indice = output_dir / "INDEX.md"

    with open(arquivo_indice, 'w', encoding='utf-8') as f:
        f.write("# Data Schemas - Projeto ECD\n\n")
        f.write(f"Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("---\n\n")

        for categoria, tabelas in TABELAS.items():
            f.write(f"## {categoria}\n\n")

            for tabela in tabelas:
                tabela_limpa = tabela.replace('.', '_')
                f.write(f"### {tabela}\n\n")
                f.write(f"- **DESCRIBE**: `{categoria}/{tabela_limpa}_DESCRIBE.txt`\n")
                f.write(f"- **SAMPLE TXT**: `{categoria}/{tabela_limpa}_SAMPLE.txt`\n")
                f.write(f"- **SAMPLE CSV**: `{categoria}/{tabela_limpa}_SAMPLE.csv`\n\n")

    print(f"\n📄 Índice criado: {arquivo_indice}")

# ==============================================================================
# EXECUTAR
# ==============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Processo interrompido pelo usuário")
    except Exception as e:
        print(f"\n\n❌ Erro fatal: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🔚 Encerrando sessão Spark...")
        spark.stop()
        print("✓ Sessão encerrada")
