"""
Script para Gerar Comandos SQL para Data Schemas
=================================================

Este script gera arquivos .sql com todos os comandos necessários
para coletar metadados e samples de todas as tabelas.

Útil se você quiser executar os comandos manualmente ou em outro ambiente.

Autor: Sistema ECD
Data: 2025-11-17
"""

from pathlib import Path
from datetime import datetime

# ==============================================================================
# DEFINIÇÃO DAS TABELAS (mesma estrutura do script principal)
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
# GERADOR DE COMANDOS SQL
# ==============================================================================

def gerar_comandos_sql():
    """Gera arquivos SQL com comandos para cada categoria."""

    print("=" * 80)
    print("  GERADOR DE COMANDOS SQL - DATA SCHEMAS")
    print("=" * 80)
    print(f"  Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Criar diretório de saída
    output_dir = Path("/home/user/ECD/sql-commands")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Arquivo consolidado
    arquivo_consolidado = output_dir / "ALL_COMMANDS.sql"

    with open(arquivo_consolidado, 'w', encoding='utf-8') as f_all:
        # Cabeçalho
        f_all.write("-- " + "=" * 76 + "\n")
        f_all.write("-- DATA SCHEMAS - PROJETO ECD\n")
        f_all.write("-- Comandos SQL para coleta de metadados e samples\n")
        f_all.write(f"-- Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f_all.write("-- " + "=" * 76 + "\n\n")

        # Processar cada categoria
        for categoria, tabelas in TABELAS.items():
            print(f"\n📂 Processando categoria: {categoria}")

            # Arquivo por categoria
            arquivo_categoria = output_dir / f"{categoria}.sql"

            with open(arquivo_categoria, 'w', encoding='utf-8') as f_cat:
                # Cabeçalho da categoria
                header = f"-- CATEGORIA: {categoria}"
                separador = "-- " + "=" * (len(header) - 3)

                f_cat.write(f"{separador}\n")
                f_cat.write(f"{header}\n")
                f_cat.write(f"{separador}\n\n")

                f_all.write(f"\n{separador}\n")
                f_all.write(f"{header}\n")
                f_all.write(f"{separador}\n\n")

                # Comandos para cada tabela
                for i, tabela in enumerate(tabelas, 1):
                    tabela_limpa = tabela.replace('.', '_')

                    # Separador de tabela
                    f_cat.write(f"\n-- [{i}/{len(tabelas)}] {tabela}\n")
                    f_cat.write(f"-- {'-' * 76}\n\n")

                    f_all.write(f"\n-- [{categoria}] {tabela}\n")
                    f_all.write(f"-- {'-' * 76}\n\n")

                    # DESCRIBE FORMATTED
                    cmd_describe = f"DESCRIBE FORMATTED {tabela};\n"
                    f_cat.write(f"-- DESCRIBE FORMATTED\n")
                    f_cat.write(cmd_describe)
                    f_cat.write("\n")

                    f_all.write(f"-- DESCRIBE FORMATTED\n")
                    f_all.write(cmd_describe)
                    f_all.write("\n")

                    # SELECT SAMPLE
                    cmd_select = f"SELECT * FROM {tabela} LIMIT 10;\n"
                    f_cat.write(f"-- SAMPLE (10 linhas)\n")
                    f_cat.write(cmd_select)
                    f_cat.write("\n")

                    f_all.write(f"-- SAMPLE (10 linhas)\n")
                    f_all.write(cmd_select)
                    f_all.write("\n")

            print(f"  ✓ Arquivo criado: {arquivo_categoria}")

    print(f"\n✓ Arquivo consolidado: {arquivo_consolidado}")

    # Criar índice
    criar_indice_sql(output_dir)

    print("\n" + "=" * 80)
    print("  ✅ Comandos SQL gerados com sucesso!")
    print("=" * 80)
    print(f"\n📁 Diretório: {output_dir}")
    print(f"\n📄 Arquivos gerados:")
    print(f"  - ALL_COMMANDS.sql (todos os comandos)")
    print(f"  - INDEX.md (índice de referência)")
    for categoria in TABELAS.keys():
        print(f"  - {categoria}.sql")
    print("\n")

def criar_indice_sql(output_dir):
    """Cria índice de referência dos comandos SQL."""

    arquivo_indice = output_dir / "INDEX.md"

    with open(arquivo_indice, 'w', encoding='utf-8') as f:
        f.write("# Comandos SQL - Data Schemas\n\n")
        f.write(f"Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("---\n\n")

        # Estatísticas
        total_tabelas = sum(len(tabelas) for tabelas in TABELAS.values())
        f.write(f"## Estatísticas\n\n")
        f.write(f"- **Total de categorias**: {len(TABELAS)}\n")
        f.write(f"- **Total de tabelas**: {total_tabelas}\n")
        f.write(f"- **Comandos por tabela**: 2 (DESCRIBE + SELECT)\n")
        f.write(f"- **Total de comandos**: {total_tabelas * 2}\n\n")
        f.write("---\n\n")

        # Arquivos
        f.write("## Arquivos Gerados\n\n")
        f.write("### Arquivo Consolidado\n\n")
        f.write("- **`ALL_COMMANDS.sql`** - Todos os comandos em um único arquivo\n\n")
        f.write("### Arquivos por Categoria\n\n")

        for categoria, tabelas in TABELAS.items():
            f.write(f"- **`{categoria}.sql`** - {len(tabelas)} tabelas\n")

        f.write("\n---\n\n")

        # Detalhamento por categoria
        f.write("## Detalhamento por Categoria\n\n")

        for categoria, tabelas in TABELAS.items():
            f.write(f"### {categoria}\n\n")
            f.write(f"Total: {len(tabelas)} tabelas\n\n")

            for i, tabela in enumerate(tabelas, 1):
                f.write(f"{i}. `{tabela}`\n")

            f.write("\n")

        f.write("---\n\n")

        # Instruções de uso
        f.write("## Como Usar\n\n")
        f.write("### Opção 1: Executar todos os comandos\n\n")
        f.write("```bash\n")
        f.write("# No ambiente Spark/Hive\n")
        f.write("spark-sql -f ALL_COMMANDS.sql > resultados.txt\n")
        f.write("```\n\n")

        f.write("### Opção 2: Executar por categoria\n\n")
        f.write("```bash\n")
        f.write("# Exemplo: apenas tabelas de produção\n")
        f.write("spark-sql -f PRODUCAO.sql > producao_resultados.txt\n")
        f.write("```\n\n")

        f.write("### Opção 3: PySpark\n\n")
        f.write("```python\n")
        f.write("# Ler e executar comandos\n")
        f.write("with open('ALL_COMMANDS.sql', 'r') as f:\n")
        f.write("    comandos = f.read().split(';')\n\n")
        f.write("for cmd in comandos:\n")
        f.write("    if cmd.strip():\n")
        f.write("        df = spark.sql(cmd)\n")
        f.write("        df.show()\n")
        f.write("```\n\n")

        f.write("### Opção 4: Usar o script Python automatizado\n\n")
        f.write("```bash\n")
        f.write("python scripts/generate_data_schemas.py\n")
        f.write("```\n\n")
        f.write("Este script executa todos os comandos e salva os resultados em arquivos separados.\n\n")

    print(f"  ✓ Índice criado: {arquivo_indice}")

# ==============================================================================
# EXECUTAR
# ==============================================================================

if __name__ == "__main__":
    try:
        gerar_comandos_sql()
    except KeyboardInterrupt:
        print("\n\n⚠️  Processo interrompido pelo usuário")
    except Exception as e:
        print(f"\n\n❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
