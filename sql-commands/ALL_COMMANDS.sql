-- ============================================================================
-- DATA SCHEMAS - PROJETO ECD
-- Comandos SQL para coleta de metadados e samples
-- Gerado em: 2025-11-17 22:09:50
-- ============================================================================


-- =======================
-- CATEGORIA: ORIGINAIS_RI
-- =======================


-- [ORIGINAIS_RI] usr_sat_ecd.ecd_ri050_plano_contas
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED usr_sat_ecd.ecd_ri050_plano_contas;

-- SAMPLE (10 linhas)
SELECT * FROM usr_sat_ecd.ecd_ri050_plano_contas LIMIT 10;


-- [ORIGINAIS_RI] usr_sat_ecd.ecd_ri051_plano_contas_referencial
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED usr_sat_ecd.ecd_ri051_plano_contas_referencial;

-- SAMPLE (10 linhas)
SELECT * FROM usr_sat_ecd.ecd_ri051_plano_contas_referencial LIMIT 10;


-- [ORIGINAIS_RI] usr_sat_ecd.ecd_ri150_saldos_periodicos_identificacao_periodo
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED usr_sat_ecd.ecd_ri150_saldos_periodicos_identificacao_periodo;

-- SAMPLE (10 linhas)
SELECT * FROM usr_sat_ecd.ecd_ri150_saldos_periodicos_identificacao_periodo LIMIT 10;


-- [ORIGINAIS_RI] usr_sat_ecd.ecd_ri155_detalhe_saldos_periodicos
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED usr_sat_ecd.ecd_ri155_detalhe_saldos_periodicos;

-- SAMPLE (10 linhas)
SELECT * FROM usr_sat_ecd.ecd_ri155_detalhe_saldos_periodicos LIMIT 10;


-- =======================
-- CATEGORIA: ORIGINAIS_RJ
-- =======================


-- [ORIGINAIS_RJ] usr_sat_ecd.ecd_rj100_balanco_patrimonial
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED usr_sat_ecd.ecd_rj100_balanco_patrimonial;

-- SAMPLE (10 linhas)
SELECT * FROM usr_sat_ecd.ecd_rj100_balanco_patrimonial LIMIT 10;


-- [ORIGINAIS_RJ] usr_sat_ecd.ecd_rj150_demonstracao_resultado_exercicio
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED usr_sat_ecd.ecd_rj150_demonstracao_resultado_exercicio;

-- SAMPLE (10 linhas)
SELECT * FROM usr_sat_ecd.ecd_rj150_demonstracao_resultado_exercicio LIMIT 10;


-- ================================
-- CATEGORIA: ORIGINAIS_PROCESSADAS
-- ================================


-- [ORIGINAIS_PROCESSADAS] teste.ecd_i150
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_i150;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_i150 LIMIT 10;


-- [ORIGINAIS_PROCESSADAS] teste.ecd_i200
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_i200;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_i200 LIMIT 10;


-- ===================
-- CATEGORIA: PRODUCAO
-- ===================


-- [PRODUCAO] teste.ecd_contas_classificadas_producao
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_contas_classificadas_producao;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_contas_classificadas_producao LIMIT 10;


-- [PRODUCAO] teste.ecd_balanco_patrimonial
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_balanco_patrimonial;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_balanco_patrimonial LIMIT 10;


-- [PRODUCAO] teste.ecd_dre
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_dre;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_dre LIMIT 10;


-- [PRODUCAO] teste.ecd_indicadores_financeiros
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_indicadores_financeiros;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_indicadores_financeiros LIMIT 10;


-- ====================
-- CATEGORIA: STREAMLIT
-- ====================


-- [STREAMLIT] teste.ecd_empresas_cadastro
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_empresas_cadastro;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_empresas_cadastro LIMIT 10;


-- [STREAMLIT] teste.ecd_score_risco_consolidado
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_score_risco_consolidado;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_score_risco_consolidado LIMIT 10;


-- [STREAMLIT] teste.ecd_saldos_contas_v2
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_saldos_contas_v2;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_saldos_contas_v2 LIMIT 10;


-- [STREAMLIT] teste.ecd_plano_contas
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_plano_contas;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_plano_contas LIMIT 10;


-- =====================
-- CATEGORIA: ML_DATASET
-- =====================


-- [ML_DATASET] teste.ecd_ml_dataset
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_dataset;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_dataset LIMIT 10;


-- [ML_DATASET] teste.ecd_ml_train
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_train;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_train LIMIT 10;


-- [ML_DATASET] teste.ecd_ml_val
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_val;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_val LIMIT 10;


-- [ML_DATASET] teste.ecd_ml_test
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_test;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_test LIMIT 10;


-- =======================
-- CATEGORIA: ML_PREDICOES
-- =======================


-- [ML_PREDICOES] teste.ecd_ml_predictions_ALL
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_predictions_ALL;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_predictions_ALL LIMIT 10;


-- [ML_PREDICOES] teste.ecd_ml_predictions_rf_val
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_predictions_rf_val;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_predictions_rf_val LIMIT 10;


-- [ML_PREDICOES] teste.ecd_ml_predictions_lr_val
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_predictions_lr_val;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_predictions_lr_val LIMIT 10;


-- [ML_PREDICOES] teste.ecd_ml_predicoes
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_predicoes;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_predicoes LIMIT 10;


-- ======================
-- CATEGORIA: ML_METRICAS
-- ======================


-- [ML_METRICAS] teste.ecd_ml_metricas
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_metricas;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_metricas LIMIT 10;


-- [ML_METRICAS] teste.ecd_ml_performance_por_classe
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_performance_por_classe;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_performance_por_classe LIMIT 10;


-- [ML_METRICAS] teste.ecd_ml_erros_rf
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_erros_rf;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_erros_rf LIMIT 10;


-- [ML_METRICAS] teste.ml_label_mapping
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ml_label_mapping;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ml_label_mapping LIMIT 10;


-- ======================
-- CATEGORIA: ML_EMPRESAS
-- ======================


-- [ML_EMPRESAS] teste.ecd_ml_stats_classificacao_empresa
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_stats_classificacao_empresa;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_stats_classificacao_empresa LIMIT 10;


-- [ML_EMPRESAS] teste.ecd_ml_valores_balanco_empresa
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_valores_balanco_empresa;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_valores_balanco_empresa LIMIT 10;


-- [ML_EMPRESAS] teste.ecd_ml_valores_dre_empresa
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_valores_dre_empresa;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_valores_dre_empresa LIMIT 10;


-- [ML_EMPRESAS] teste.ecd_ml_empresas_consolidado
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_empresas_consolidado;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_empresas_consolidado LIMIT 10;


-- =====================
-- CATEGORIA: ML_ANALISE
-- =====================


-- [ML_ANALISE] teste.ecd_ml_empresas_aptas_indicadores
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_empresas_aptas_indicadores;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_empresas_aptas_indicadores LIMIT 10;


-- [ML_ANALISE] teste.ecd_ml_empresas_indices_padrao
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_empresas_indices_padrao;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_empresas_indices_padrao LIMIT 10;


-- [ML_ANALISE] teste.ecd_ml_indices_padrao_decis
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_indices_padrao_decis;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_indices_padrao_decis LIMIT 10;


-- [ML_ANALISE] teste.ecd_ml_candidatas_ajuste_manual
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_candidatas_ajuste_manual;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_candidatas_ajuste_manual LIMIT 10;


-- [ML_ANALISE] teste.ecd_ml_sinteticas_por_heranca
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_sinteticas_por_heranca;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_sinteticas_por_heranca LIMIT 10;


-- [ML_ANALISE] teste.ecd_ml_fallback_classificacoes
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_ml_fallback_classificacoes;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_ml_fallback_classificacoes LIMIT 10;


-- ======================
-- CATEGORIA: INDICADORES
-- ======================


-- [INDICADORES] teste.ecd_indicadores_hibrido
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_indicadores_hibrido;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_indicadores_hibrido LIMIT 10;


-- [INDICADORES] teste.ecd_indices_padrao_setoriais
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_indices_padrao_setoriais;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_indices_padrao_setoriais LIMIT 10;


-- [INDICADORES] teste.ecd_empresas_classificacao_resumo
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_empresas_classificacao_resumo;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_empresas_classificacao_resumo LIMIT 10;


-- [INDICADORES] teste.ecd_evolucao_scores
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_evolucao_scores;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_evolucao_scores LIMIT 10;


-- [INDICADORES] teste.ecd_empresas_criticas
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_empresas_criticas;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_empresas_criticas LIMIT 10;


-- ====================
-- CATEGORIA: VALIDACAO
-- ====================


-- [VALIDACAO] teste.pc_referencia_completa
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.pc_referencia_completa;

-- SAMPLE (10 linhas)
SELECT * FROM teste.pc_referencia_completa LIMIT 10;


-- [VALIDACAO] teste.ecd_contas_classificadas_final
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_contas_classificadas_final;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_contas_classificadas_final LIMIT 10;


-- [VALIDACAO] teste.ecd_resumo_executivo
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_resumo_executivo;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_resumo_executivo LIMIT 10;


-- [VALIDACAO] teste.ecd_detalhamento_metodo
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_detalhamento_metodo;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_detalhamento_metodo LIMIT 10;


-- [VALIDACAO] teste.ecd_top_classificacoes
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_top_classificacoes;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_top_classificacoes LIMIT 10;


-- [VALIDACAO] teste.ecd_empresas_equacao_ok
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_empresas_equacao_ok;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_empresas_equacao_ok LIMIT 10;


-- [VALIDACAO] teste.ecd_amostra_ml
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_amostra_ml;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_amostra_ml LIMIT 10;


-- [VALIDACAO] teste.ecd_contas_nao_classificadas
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_contas_nao_classificadas;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_contas_nao_classificadas LIMIT 10;


-- [VALIDACAO] teste.ecd_stats_por_empresa
-- ----------------------------------------------------------------------------

-- DESCRIBE FORMATTED
DESCRIBE FORMATTED teste.ecd_stats_por_empresa;

-- SAMPLE (10 linhas)
SELECT * FROM teste.ecd_stats_por_empresa LIMIT 10;

