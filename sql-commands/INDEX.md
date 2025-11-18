# Comandos SQL - Data Schemas

Gerado em: 2025-11-17 22:09:50

---

## Estatísticas

- **Total de categorias**: 12
- **Total de tabelas**: 52
- **Comandos por tabela**: 2 (DESCRIBE + SELECT)
- **Total de comandos**: 104

---

## Arquivos Gerados

### Arquivo Consolidado

- **`ALL_COMMANDS.sql`** - Todos os comandos em um único arquivo

### Arquivos por Categoria

- **`ORIGINAIS_RI.sql`** - 4 tabelas
- **`ORIGINAIS_RJ.sql`** - 2 tabelas
- **`ORIGINAIS_PROCESSADAS.sql`** - 2 tabelas
- **`PRODUCAO.sql`** - 4 tabelas
- **`STREAMLIT.sql`** - 4 tabelas
- **`ML_DATASET.sql`** - 4 tabelas
- **`ML_PREDICOES.sql`** - 4 tabelas
- **`ML_METRICAS.sql`** - 4 tabelas
- **`ML_EMPRESAS.sql`** - 4 tabelas
- **`ML_ANALISE.sql`** - 6 tabelas
- **`INDICADORES.sql`** - 5 tabelas
- **`VALIDACAO.sql`** - 9 tabelas

---

## Detalhamento por Categoria

### ORIGINAIS_RI

Total: 4 tabelas

1. `usr_sat_ecd.ecd_ri050_plano_contas`
2. `usr_sat_ecd.ecd_ri051_plano_contas_referencial`
3. `usr_sat_ecd.ecd_ri150_saldos_periodicos_identificacao_periodo`
4. `usr_sat_ecd.ecd_ri155_detalhe_saldos_periodicos`

### ORIGINAIS_RJ

Total: 2 tabelas

1. `usr_sat_ecd.ecd_rj100_balanco_patrimonial`
2. `usr_sat_ecd.ecd_rj150_demonstracao_resultado_exercicio`

### ORIGINAIS_PROCESSADAS

Total: 2 tabelas

1. `teste.ecd_i150`
2. `teste.ecd_i200`

### PRODUCAO

Total: 4 tabelas

1. `teste.ecd_contas_classificadas_producao`
2. `teste.ecd_balanco_patrimonial`
3. `teste.ecd_dre`
4. `teste.ecd_indicadores_financeiros`

### STREAMLIT

Total: 4 tabelas

1. `teste.ecd_empresas_cadastro`
2. `teste.ecd_score_risco_consolidado`
3. `teste.ecd_saldos_contas_v2`
4. `teste.ecd_plano_contas`

### ML_DATASET

Total: 4 tabelas

1. `teste.ecd_ml_dataset`
2. `teste.ecd_ml_train`
3. `teste.ecd_ml_val`
4. `teste.ecd_ml_test`

### ML_PREDICOES

Total: 4 tabelas

1. `teste.ecd_ml_predictions_ALL`
2. `teste.ecd_ml_predictions_rf_val`
3. `teste.ecd_ml_predictions_lr_val`
4. `teste.ecd_ml_predicoes`

### ML_METRICAS

Total: 4 tabelas

1. `teste.ecd_ml_metricas`
2. `teste.ecd_ml_performance_por_classe`
3. `teste.ecd_ml_erros_rf`
4. `teste.ml_label_mapping`

### ML_EMPRESAS

Total: 4 tabelas

1. `teste.ecd_ml_stats_classificacao_empresa`
2. `teste.ecd_ml_valores_balanco_empresa`
3. `teste.ecd_ml_valores_dre_empresa`
4. `teste.ecd_ml_empresas_consolidado`

### ML_ANALISE

Total: 6 tabelas

1. `teste.ecd_ml_empresas_aptas_indicadores`
2. `teste.ecd_ml_empresas_indices_padrao`
3. `teste.ecd_ml_indices_padrao_decis`
4. `teste.ecd_ml_candidatas_ajuste_manual`
5. `teste.ecd_ml_sinteticas_por_heranca`
6. `teste.ecd_ml_fallback_classificacoes`

### INDICADORES

Total: 5 tabelas

1. `teste.ecd_indicadores_hibrido`
2. `teste.ecd_indices_padrao_setoriais`
3. `teste.ecd_empresas_classificacao_resumo`
4. `teste.ecd_evolucao_scores`
5. `teste.ecd_empresas_criticas`

### VALIDACAO

Total: 9 tabelas

1. `teste.pc_referencia_completa`
2. `teste.ecd_contas_classificadas_final`
3. `teste.ecd_resumo_executivo`
4. `teste.ecd_detalhamento_metodo`
5. `teste.ecd_top_classificacoes`
6. `teste.ecd_empresas_equacao_ok`
7. `teste.ecd_amostra_ml`
8. `teste.ecd_contas_nao_classificadas`
9. `teste.ecd_stats_por_empresa`

---

## Como Usar

### Opção 1: Executar todos os comandos

```bash
# No ambiente Spark/Hive
spark-sql -f ALL_COMMANDS.sql > resultados.txt
```

### Opção 2: Executar por categoria

```bash
# Exemplo: apenas tabelas de produção
spark-sql -f PRODUCAO.sql > producao_resultados.txt
```

### Opção 3: PySpark

```python
# Ler e executar comandos
with open('ALL_COMMANDS.sql', 'r') as f:
    comandos = f.read().split(';')

for cmd in comandos:
    if cmd.strip():
        df = spark.sql(cmd)
        df.show()
```

### Opção 4: Usar o script Python automatizado

```bash
python scripts/generate_data_schemas.py
```

Este script executa todos os comandos e salva os resultados em arquivos separados.

