# Scripts para Geração de Data Schemas

Este diretório contém scripts para gerar automaticamente os data schemas de todas as tabelas do projeto ECD.

## 📁 Arquivos

### 1. `generate_data_schemas.py` ⭐ RECOMENDADO

**Script principal que executa tudo automaticamente.**

- Conecta ao Spark
- Executa `DESCRIBE FORMATTED` em todas as tabelas
- Executa `SELECT * FROM ... LIMIT 10` em todas as tabelas
- Salva resultados em arquivos organizados por categoria

**Saída:**
```
data-schemas/
├── INDEX.md (índice de todas as tabelas)
├── ORIGINAIS_RI/
│   ├── usr_sat_ecd_ecd_ri050_plano_contas_DESCRIBE.txt
│   ├── usr_sat_ecd_ecd_ri050_plano_contas_SAMPLE.txt
│   ├── usr_sat_ecd_ecd_ri050_plano_contas_SAMPLE.csv
│   └── ...
├── PRODUCAO/
│   ├── teste_ecd_balanco_patrimonial_DESCRIBE.txt
│   ├── teste_ecd_balanco_patrimonial_SAMPLE.txt
│   ├── teste_ecd_balanco_patrimonial_SAMPLE.csv
│   └── ...
└── ... (outras categorias)
```

### 2. `generate_sql_commands.py`

**Gera apenas os comandos SQL, sem executar.**

Útil se você quiser:
- Revisar os comandos antes de executar
- Executar manualmente em outro ambiente
- Customizar quais tabelas processar

**Saída:**
```
sql-commands/
├── INDEX.md (guia de uso)
├── ALL_COMMANDS.sql (todos os comandos)
├── ORIGINAIS_RI.sql
├── PRODUCAO.sql
├── ML_DATASET.sql
└── ... (um arquivo por categoria)
```

## 🚀 Como Usar

### Opção 1: Executar Script Automatizado (Recomendado)

```bash
cd /home/user/ECD
python scripts/generate_data_schemas.py
```

**O que acontece:**
1. ✓ Conecta ao Spark (perfil `efd_t2`)
2. ✓ Processa 52 tabelas em 11 categorias
3. ✓ Gera 156 arquivos (3 por tabela: DESCRIBE, SAMPLE.txt, SAMPLE.csv)
4. ✓ Cria índice navegável
5. ✓ Exibe relatório de progresso

**Tempo estimado:** 10-20 minutos (dependendo do tamanho das tabelas)

### Opção 2: Gerar Apenas os Comandos SQL

```bash
cd /home/user/ECD
python scripts/generate_sql_commands.py
```

Depois execute manualmente:

```bash
# Todos os comandos
spark-sql -f sql-commands/ALL_COMMANDS.sql > resultados.txt

# Ou por categoria
spark-sql -f sql-commands/PRODUCAO.sql > producao.txt
```

### Opção 3: Executar em Notebook Jupyter

Copie o código para um notebook:

```python
# Carregar o script
exec(open('/home/user/ECD/scripts/generate_data_schemas.py').read())
```

### Opção 4: Executar Apenas Categorias Específicas

Edite `generate_data_schemas.py` e comente as categorias que não quer processar:

```python
TABELAS = {
    "PRODUCAO": [  # ← Apenas esta categoria
        "teste.ecd_contas_classificadas_producao",
        "teste.ecd_balanco_patrimonial",
        "teste.ecd_dre",
        "teste.ecd_indicadores_financeiros",
    ],

    # "ML_DATASET": [  # ← Comentadas
    #     ...
    # ],
}
```

## 📊 Tabelas Processadas

### Resumo por Categoria

| Categoria | Tabelas | Descrição |
|-----------|---------|-----------|
| **ORIGINAIS_RI** | 4 | Tabelas de Registro de Informações (RI) |
| **ORIGINAIS_RJ** | 2 | Tabelas de Demonstrações (RJ) |
| **ORIGINAIS_PROCESSADAS** | 2 | Tabelas I (saldos consolidados) |
| **PRODUCAO** | 4 | Pipeline principal (BP, DRE, Indicadores) |
| **STREAMLIT** | 4 | Tabelas usadas pela aplicação web |
| **ML_DATASET** | 4 | Datasets para Machine Learning |
| **ML_PREDICOES** | 4 | Predições dos modelos ML |
| **ML_METRICAS** | 4 | Métricas de performance ML |
| **ML_EMPRESAS** | 4 | Análise por empresa (ML) |
| **ML_ANALISE** | 6 | Análises avançadas ML |
| **INDICADORES** | 5 | Indicadores financeiros e análises |
| **VALIDACAO** | 9 | Validação e controle de qualidade |
| **TOTAL** | **52** | |

### Tabelas Prioritárias (Essenciais)

Se você tem tempo limitado, comece por estas:

**FASE 1 - Dados Originais (8 tabelas):**
```
usr_sat_ecd.ecd_ri050_plano_contas
usr_sat_ecd.ecd_ri155_detalhe_saldos_periodicos
teste.ecd_i150  ← PRINCIPAL
teste.ecd_i200
```

**FASE 2 - Produção (4 tabelas):**
```
teste.ecd_contas_classificadas_producao
teste.ecd_balanco_patrimonial  ← Particionado por ano
teste.ecd_dre  ← Particionado por ano
teste.ecd_indicadores_financeiros  ← Particionado por ano
```

**FASE 3 - Aplicação (4 tabelas):**
```
teste.ecd_empresas_cadastro
teste.ecd_plano_contas
```

## 🔧 Configuração

### Requisitos

- PySpark configurado
- Acesso ao banco de dados (schemas `usr_sat_ecd` e `teste`)
- Perfil Spark: `efd_t2`
- Bibliotecas: pandas, numpy

### Ajustar Ambiente

Se seus paths forem diferentes, edite no início do script:

```python
# Ajustar estes paths
sys.path.append("/home/tsevero/notebooks/SAT_BIG_DATA/data-pipeline/batch/poc")
sys.path.append("/home/tsevero/notebooks/SAT_BIG_DATA/data-pipeline/batch/plugins")
sys.path.append("/home/tsevero/notebooks/SAT_BIG_DATA/data-pipeline/batch/dags")
```

### Ajustar Perfil Spark

Se usar outro perfil:

```python
# Trocar 'efd_t2' pelo seu perfil
session = get_session(profile='seu_perfil_aqui')
```

## 📝 Formato dos Arquivos Gerados

### DESCRIBE FORMATTED

```
col_name              data_type            comment
----------------------------------------------------
cnpj_empresa          string               NULL
ano                   int                  NULL
trimestre             int                  NULL
...

# Partition Information
# col_name            data_type            comment
ano                   int                  NULL
```

### SAMPLE (TXT)

```
  cnpj_empresa   ano  trimestre  ...
0  12345678000190  2023  4       ...
1  98765432000100  2023  4       ...
...
```

### SAMPLE (CSV)

```csv
cnpj_empresa,ano,trimestre,...
12345678000190,2023,4,...
98765432000100,2023,4,...
```

## ⚠️ Avisos Importantes

### Tabelas Particionadas

Algumas tabelas são particionadas por `ano`:
- `teste.ecd_balanco_patrimonial`
- `teste.ecd_dre`
- `teste.ecd_indicadores_financeiros`

O DESCRIBE FORMATTED mostrará as informações de particionamento.

### Tabelas que Podem Não Existir

Algumas tabelas podem não existir se os notebooks correspondentes não foram executados:
- Tabelas ML (se modelo não foi treinado)
- Tabelas de validação (se análise não foi feita)

**O script pula automaticamente tabelas inexistentes.**

### Tabelas Grandes

Algumas tabelas podem ter milhões de registros:
- `usr_sat_ecd.ecd_ri155_detalhe_saldos_periodicos`
- `teste.ecd_i150`
- `teste.ecd_ml_dataset`

**O LIMIT 10 garante que apenas 10 linhas sejam retornadas.**

## 🐛 Troubleshooting

### Erro: "Table not found"

**Causa:** Tabela não existe ainda

**Solução:** Normal para tabelas ML/Validação. Execute os notebooks correspondentes primeiro.

### Erro: "Permission denied"

**Causa:** Sem acesso ao schema

**Solução:** Verifique permissões no Spark/Hive

### Erro: "Java heap space"

**Causa:** Tabela muito grande

**Solução:** O script usa LIMIT 10, mas se ainda der erro:
```python
# Aumentar memória do Spark
spark_builder.config("spark.driver.memory", "8g")
```

### Script muito lento

**Causa:** Muitas tabelas ou tabelas grandes

**Solução:** Processe por categoria:
1. Comente categorias desnecessárias
2. Execute em paralelo (várias instâncias do script)

## 📚 Referências

- **Notebook exemplo:** `/home/user/ECD/notebooks/ECD-Exemplo.ipynb`
- **Pipeline produção:** `/home/user/ECD/main.py`
- **README principal:** `/home/user/ECD/README.md`

## 🤝 Contribuindo

Para adicionar novas tabelas ao script:

1. Edite `TABELAS` em `generate_data_schemas.py`
2. Adicione na categoria apropriada
3. Execute o script
4. Commit os novos data schemas

## 📞 Suporte

Em caso de dúvidas:
1. Verifique logs do script
2. Teste comando SQL manualmente
3. Consulte documentação do PySpark

---

**Última atualização:** 2025-11-17
