# Sistema ECD - Documentação Técnica

## Visão Geral

Sistema de produção para classificação automática de contas contábeis e geração de demonstrações financeiras (BP e DRE) com indicadores.

## Arquitetura

```
ECD/
├── src/                          # Código-fonte modular
│   ├── classificacao/            # Pipeline de classificação
│   │   ├── regras.py            # Classificador por regras hierárquicas
│   │   ├── ml_classifier.py     # Classificador ML (TF-IDF + Random Forest)
│   │   └── pipeline.py          # Pipeline híbrido (Regras + ML)
│   ├── demonstracoes/           # Geradores de demonstrações
│   │   ├── estrutura_contas.py  # Estrutura BP/DRE
│   │   ├── balanco_patrimonial.py
│   │   └── dre.py
│   ├── indicadores/             # Calculadores de indicadores
│   │   ├── financeiros.py       # Liquidez, endividamento, etc.
│   │   └── rentabilidade.py     # ROE, ROA, margens, etc.
│   ├── validacao/               # Validadores
│   │   ├── equacao_contabil.py  # Valida Ativo = Passivo + PL
│   │   └── validacao_cruzada.py # Valida consistência BP x DRE
│   ├── utils/                   # Utilitários
│   │   ├── logger.py
│   │   └── spark_helper.py
│   └── config.py                # Configurações globais
├── main.py                      # Pipeline principal
├── app_streamlit.py             # Interface web (a criar)
├── requirements.txt             # Dependências
└── notebooks/                   # Notebooks de análise (antigos)
```

## Pipeline de Classificação

### Estratégia Híbrida (Regras + ML)

O sistema usa uma abordagem em 3 fases para máxima qualidade:

#### **Fase 1: Classificação por Regras (70-80% de cobertura)**
- Baseada em natureza da conta (cd_natureza)
- Padrões de código hierárquico
- Palavras-chave na descrição
- **Vantagens:** Determinístico, auditável, rápido
- **Cobertura esperada:** 70-80% das contas

#### **Fase 2: Machine Learning (15-20% adicional)**
- TF-IDF para análise semântica das descrições
- Random Forest Classifier
- Features: descrição + código + natureza
- Threshold de confiança: 70% (configurável)
- **Vantagens:** Aprende padrões, adapta-se
- **Cobertura adicional:** 15-20%

#### **Fase 3: Validação Cruzada**
- Valida consistência natureza x classificação
- Detecta contas que aparecem em BP e DRE
- Valida equação contábil: ATIVO = PASSIVO + PL

## Demonstrações Contábeis

### Balanço Patrimonial (BP)

Estrutura hierárquica em 3 níveis:

```
ATIVO (1)
├── Ativo Circulante (1.01)
│   ├── Disponibilidades (1.01.01)
│   ├── Créditos (1.01.02)
│   ├── Estoques (1.01.03)
│   ├── Tributos a Recuperar (1.01.04)
│   └── Outros Ativos Circulantes (1.01.05)
└── Ativo Não Circulante (1.02)
    ├── Realizável LP (1.02.01)
    ├── Investimentos (1.02.02)
    ├── Imobilizado (1.02.03)
    └── Intangível (1.02.04)

PASSIVO (2)
├── Passivo Circulante (2.01)
│   ├── Obrigações Trabalhistas (2.01.01)
│   ├── Fornecedores (2.01.02)
│   ├── Obrigações Fiscais (2.01.03)
│   ├── Empréstimos CP (2.01.04)
│   └── Outras Obrigações (2.01.05)
└── Passivo Não Circulante (2.02)
    ├── Empréstimos LP (2.02.01)
    └── Outras Obrigações LP (2.02.02)

PATRIMÔNIO LÍQUIDO (3)
├── Capital Social (3.01)
├── Reservas (3.02)
├── Lucros/Prejuízos Acumulados (3.03)
└── Ajustes de Avaliação (3.04)
```

### DRE (Demonstração do Resultado)

```
(+) Receita Bruta (4.01)
(-) Deduções da Receita (4.02)
(=) RECEITA LÍQUIDA
(-) CMV/CSP (5.01, 5.02)
(=) LUCRO BRUTO
(-) Despesas Operacionais (6.01)
    ├── Despesas Administrativas (6.01.01)
    ├── Despesas com Vendas (6.01.02)
    └── Despesas Financeiras (6.01.03)
(=) LUCRO OPERACIONAL
(+/-) Receitas/Despesas Financeiras (4.03, 6.01.03)
(=) LUCRO ANTES DE IR
(-) IR/CSLL
(=) LUCRO LÍQUIDO
```

## Indicadores Calculados

### Indicadores Financeiros (Liquidez e Endividamento)

1. **Liquidez:**
   - Liquidez Corrente = AC / PC
   - Liquidez Seca = (AC - Estoques) / PC
   - Liquidez Imediata = Disponibilidades / PC
   - Liquidez Geral = (AC + ANC) / (PC + PNC)

2. **Endividamento:**
   - Endividamento Geral = Passivo Total / Ativo Total
   - Composição do Endividamento = PC / Passivo Total
   - Alavancagem Financeira = Passivo / PL
   - Imobilização do PL = ANC / PL

3. **Estrutura de Capital:**
   - Capital de Giro = AC - PC
   - Participação PL = PL / (Passivo + PL)
   - Índice de Solvência = Ativo Total / Passivo Total

### Indicadores de Rentabilidade

1. **Margens:**
   - Margem Bruta = Lucro Bruto / Receita Líquida
   - Margem Operacional = Lucro Operacional / Receita Líquida
   - Margem Líquida = Lucro Líquido / Receita Líquida
   - Margem EBITDA = EBITDA / Receita Líquida

2. **Retorno:**
   - ROA = Lucro Líquido / Ativo Total
   - ROE = Lucro Líquido / Patrimônio Líquido
   - ROIC = EBIT / Capital Investido

3. **Giro:**
   - Giro do Ativo = Receita Líquida / Ativo Total
   - Giro do PL = Receita Líquida / PL

4. **Análise DuPont:**
   - ROE = Margem Líquida × Giro do Ativo × Alavancagem

## Como Usar

### 1. Instalação

```bash
# Instalar dependências
pip install -r requirements.txt

# Criar diretórios necessários
mkdir -p logs models
```

### 2. Executar Pipeline Completo

```bash
# Produção (com ML)
python main.py --modo producao

# Sem ML (apenas regras)
python main.py --modo producao --no-ml

# Processar ano específico
python main.py --ano 2023

# Modo teste
python main.py --modo teste
```

### 3. Treinar Modelo ML (Opcional)

```python
from pyspark.sql import SparkSession
from src.classificacao.pipeline import PipelineClassificacao

spark = SparkSession.builder.getOrCreate()
pipeline = PipelineClassificacao(spark)

# df_treino deve ter contas já classificadas corretamente
metricas = pipeline.treinar_modelo_ml(df_treino)
print(f"Acurácia: {metricas}")
```

### 4. Executar Interface Web

```bash
streamlit run app_streamlit.py
```

## Tabelas Geradas

O pipeline gera as seguintes tabelas no Hive/Impala:

| Tabela | Descrição | Particionamento |
|--------|-----------|-----------------|
| `teste.ecd_contas_classificadas_producao` | Plano de contas classificado | - |
| `teste.ecd_balanco_patrimonial` | Balanço Patrimonial consolidado | ano |
| `teste.ecd_dre` | DRE consolidada | ano |
| `teste.ecd_indicadores_financeiros` | Indicadores calculados | ano |

## Validações Implementadas

1. **Validação de Consistência:**
   - Natureza da conta compatível com classificação
   - Contas não aparecem em BP e DRE simultaneamente

2. **Equação Contábil:**
   - ATIVO = PASSIVO + PATRIMÔNIO LÍQUIDO
   - Tolerância configurável (default: 1%)

3. **Integridade de Dados:**
   - Todas colunas obrigatórias presentes
   - Valores numéricos válidos
   - Códigos de conta únicos

## Qualidade Esperada

Com base nos testes:

- **Cobertura de classificação:** 85-95%
- **Acurácia (com ground truth):** 90-95%
- **Taxa de validação contábil:** > 95%
- **Contas com baixa confiança:** < 10%

## Configuração

Principais configurações em `src/config.py`:

```python
# Conexão Impala
IMPALA_CONFIG = {
    'host': 'localhost',
    'port': 21050,
    'database': 'default'
}

# Machine Learning
ML_CONFIG = {
    'confidence_threshold': 0.7,  # Threshold para aceitar classificação ML
    'n_estimators': 100,
    'max_depth': 10
}

# Validação
VALIDACAO_THRESHOLDS = {
    'tolerancia_equacao': 0.01,  # 1%
    'min_contas_classificadas': 0.80  # 80%
}
```

## Logs

Todos os logs são salvos em `logs/pipeline_main.log` com:
- Timestamp de cada etapa
- Contadores de registros
- Warnings e erros
- Tempo de execução

## Troubleshooting

### Erro: "Modelo ML não encontrado"
```bash
# Treinar modelo primeiro
python -c "from main import treinar_modelo; treinar_modelo()"
```

### Baixa cobertura de classificação
- Verificar keywords em `src/config.py`
- Adicionar mais regras em `src/classificacao/regras.py`
- Treinar modelo ML com mais dados

### Equação contábil não fecha
- Verificar classificação de naturezas
- Validar multiplicador de saldos (ativo positivo, passivo negativo)
- Revisar contas não classificadas

## Performance

Para grandes volumes (> 1M de contas):

1. Aumentar partições Spark:
```python
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

2. Cache de DataFrames intermediários:
```python
df_saldos.cache()
```

3. Processar por ano:
```bash
for ano in 2020 2021 2022 2023; do
    python main.py --ano $ano
done
```

## Próximos Passos

1. [ ] Implementar interface Streamlit integrada
2. [ ] Adicionar exportação para Excel
3. [ ] Criar dashboards de acompanhamento
4. [ ] Implementar alertas automáticos
5. [ ] Adicionar suporte a múltiplos períodos comparativos

## Suporte

Para dúvidas ou problemas:
- Verificar logs em `logs/pipeline_main.log`
- Consultar documentação das classes (docstrings)
- Revisar exemplos nos notebooks antigos
