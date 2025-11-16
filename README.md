# ECD - Escrituração Contábil Digital

Sistema de Inteligência Fiscal para análise de dados contábeis da Receita Estadual de Santa Catarina.

## 📋 Sobre o Projeto

O **ECD (Escrituração Contábil Digital)** é uma plataforma avançada de inteligência fiscal que analisa declarações contábeis digitais de empresas, fornecendo:

- **Dashboard Executivo**: Visão geral de dados contábeis agregados
- **Análise Setorial**: Comparação de empresas por setor (classificação CNAE)
- **Perfil de Empresas**: Informações financeiras detalhadas por empresa
- **Fiscalização Inteligente**: Detecção de riscos e anomalias usando Machine Learning
- **Monitoramento de Alto Risco**: Identificação de empresas com irregularidades financeiras
- **Indicadores Financeiros**: Análise de métricas de liquidez, rentabilidade e endividamento
- **Plano de Contas**: Análise de conformidade com padrões contábeis

O sistema foi desenvolvido para **vigilância fiscal e auditoria inteligente** de declarações contábeis digitais, processando milhares de empresas e anos de dados históricos.

## ✨ Características Principais

- 🎯 **Machine Learning**: Detecção automática de anomalias contábeis
- 📊 **Visualizações Interativas**: Gráficos dinâmicos com Plotly
- 🔒 **Segurança**: Autenticação LDAP com SSL e controle de acesso
- ⚡ **Performance**: Sistema de cache otimizado para grandes volumes de dados
- 🎨 **Interface Moderna**: Dashboard profissional desenvolvido com Streamlit
- 📈 **Big Data**: Integração com Impala/Cloudera para processamento de grandes volumes

## 🛠️ Tecnologias Utilizadas

### Frontend & Dashboard
- **Streamlit** - Framework web para dashboards interativos
- **Plotly Express & Graph Objects** - Visualizações interativas de dados
- **HTML/CSS** - Estilização customizada da interface

### Processamento de Dados
- **Pandas** - Manipulação e análise de dados
- **NumPy** - Operações numéricas
- **SQLAlchemy** - ORM para banco de dados

### Machine Learning
- **scikit-learn**:
  - `IsolationForest` - Detecção de anomalias
  - `RandomForestClassifier` - Classificação de risco
  - `StandardScaler` - Normalização de features
  - `KMeans` - Agrupamento de empresas
- **joblib** - Serialização de modelos

### Banco de Dados
- **Impala** - Motor SQL para Big Data (Cloudera)
- **LDAP** - Autenticação com criptografia SSL

## 📁 Estrutura do Projeto

```
ECD/
├── src/                            # 🆕 Sistema de Produção Modular
│   ├── classificacao/             # Pipeline de classificação híbrido
│   ├── demonstracoes/             # Geradores de BP e DRE
│   ├── indicadores/               # Calculadores de indicadores
│   ├── validacao/                 # Validadores contábeis
│   └── utils/                     # Utilitários
├── main.py                         # 🆕 Pipeline principal de produção
├── ECD.py                          # Aplicação Streamlit (2.762 linhas)
├── notebooks/                      # Notebooks de análise (legado)
│   ├── ECD.ipynb                  # Notebook principal
│   ├── ECD-PC (4).ipynb           # Análise de plano de contas
│   └── ECD-PC-ML (4).ipynb        # Análise com ML
├── requirements.txt                # Dependências Python
├── SISTEMA_PRODUCAO.md            # 🆕 Documentação técnica completa
└── README.md                       # Este arquivo
```

## 🆕 Sistema de Produção (Novo)

Foi desenvolvido um **sistema modular de produção** para substituir os notebooks dispersos por uma arquitetura limpa e escalável:

### Pipeline Completo de Classificação

**Estratégia Híbrida em 3 Fases:**

1. **Classificação por Regras (70-80% cobertura)**
   - Baseada em natureza da conta + código hierárquico + palavras-chave
   - Determinístico e auditável

2. **Machine Learning (15-20% adicional)**
   - TF-IDF + Random Forest
   - Análise semântica de descrições
   - Threshold de confiança configurável (70%)

3. **Validação Cruzada**
   - Valida Ativo = Passivo + PL
   - Detecta inconsistências BP x DRE
   - Relatórios de qualidade

### Executar Sistema de Produção

```bash
# Pipeline completo (classificação + BP + DRE + indicadores)
python main.py --modo producao

# Processar ano específico
python main.py --ano 2023

# Sem ML (apenas regras)
python main.py --no-ml
```

**Documentação completa:** [SISTEMA_PRODUCAO.md](SISTEMA_PRODUCAO.md)

### Vantagens do Novo Sistema

✅ **Modular**: Fácil manutenção e extensão
✅ **Testável**: Separação clara de responsabilidades
✅ **Confiável**: Validações em cada etapa
✅ **Escalável**: Otimizado para grandes volumes
✅ **Documentado**: Docstrings e type hints completos

## 🔧 Pré-requisitos

- Python 3.8+
- Acesso ao servidor Impala (bdaworkernode02.sef.sc.gov.br)
- Credenciais LDAP válidas
- Bibliotecas Python (ver seção Instalação)

## 📦 Instalação

1. Clone o repositório:
```bash
git clone http://local_proxy@127.0.0.1:36405/git/tiagossevero/ECD
cd ECD
```

2. Instale as dependências:
```bash
pip install streamlit pandas numpy plotly sqlalchemy scikit-learn joblib
```

3. Configure as credenciais no arquivo `.streamlit/secrets.toml`:
```toml
[connections.impala]
host = "bdaworkernode02.sef.sc.gov.br"
port = 21050
database = "teste"
username = "seu_usuario_ldap"
password = "sua_senha_ldap"
```

## ⚙️ Configuração

### Banco de Dados

O sistema utiliza as seguintes tabelas no Impala:

| Tabela | Descrição |
|--------|-----------|
| `teste.ecd_empresas_cadastro` | Cadastro de empresas com CNAE |
| `teste.ecd_indicadores_financeiros` | Indicadores financeiros calculados |
| `teste.ecd_balanco_patrimonial` | Balanços patrimoniais |
| `teste.ecd_dre` | Demonstrações de Resultado do Exercício |
| `teste.ecd_score_risco_consolidado` | Scores de risco consolidados |
| `teste.ecd_saldos_contas_v2` | Saldos de contas contábeis |
| `teste.ecd_plano_contas` | Plano de contas |

### Autenticação

Senha de acesso ao sistema: `ecd2025`

## 🚀 Uso

Execute o aplicativo Streamlit:

```bash
streamlit run ECD.py
```

Acesse o dashboard em: `http://localhost:8501`

## 📊 Funcionalidades

### 1. 🏠 Dashboard Geral
- Total de empresas analisadas
- Médias de ativos, receitas e liquidez
- Top 15 setores por quantidade e patrimônio
- Métricas de rentabilidade e saúde financeira

### 2. 📊 Análise por Setor
- Estatísticas detalhadas por setor
- Indicadores financeiros por indústria
- Principais empresas por setor
- Gráficos comparativos setoriais

### 3. 🏢 Detalhamento de Empresa
Interface com 5 abas:
- **Cadastro**: CNPJ, razão social, CNAE
- **Balanço Patrimonial**: Ativos e Passivos
- **DRE**: Receitas e Despesas
- **Indicadores Financeiros**: Métricas calculadas
- **Avaliação de Risco**: Score e análise de anomalias

### 4. 🎯 Fiscalização Inteligente (ML)
- Detecção de anomalias baseada em ML
- Modelo de scoring de risco
- Top 50 empresas suspeitas
- Visualização de scores de anomalia

### 5. ⚠️ Empresas Alto Risco
- Ranking por score de risco
- Múltiplos indicadores de risco
- Filtros por nível de risco

### 6. 📉 Indicadores Financeiros
- Liquidez, rentabilidade, endividamento
- Análise comparativa
- Análise de tendências

### 7. 🗂️ Plano de Contas
- Contas universais (uso >80%)
- Contas de alta variabilidade
- Classificação de contas

## 🤖 Machine Learning

### Modelos Implementados

#### 1. Isolation Forest
- **Propósito**: Detecção de anomalias contábeis
- **Features**: Indicadores financeiros, saldos de contas
- **Output**: Score de anomalia (-1 a 1)

#### 2. Random Forest Classifier
- **Propósito**: Classificação de risco
- **Features**: Métricas financeiras normalizadas
- **Output**: Score de risco (0 a 100)

#### 3. K-Means Clustering
- **Propósito**: Agrupamento de empresas similares
- **Features**: Perfil financeiro
- **Output**: Clusters de empresas

### Sistema de Cache

O sistema utiliza duas estratégias de cache para otimizar performance:

- `@st.cache_resource`: Engine de banco (compartilhado entre sessões)
- `@st.cache_data(ttl=3600)`: Queries de dados (cache de 1 hora)

## 📈 Indicadores Calculados

### Liquidez
- **Liquidez Corrente**: Ativo Circulante / Passivo Circulante
- **Liquidez Seca**: (AC - Estoques) / Passivo Circulante
- **Liquidez Imediata**: Disponibilidades / Passivo Circulante

### Rentabilidade
- **ROA**: Lucro Líquido / Ativo Total
- **ROE**: Lucro Líquido / Patrimônio Líquido
- **Margem Líquida**: Lucro Líquido / Receita Líquida

### Endividamento
- **Endividamento Geral**: Passivo Total / Ativo Total
- **Composição do Endividamento**: PC / (PC + PNC)
- **Imobilização do PL**: Ativo Permanente / PL

## 🔍 Detecção de Anomalias

O sistema identifica anomalias através de:

1. **Análise de Padrões**: Comparação com médias setoriais
2. **Detecção Estatística**: Identificação de outliers
3. **Machine Learning**: Isolation Forest para padrões complexos
4. **Regras de Negócio**: Validação de limites contábeis

### Critérios de Alto Risco

- Score de risco > 70
- Anomalias em múltiplos indicadores
- Desvios significativos da média setorial
- Inconsistências no plano de contas

## 🤝 Contribuição

Este é um projeto interno da Receita Estadual de Santa Catarina. Para contribuir:

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/NovaFuncionalidade`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/NovaFuncionalidade`)
5. Abra um Pull Request

## 📝 Notas de Versão

### Versão 2.0 - Dashboard Streamlit com Machine Learning
- Interface web completa com Streamlit
- Implementação de modelos de ML
- Sistema de cache otimizado
- 7 módulos de análise
- Visualizações interativas com Plotly

## 👥 Autores

- **Receita Estadual de Santa Catarina** - Desenvolvimento e manutenção

## 📄 Licença

Este projeto é de propriedade da Receita Estadual de Santa Catarina e destina-se exclusivamente ao uso interno para fins de fiscalização tributária.

---

## 🆘 Suporte

Para questões técnicas ou suporte, entre em contato com a equipe de TI da Receita Estadual de Santa Catarina.

## 🔐 Segurança

- Todas as conexões utilizam SSL/TLS
- Autenticação via LDAP
- Dados sensíveis protegidos por controle de acesso
- Logs de auditoria para todas as operações

## ⚡ Performance

- Cache de 1 hora para queries de dados
- Otimização de queries SQL no Impala
- Processamento vetorizado com NumPy/Pandas
- Lazy loading de visualizações

---

**Desenvolvido com ❤️ para a Receita Estadual de Santa Catarina**
