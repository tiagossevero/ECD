"""
Configurações globais do sistema ECD
"""

# Configurações Impala/Cloudera
IMPALA_CONFIG = {
    'host': 'localhost',  # Ajustar conforme ambiente
    'port': 21050,
    'database': 'default'
}

# Tabelas principais
TABELAS = {
    'ecd_i150': 'teste.ecd_i150',  # Tabela de saldos
    'ecd_contas': 'teste.ecd_contas',  # Plano de contas
    'ecd_i200': 'teste.ecd_i200',  # Tabela de histórico
    'ecd_classificadas': 'teste.ecd_contas_classificadas_producao',  # Saída classificação
    'ecd_bp': 'teste.ecd_balanco_patrimonial',  # Balanço Patrimonial
    'ecd_dre': 'teste.ecd_dre',  # DRE
    'ecd_indicadores': 'teste.ecd_indicadores_financeiros'  # Indicadores
}

# Estrutura do Balanço Patrimonial
ESTRUTURA_BP = {
    '1': {
        'codigo': '1',
        'nome': 'ATIVO',
        'tipo': 'grupo',
        'filhos': {
            '1.01': {
                'codigo': '1.01',
                'nome': 'ATIVO CIRCULANTE',
                'tipo': 'subgrupo',
                'filhos': {
                    '1.01.01': {'codigo': '1.01.01', 'nome': 'Disponibilidades', 'contas': ['caixa', 'banco', 'aplicações financeiras']},
                    '1.01.02': {'codigo': '1.01.02', 'nome': 'Créditos', 'contas': ['duplicatas a receber', 'clientes', 'contas a receber']},
                    '1.01.03': {'codigo': '1.01.03', 'nome': 'Estoques', 'contas': ['estoque', 'mercadorias', 'produtos acabados']},
                    '1.01.04': {'codigo': '1.01.04', 'nome': 'Tributos a Recuperar', 'contas': ['icms a recuperar', 'pis a recuperar', 'cofins a recuperar']},
                    '1.01.05': {'codigo': '1.01.05', 'nome': 'Outros Ativos Circulantes', 'contas': ['despesas antecipadas', 'adiantamentos']}
                }
            },
            '1.02': {
                'codigo': '1.02',
                'nome': 'ATIVO NÃO CIRCULANTE',
                'tipo': 'subgrupo',
                'filhos': {
                    '1.02.01': {'codigo': '1.02.01', 'nome': 'Realizável a Longo Prazo', 'contas': ['títulos a receber lp', 'empréstimos lp']},
                    '1.02.02': {'codigo': '1.02.02', 'nome': 'Investimentos', 'contas': ['participações societárias', 'investimentos']},
                    '1.02.03': {'codigo': '1.02.03', 'nome': 'Imobilizado', 'contas': ['imóveis', 'veículos', 'móveis', 'máquinas', 'equipamentos']},
                    '1.02.04': {'codigo': '1.02.04', 'nome': 'Intangível', 'contas': ['software', 'marcas', 'patentes', 'goodwill']}
                }
            }
        }
    },
    '2': {
        'codigo': '2',
        'nome': 'PASSIVO',
        'tipo': 'grupo',
        'filhos': {
            '2.01': {
                'codigo': '2.01',
                'nome': 'PASSIVO CIRCULANTE',
                'tipo': 'subgrupo',
                'filhos': {
                    '2.01.01': {'codigo': '2.01.01', 'nome': 'Obrigações Trabalhistas', 'contas': ['salários a pagar', 'férias a pagar', 'encargos']},
                    '2.01.02': {'codigo': '2.01.02', 'nome': 'Fornecedores', 'contas': ['fornecedores', 'contas a pagar']},
                    '2.01.03': {'codigo': '2.01.03', 'nome': 'Obrigações Fiscais', 'contas': ['tributos a recolher', 'impostos a pagar']},
                    '2.01.04': {'codigo': '2.01.04', 'nome': 'Empréstimos e Financiamentos', 'contas': ['empréstimos cp', 'financiamentos cp']},
                    '2.01.05': {'codigo': '2.01.05', 'nome': 'Outras Obrigações', 'contas': ['provisões', 'obrigações diversas']}
                }
            },
            '2.02': {
                'codigo': '2.02',
                'nome': 'PASSIVO NÃO CIRCULANTE',
                'tipo': 'subgrupo',
                'filhos': {
                    '2.02.01': {'codigo': '2.02.01', 'nome': 'Empréstimos e Financiamentos LP', 'contas': ['empréstimos lp', 'financiamentos lp']},
                    '2.02.02': {'codigo': '2.02.02', 'nome': 'Outras Obrigações LP', 'contas': ['provisões lp', 'tributos diferidos']}
                }
            }
        }
    },
    '3': {
        'codigo': '3',
        'nome': 'PATRIMÔNIO LÍQUIDO',
        'tipo': 'grupo',
        'filhos': {
            '3.01': {'codigo': '3.01', 'nome': 'Capital Social', 'contas': ['capital social', 'capital subscrito']},
            '3.02': {'codigo': '3.02', 'nome': 'Reservas', 'contas': ['reservas de lucros', 'reservas de capital']},
            '3.03': {'codigo': '3.03', 'nome': 'Lucros/Prejuízos Acumulados', 'contas': ['lucros acumulados', 'prejuízos acumulados']},
            '3.04': {'codigo': '3.04', 'nome': 'Ajustes de Avaliação Patrimonial', 'contas': ['ajustes']}
        }
    }
}

# Estrutura da DRE
ESTRUTURA_DRE = {
    '4': {
        'codigo': '4',
        'nome': 'RECEITAS',
        'tipo': 'grupo',
        'filhos': {
            '4.01': {'codigo': '4.01', 'nome': 'Receita Bruta', 'contas': ['receita de vendas', 'receita de serviços']},
            '4.02': {'codigo': '4.02', 'nome': 'Deduções da Receita', 'contas': ['devoluções', 'abatimentos', 'impostos sobre vendas']}
        }
    },
    '5': {
        'codigo': '5',
        'nome': 'CUSTOS',
        'tipo': 'grupo',
        'filhos': {
            '5.01': {'codigo': '5.01', 'nome': 'Custo das Mercadorias Vendidas (CMV)', 'contas': ['cmv', 'custo de vendas']},
            '5.02': {'codigo': '5.02', 'nome': 'Custo dos Serviços Prestados (CSP)', 'contas': ['csp', 'custo de serviços']}
        }
    },
    '6': {
        'codigo': '6',
        'nome': 'DESPESAS',
        'tipo': 'grupo',
        'filhos': {
            '6.01': {'codigo': '6.01', 'nome': 'Despesas Operacionais', 'filhos': {
                '6.01.01': {'codigo': '6.01.01', 'nome': 'Despesas Administrativas', 'contas': ['salários', 'aluguéis', 'despesas gerais']},
                '6.01.02': {'codigo': '6.01.02', 'nome': 'Despesas com Vendas', 'contas': ['comissões', 'propaganda', 'marketing']},
                '6.01.03': {'codigo': '6.01.03', 'nome': 'Despesas Financeiras', 'contas': ['juros', 'despesas bancárias']}
            }},
            '6.02': {'codigo': '6.02', 'nome': 'Outras Receitas e Despesas', 'contas': ['receitas financeiras', 'outras receitas']}
        }
    }
}

# Naturezas de conta (baseado na estrutura ECD)
NATUREZAS = {
    '01': 'Ativa',
    '02': 'Passiva',
    '03': 'Ativa/Passiva',
    '04': 'Patrimônio Líquido',
    '05': 'Compensação Ativa',
    '06': 'Compensação Passiva',
    '07': 'Resultado (Receita)',
    '08': 'Resultado (Despesa)',
    '09': 'Híbrida'
}

# Palavras-chave para classificação por regras
KEYWORDS_ATIVO_CIRCULANTE = [
    'caixa', 'banco', 'aplicação financeira', 'aplicações de liquidez imediata',
    'duplicatas a receber', 'clientes', 'contas a receber',
    'estoque', 'mercadoria', 'produto acabado',
    'icms a recuperar', 'pis a recuperar', 'cofins a recuperar',
    'despesa antecipada', 'adiantamento'
]

KEYWORDS_ATIVO_NAO_CIRCULANTE = [
    'imóvel', 'veículo', 'móvel', 'máquina', 'equipamento', 'imobilizado',
    'software', 'marca', 'patente', 'goodwill', 'intangível',
    'participação societária', 'investimento',
    'título a receber longo prazo', 'empréstimo longo prazo'
]

KEYWORDS_PASSIVO_CIRCULANTE = [
    'fornecedor', 'conta a pagar',
    'salário a pagar', 'féria a pagar', 'encargo',
    'tributo a recolher', 'imposto a pagar',
    'empréstimo curto prazo', 'financiamento curto prazo'
]

KEYWORDS_PASSIVO_NAO_CIRCULANTE = [
    'empréstimo longo prazo', 'financiamento longo prazo',
    'provisão longo prazo', 'tributo diferido'
]

KEYWORDS_PATRIMONIO_LIQUIDO = [
    'capital social', 'capital subscrito',
    'reserva', 'lucro acumulado', 'prejuízo acumulado',
    'ajuste de avaliação'
]

KEYWORDS_RECEITA = [
    'receita', 'venda', 'serviço prestado', 'faturamento'
]

KEYWORDS_CUSTO = [
    'cmv', 'custo de venda', 'custo de mercadoria',
    'csp', 'custo de serviço'
]

KEYWORDS_DESPESA = [
    'despesa', 'salário', 'aluguel', 'comissão',
    'propaganda', 'marketing', 'juro', 'despesa bancária'
]

# Configurações de Machine Learning
ML_CONFIG = {
    'test_size': 0.2,
    'random_state': 42,
    'n_estimators': 100,
    'max_depth': 10,
    'min_samples_split': 5,
    'tfidf_max_features': 1000,
    'confidence_threshold': 0.7  # Threshold para aceitar classificação ML
}

# Thresholds para validação
VALIDACAO_THRESHOLDS = {
    'tolerancia_equacao': 0.01,  # 1% de tolerância na equação contábil
    'min_contas_classificadas': 0.80,  # Mínimo 80% das contas classificadas
    'max_contas_nao_classificadas': 0.20  # Máximo 20% não classificadas
}

# Configurações de logging
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'logs/ecd_pipeline.log'
}
