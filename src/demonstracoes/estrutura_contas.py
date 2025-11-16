"""
Definição da estrutura hierárquica de contas para BP e DRE
"""
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import re


@dataclass
class ContaEstrutura:
    """Representa uma conta na estrutura BP/DRE"""
    codigo: str
    nome: str
    tipo: str  # 'grupo', 'subgrupo', 'conta'
    nivel: int
    keywords: List[str] = None
    parent: str = None

    def __post_init__(self):
        if self.keywords is None:
            self.keywords = []


class EstruturaContas:
    """Gerencia a estrutura hierárquica de contas do BP e DRE"""

    def __init__(self):
        self.estrutura_bp = self._criar_estrutura_bp()
        self.estrutura_dre = self._criar_estrutura_dre()
        self.todas_contas = {**self.estrutura_bp, **self.estrutura_dre}

    def _criar_estrutura_bp(self) -> Dict[str, ContaEstrutura]:
        """Cria estrutura do Balanço Patrimonial"""
        contas = {}

        # ATIVO
        contas['1'] = ContaEstrutura('1', 'ATIVO', 'grupo', 1)
        contas['1.01'] = ContaEstrutura('1.01', 'ATIVO CIRCULANTE', 'subgrupo', 2, parent='1')
        contas['1.01.01'] = ContaEstrutura(
            '1.01.01', 'Disponibilidades', 'conta', 3,
            keywords=['caixa', 'banco', 'aplicação financeira', 'aplicações de liquidez'],
            parent='1.01'
        )
        contas['1.01.02'] = ContaEstrutura(
            '1.01.02', 'Créditos', 'conta', 3,
            keywords=['duplicatas a receber', 'clientes', 'contas a receber', 'títulos a receber'],
            parent='1.01'
        )
        contas['1.01.03'] = ContaEstrutura(
            '1.01.03', 'Estoques', 'conta', 3,
            keywords=['estoque', 'mercadoria', 'produto acabado', 'matéria-prima'],
            parent='1.01'
        )
        contas['1.01.04'] = ContaEstrutura(
            '1.01.04', 'Tributos a Recuperar', 'conta', 3,
            keywords=['icms a recuperar', 'pis a recuperar', 'cofins a recuperar', 'tributo a recuperar'],
            parent='1.01'
        )
        contas['1.01.05'] = ContaEstrutura(
            '1.01.05', 'Outros Ativos Circulantes', 'conta', 3,
            keywords=['despesa antecipada', 'adiantamento', 'outros ativos circulantes'],
            parent='1.01'
        )

        contas['1.02'] = ContaEstrutura('1.02', 'ATIVO NÃO CIRCULANTE', 'subgrupo', 2, parent='1')
        contas['1.02.01'] = ContaEstrutura(
            '1.02.01', 'Realizável a Longo Prazo', 'conta', 3,
            keywords=['título a receber longo prazo', 'empréstimo longo prazo', 'realizável longo prazo'],
            parent='1.02'
        )
        contas['1.02.02'] = ContaEstrutura(
            '1.02.02', 'Investimentos', 'conta', 3,
            keywords=['participação societária', 'investimento', 'ações'],
            parent='1.02'
        )
        contas['1.02.03'] = ContaEstrutura(
            '1.02.03', 'Imobilizado', 'conta', 3,
            keywords=['imóvel', 'veículo', 'móvel', 'máquina', 'equipamento', 'imobilizado'],
            parent='1.02'
        )
        contas['1.02.04'] = ContaEstrutura(
            '1.02.04', 'Intangível', 'conta', 3,
            keywords=['software', 'marca', 'patente', 'goodwill', 'intangível'],
            parent='1.02'
        )

        # PASSIVO
        contas['2'] = ContaEstrutura('2', 'PASSIVO', 'grupo', 1)
        contas['2.01'] = ContaEstrutura('2.01', 'PASSIVO CIRCULANTE', 'subgrupo', 2, parent='2')
        contas['2.01.01'] = ContaEstrutura(
            '2.01.01', 'Obrigações Trabalhistas', 'conta', 3,
            keywords=['salário a pagar', 'féria a pagar', 'encargo', 'provisão férias'],
            parent='2.01'
        )
        contas['2.01.02'] = ContaEstrutura(
            '2.01.02', 'Fornecedores', 'conta', 3,
            keywords=['fornecedor', 'conta a pagar', 'obrigações com fornecedores'],
            parent='2.01'
        )
        contas['2.01.03'] = ContaEstrutura(
            '2.01.03', 'Obrigações Fiscais', 'conta', 3,
            keywords=['tributo a recolher', 'imposto a pagar', 'obrigações fiscais'],
            parent='2.01'
        )
        contas['2.01.04'] = ContaEstrutura(
            '2.01.04', 'Empréstimos e Financiamentos CP', 'conta', 3,
            keywords=['empréstimo curto prazo', 'financiamento curto prazo', 'empréstimo cp'],
            parent='2.01'
        )
        contas['2.01.05'] = ContaEstrutura(
            '2.01.05', 'Outras Obrigações', 'conta', 3,
            keywords=['provisão', 'obrigação diversa', 'outras obrigações'],
            parent='2.01'
        )

        contas['2.02'] = ContaEstrutura('2.02', 'PASSIVO NÃO CIRCULANTE', 'subgrupo', 2, parent='2')
        contas['2.02.01'] = ContaEstrutura(
            '2.02.01', 'Empréstimos e Financiamentos LP', 'conta', 3,
            keywords=['empréstimo longo prazo', 'financiamento longo prazo', 'empréstimo lp'],
            parent='2.02'
        )
        contas['2.02.02'] = ContaEstrutura(
            '2.02.02', 'Outras Obrigações LP', 'conta', 3,
            keywords=['provisão longo prazo', 'tributo diferido', 'outras obrigações lp'],
            parent='2.02'
        )

        # PATRIMÔNIO LÍQUIDO
        contas['3'] = ContaEstrutura('3', 'PATRIMÔNIO LÍQUIDO', 'grupo', 1)
        contas['3.01'] = ContaEstrutura(
            '3.01', 'Capital Social', 'conta', 2,
            keywords=['capital social', 'capital subscrito', 'capital integralizado'],
            parent='3'
        )
        contas['3.02'] = ContaEstrutura(
            '3.02', 'Reservas', 'conta', 2,
            keywords=['reserva de lucro', 'reserva de capital', 'reserva legal'],
            parent='3'
        )
        contas['3.03'] = ContaEstrutura(
            '3.03', 'Lucros/Prejuízos Acumulados', 'conta', 2,
            keywords=['lucro acumulado', 'prejuízo acumulado', 'resultado acumulado'],
            parent='3'
        )
        contas['3.04'] = ContaEstrutura(
            '3.04', 'Ajustes de Avaliação Patrimonial', 'conta', 2,
            keywords=['ajuste de avaliação', 'avaliação patrimonial'],
            parent='3'
        )

        return contas

    def _criar_estrutura_dre(self) -> Dict[str, ContaEstrutura]:
        """Cria estrutura da DRE"""
        contas = {}

        # RECEITAS
        contas['4'] = ContaEstrutura('4', 'RECEITAS', 'grupo', 1)
        contas['4.01'] = ContaEstrutura(
            '4.01', 'Receita Bruta', 'conta', 2,
            keywords=['receita de venda', 'receita de serviço', 'faturamento', 'receita operacional'],
            parent='4'
        )
        contas['4.02'] = ContaEstrutura(
            '4.02', 'Deduções da Receita', 'conta', 2,
            keywords=['devolução', 'abatimento', 'imposto sobre venda', 'dedução'],
            parent='4'
        )
        contas['4.03'] = ContaEstrutura(
            '4.03', 'Receitas Financeiras', 'conta', 2,
            keywords=['receita financeira', 'juros ativo', 'rendimento aplicação'],
            parent='4'
        )
        contas['4.04'] = ContaEstrutura(
            '4.04', 'Outras Receitas', 'conta', 2,
            keywords=['outra receita', 'receita eventual', 'ganho'],
            parent='4'
        )

        # CUSTOS
        contas['5'] = ContaEstrutura('5', 'CUSTOS', 'grupo', 1)
        contas['5.01'] = ContaEstrutura(
            '5.01', 'CMV - Custo das Mercadorias Vendidas', 'conta', 2,
            keywords=['cmv', 'custo de venda', 'custo de mercadoria'],
            parent='5'
        )
        contas['5.02'] = ContaEstrutura(
            '5.02', 'CSP - Custo dos Serviços Prestados', 'conta', 2,
            keywords=['csp', 'custo de serviço', 'custo dos serviços'],
            parent='5'
        )

        # DESPESAS
        contas['6'] = ContaEstrutura('6', 'DESPESAS', 'grupo', 1)
        contas['6.01'] = ContaEstrutura('6.01', 'Despesas Operacionais', 'subgrupo', 2, parent='6')
        contas['6.01.01'] = ContaEstrutura(
            '6.01.01', 'Despesas Administrativas', 'conta', 3,
            keywords=['salário', 'aluguel', 'despesa geral', 'despesa administrativa', 'honorário'],
            parent='6.01'
        )
        contas['6.01.02'] = ContaEstrutura(
            '6.01.02', 'Despesas com Vendas', 'conta', 3,
            keywords=['comissão', 'propaganda', 'marketing', 'despesa com venda'],
            parent='6.01'
        )
        contas['6.01.03'] = ContaEstrutura(
            '6.01.03', 'Despesas Financeiras', 'conta', 3,
            keywords=['juro', 'despesa bancária', 'despesa financeira', 'juros passivo'],
            parent='6.01'
        )
        contas['6.02'] = ContaEstrutura(
            '6.02', 'Outras Despesas', 'conta', 2,
            keywords=['outra despesa', 'perda', 'despesa eventual'],
            parent='6'
        )

        return contas

    def get_conta(self, codigo: str) -> Optional[ContaEstrutura]:
        """Retorna conta por código"""
        return self.todas_contas.get(codigo)

    def get_contas_by_nivel(self, nivel: int) -> List[ContaEstrutura]:
        """Retorna todas contas de um nível específico"""
        return [c for c in self.todas_contas.values() if c.nivel == nivel]

    def get_contas_bp(self) -> Dict[str, ContaEstrutura]:
        """Retorna apenas contas do BP"""
        return self.estrutura_bp

    def get_contas_dre(self) -> Dict[str, ContaEstrutura]:
        """Retorna apenas contas da DRE"""
        return self.estrutura_dre

    def match_by_keywords(self, descricao: str) -> List[Tuple[str, float]]:
        """
        Busca contas que correspondem às keywords na descrição

        Args:
            descricao: Descrição da conta a classificar

        Returns:
            Lista de tuplas (codigo_conta, score) ordenada por score
        """
        descricao_lower = descricao.lower()
        matches = []

        for codigo, conta in self.todas_contas.items():
            if not conta.keywords:
                continue

            score = 0
            for keyword in conta.keywords:
                if keyword in descricao_lower:
                    # Score maior para matches exatos de palavras completas
                    if re.search(r'\b' + re.escape(keyword) + r'\b', descricao_lower):
                        score += 2
                    else:
                        score += 1

            if score > 0:
                matches.append((codigo, score))

        # Ordenar por score (maior primeiro)
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    def get_parent_chain(self, codigo: str) -> List[str]:
        """
        Retorna cadeia de códigos pai de uma conta

        Args:
            codigo: Código da conta

        Returns:
            Lista com [nivel1, nivel2, nivel3, ...]
        """
        chain = []
        conta = self.get_conta(codigo)

        while conta:
            chain.insert(0, conta.codigo)
            if conta.parent:
                conta = self.get_conta(conta.parent)
            else:
                conta = None

        return chain

    def is_bp_account(self, codigo: str) -> bool:
        """Verifica se conta pertence ao BP"""
        return codigo[0] in ['1', '2', '3']

    def is_dre_account(self, codigo: str) -> bool:
        """Verifica se conta pertence à DRE"""
        return codigo[0] in ['4', '5', '6']
