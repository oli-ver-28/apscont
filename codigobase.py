
import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile 
import matplotlib.pyplot as plt

def read_csv_from_zip(url, file, sep=';'): 
    r=requests.get(url)
    zf = ZipFile(BytesIO(r.content))
    file=zf.open(file)
    lines = file.readlines()
    lines=[i.strip().decode('ISO-8859-1') for i in lines]
    file.close()
    values = [i.replace('\n','').strip().split(sep) for i in lines]
    df = pd.DataFrame(values[1:], columns=values[0])
    return df

def relatorio_cias_abertas(ano, cod, tipo_periodo, tipo_demonstrativo): 
    url=f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo_periodo}/DADOS/{tipo_periodo.lower()}_cia_aberta_{ano}.zip'
    arquivo = f'{tipo_periodo.lower()}_cia_aberta_{cod}_{tipo_demonstrativo}_{ano}.csv'
    df = read_csv_from_zip(url, arquivo) 
    return df

def carregar_data(ano, cod, tipo_periodo, tipo_demonstrativo, colunas_para_remover,
filtro_cvm='022470'):
    url =f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo_periodo}/DADOS/{tipo_periodo.lower()}_cia_aberta_{ano}.zip'
    arquivo = f'{tipo_periodo.lower()}_cia_aberta_{cod}_{tipo_demonstrativo}_{ano}.csv'
    df = read_csv_from_zip(url, arquivo)
    df = df[df['CD_CVM'] == filtro_cvm]
    df.drop(columns=colunas_para_remover, inplace=True)
    df['DT_FIM_EXERC'] = pd.to_datetime(df['DT_FIM_EXERC'])
    df = df[df['DT_FIM_EXERC'].dt.year == ano]
    df['VL_CONTA'] = pd.to_numeric(df['VL_CONTA'], errors='coerce').fillna(0)
    df = df[df['VL_CONTA'] != 0]
    return df
def calculo(df_ativos, df_passivos, df_receitas, df_despesas,
df_depreciacao,pl_anterior,estoque_anterior,receb_anterior,passivo_fornecedores_anterior):
    metrics = {}
    pl_atual = df_passivos[df_passivos['DS_CONTA'].str.contains('Patrimônio LíquidoConsolidado')]['VL_CONTA'].sum()
    if pl_anterior is not None:
        pl_medio = (pl_anterior + pl_atual) / 2
    else:
        pl_medio = pl_atual


    estoque_atual = df_ativos[df_ativos['DS_CONTA'].str.contains('Estoques')]['VL_CONTA'].sum()
    if estoque_anterior is not None:
        estoque_medio = (estoque_anterior + estoque_atual) / 2
    else:
        estoque_medio = estoque_atual

    receb_atual = df_ativos[df_ativos['DS_CONTA'].str.contains('Contas aReceber')]['VL_CONTA'].sum()
    if receb_anterior is not None:
        receb_medio = (receb_anterior + receb_atual) / 2
    else:
        receb_medio = receb_atual

    passivo_fornecedores_atual = df_passivos.loc[df_passivos.loc[df_passivos['CD_CONTA'] == '2.01.02'].first_valid_index(), 'VL_CONTA']
    if passivo_fornecedores_anterior is not None:
        passivo_fornecedores_medio = (passivo_fornecedores_anterior +
        passivo_fornecedores_atual) / 2
    else:
        passivo_fornecedores_medio = passivo_fornecedores_atual

    passivos_circulantes = df_passivos[df_passivos['DS_CONTA'].str.contains('PassivoCirculante')]['VL_CONTA'].sum()
    if passivos_circulantes != 0:
         metrics['liquidez_corrente'] = round(df_ativos[df_ativos['DS_CONTA'].str.contains('Ativo Circulante')]['VL_CONTA'].sum() / passivos_circulantes, 1)
         caixa_equivalentes = df_ativos[df_ativos['DS_CONTA'].str.contains('Caixa e Equivalentes de Caixa')]['VL_CONTA'].sum()
         metrics['liquidez_imediata'] = round(caixa_equivalentes / passivos_circulantes, 1)
         estoques = df_ativos[df_ativos['DS_CONTA'].str.contains('Estoques')]['VL_CONTA'].sum()
         metrics['liquidez_seca'] = round((df_ativos[df_ativos['DS_CONTA'].str.contains('AtivoCirculante')]['VL_CONTA'].sum() - estoques) / passivos_circulantes, 1)
    else:
        metrics['liquidez_corrente'] = metrics['liquidez_imediata'] = metrics['liquidez_seca'] = float('inf')


    # EBIT and EBITDA
    df_receitas['VL_CONTA'] = pd.to_numeric(df_receitas['VL_CONTA'], errors='coerce')
    df_receitas['VL_CONTA'] = pd.to_numeric(df_receitas['VL_CONTA'], errors='coerce')
    df_receitas['VL_CONTA'] = pd.to_numeric(df_receitas['VL_CONTA'], errors='coerce')

    total_receitas = df_receitas.loc[df_receitas['DS_CONTA'].str.contains('Receita de Venda de Bens e/ou Serviços', case=False, na=False), 'VL_CONTA'].sum()
    total_despesas = df_receitas.loc[df_receitas['DS_CONTA'].str.contains('Custo dos Bens e/ou Serviços Vendidos|Despesas/Receitas Operacionais', case=False, na=False),'VL_CONTA'].sum()
    ebit = total_receitas + total_despesas
    metrics['EBIT'] = round(ebit, 2)
    df_depreciacao_amortizacao=df_receitas
    df_depreciacao_amortizacao['VL_CONTA'] = pd.to_numeric(df_depreciacao_amortizacao['VL_CONTA'], errors='coerce')
    regex_depreciacao = r'Deprecia[cç][ãa]o'
    depreciacao =df_depreciacao_amortizacao.loc[df_depreciacao_amortizacao['DS_CONTA'].str.contains(regex_depreciacao, case=False, regex=True, na=False), 'VL_CONTA'].sum()
    metrics['EBITDA'] = round(ebit - depreciacao, 2)


    # Patrimonio Liquido, Divida Liquida e passivo total
    patrimonio_liquido = df_passivos[df_passivos['DS_CONTA'].str.contains('PatrimônioLíquido Consolidado')]['VL_CONTA'].sum()
    total_passivo = df_passivos.loc[df_passivos['DS_CONTA'].str.contains('Passivo Circulante|Passivo Não Circulante',case=False, regex=True),'VL_CONTA'].sum()
    divida_liquida=df_passivos.loc[df_passivos['DS_CONTA'].str.contains('Empréstimos e Financiamentos', case=False,regex=True),'VL_CONTA'].sum()


    # Capital Proprio e de Terceiros

    comp_capital_proprio = patrimonio_liquido/df_passivos[df_passivos['DS_CONTA'].str.contains('PassivoTotal')]['VL_CONTA'].sum()
    metrics['Composição_do_capital_proprio'] = round(comp_capital_proprio*100, 0)
    comp_capital_de_terceiros = total_passivo/df_passivos[df_passivos['DS_CONTA'].str.contains('PassivoTotal')]['VL_CONTA'].sum()
    metrics['Composição_do_capital_de_terceiros'] = round(comp_capital_de_terceiros*100,0)
    metrics['capital_proprio'] = patrimonio_liquido
    metrics['capital_terceiros'] = total_passivo


    # Divida Liquida/EBITDA

    metrics['divida_liquida_sobre_EBITDA'] = round((divida_liquida - caixa_equivalentes) /
    metrics['EBITDA'],1) if metrics['EBITDA'] != 0 else float('inf')


    # ICJ
    metrics['indice_cobertura_juros'] = round(ebit /abs(df_despesas[df_despesas['DS_CONTA'].str.contains('DespesasFinanceiras')]['VL_CONTA'].sum()), 1) if df_despesas[df_despesas['DS_CONTA'].str.contains('DespesasFinanceiras')]['VL_CONTA'].sum() != 0 else float('inf')



    # Endividamento
    metrics['endividamento'] = round((total_passivo / df_ativos[df_ativos['DS_CONTA'].str.contains('Ativo Total')]['VL_CONTA'].sum())*100, 0)



    # ROE

    lucro_liquido = df_receitas.loc[df_receitas['DS_CONTA'].str.contains('Lucro/PrejuízoConsolidado do Período', case=False), 'VL_CONTA'].sum()
    metrics['ROE'] = round((lucro_liquido / pl_medio)*100, 0) if pl_medio != 0 else float('inf')



    # Margens
    receita_vendas = df_receitas[df_receitas['DS_CONTA'].str.contains('Receita de Venda de Bens e/ou Serviços', case=False, na=False)]['VL_CONTA'].sum()
    custo_vendas = df_despesas[df_despesas['DS_CONTA'].str.contains('Custo dos Bens e/ou Serviços Vendidos', case=False, na=False)]['VL_CONTA'].sum()
    margem_bruta = (receita_vendas + custo_vendas) / receita_vendas if receita_vendas != 0 else 0
    margem_ebit = (receita_vendas + total_despesas) / receita_vendas if receita_vendas != 0 else 0
    margem_liquida = (lucro_liquido) / receita_vendas if receita_vendas != 0 else 0
    metrics['Margem_bruta'] = round(margem_bruta*100, 0)
    metrics['Margem_EBIT'] = round(margem_ebit*100, 0)
    metrics['Margem_liquida'] = round(margem_liquida*100, 0)




    # Perfil da Divida
    perfil_divida = round((passivos_circulantes/total_passivo)*100,0)
    metrics['Perfil_da_divida']=perfil_divida



    # PMRE
    if custo_vendas != 0:
        pme = (estoque_medio / abs(custo_vendas)) * 365
    else:
        pme = float('inf')
        metrics['PMRE'] = round(pme, 0)


    # PMRV
    total_vendas = df_receitas[df_receitas['DS_CONTA'].str.contains('Receita de Venda de Bens e/ou Serviços', case=False, na=False)]['VL_CONTA'].sum()
    if total_vendas != 0:
        pmr= (receb_medio/total_vendas)*365
    else:
        pme = float('inf')
        metrics['PMRV'] = round(pmr, 0)
    # PMPF
    if estoque_anterior is not None:
        compras = (estoque_atual - estoque_anterior + abs(custo_vendas))
    else:
        compras = abs(custo_vendas)
        if compras != 0:
            pmp = abs((passivo_fornecedores_medio/compras)*365)
        else:
            pmp = float('inf')
            metrics['PMPF'] = round(pmp, 0)




    # Ciclo Operacional
    ciclo_operacional = pme + pmr
    metrics['Ciclo_Operacional'] = round(ciclo_operacional, 0)



    # Ciclo de Caixa
    ciclo_de_caixa = ciclo_operacional - pmp
    metrics['Ciclo_de_Caixa'] = round(ciclo_de_caixa, 0)
    return metrics, pl_atual, estoque_atual, receb_atual, passivo_fornecedores_atual



def analises(year_range): 
   
    colunas_para_remover = ['CNPJ_CIA', 'VERSAO', 'DENOM_CIA', 'CD_CVM','GRUPO_DFP', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'ST_CONTA_FIXA']
    results = []
    pl_anterior = None
    estoque_anterior = None
    receb_anterior = None
    passivo_fornecedores_anterior = None
    for ano in year_range:
        df_ativos = carregar_data(ano, 'BPA', 'DFP', 'con', colunas_para_remover)
        df_passivos = carregar_data(ano, 'BPP', 'DFP', 'con', colunas_para_remover)
        df_receitas = carregar_data(ano, 'DRE', 'DFP', 'con', colunas_para_remover)
        df_despesas = carregar_data(ano, 'DRE', 'DFP', 'con', colunas_para_remover)
        df_depreciacao = carregar_data(ano, 'DRE', 'DFP', 'con', colunas_para_remover)
        metrics, pl_atual, estoque_atual, receb_atual, passivo_fornecedores_atual = calculo(df_ativos, df_passivos, df_receitas, df_despesas, df_depreciacao, pl_anterior,
        estoque_anterior, receb_anterior, passivo_fornecedores_anterior)
        metrics['Ano'] = ano
        results.append(metrics)
        pl_anterior = pl_atual
        estoque_anterior = estoque_atual
        receb_anterior = receb_atual
        passivo_fornecedores_anterior = passivo_fornecedores_atual
        return pd.DataFrame(results).set_index('Ano')
    

def DuPont_Tradicional(year_range): 
    Dupont = pd.DataFrame()
    colunas_para_remover = ['CNPJ_CIA', 'VERSAO', 'DENOM_CIA', 'CD_CVM','GRUPO_DFP', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'ST_CONTA_FIXA']
    pl_anterior = None
    ativo_anterior = None
    for ano in year_range:
        df_ativos = carregar_data(ano, 'BPA', 'DFP', 'con', colunas_para_remover)
        df_passivos = carregar_data(ano, 'BPP', 'DFP', 'con', colunas_para_remover)
        df_receitas = carregar_data(ano, 'DRE', 'DFP', 'con', colunas_para_remover)
        lucro_liquido = df_receitas.loc[df_receitas['DS_CONTA'].str.contains('Lucro/Prejuízo Consolidado do Período', case=False), 'VL_CONTA'].sum()
        receita_liquida = df_receitas[df_receitas['DS_CONTA'].str.contains('Receita de Venda de Bens e/ou Serviços', case=False, na=False)]['VL_CONTA'].sum()
        ativo_atual = df_ativos[df_ativos['DS_CONTA'].str.contains('AtivoTotal')]['VL_CONTA'].sum()
        if ativo_anterior is not None:
            ativo_total_medio = (ativo_anterior + ativo_atual) / 2
        else:
            ativo_total_medio = ativo_atual
        pl_atual = df_passivos[df_passivos['DS_CONTA'].str.contains('Patrimônio LíquidoConsolidado')]['VL_CONTA'].sum()
        if pl_anterior is not None:
            pl_medio = (pl_anterior + pl_atual) / 2
        else:
            pl_medio = pl_atual
        margem_liquida = (lucro_liquido / receita_liquida)*100 if receita_liquida else 0
        giro_do_ativo = receita_liquida / ativo_total_medio if ativo_total_medio else 0
        roa_errado = margem_liquida * giro_do_ativo
        alavancagem = ativo_total_medio / pl_medio if pl_medio else 0
        roe = roa_errado * alavancagem
        DuPont2 = pd.DataFrame({
                                'Ano': [ano],
                                'Margem Líquida DuPont T': [margem_liquida],
                                'Giro do Ativo DuPont T': [giro_do_ativo],
                                'ROA DuPont T': [roa_errado],
                                'Alavancagem DuPont T': [alavancagem],
                                'ROE DuPont T': [roe]
                                })
        Dupont = pd.concat([Dupont, DuPont2], ignore_index=True)
        ativo_anterior = ativo_atual
        pl_anterior = pl_atual
        Dupont.set_index('Ano', inplace=True)
        return Dupont
    
def DuPont_Ajustada(year_range): 
    DuPont_Ajustada = pd.DataFrame()
    colunas_para_remover = ['CNPJ_CIA', 'VERSAO', 'DENOM_CIA', 'CD_CVM','GRUPO_DFP', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'ST_CONTA_FIXA']
    pl_anterior = None
    ativo_liquido_anterior = None
    Passivo_financeiro_anterior = None
    for ano in year_range:
        df_passivos = carregar_data(ano, 'BPP', 'DFP', 'con', colunas_para_remover)
        df_receitas = carregar_data(ano, 'DRE', 'DFP', 'con', colunas_para_remover)
        lucro_liquido = df_receitas.loc[df_receitas['DS_CONTA'].str.contains('Lucro/Prejuízo Consolidado do Período', case=False), 'VL_CONTA'].sum()
        receita_liquida = df_receitas[df_receitas['DS_CONTA'].str.contains('Receita de Venda de Bens e/ou Serviços', case=False, na=False)]['VL_CONTA'].sum()
        divida_bruta = df_passivos[df_passivos['DS_CONTA'].str.contains('Empréstimos e Financiamentos')]['VL_CONTA'].sum()
        pl_atual = df_passivos[df_passivos['DS_CONTA'].str.contains('Patrimônio Líquido Consolidado')]['VL_CONTA'].sum()
        if pl_anterior is not None:
            pl_medio = (pl_anterior + pl_atual) / 2
        else:
            pl_medio = pl_atual
        ativo_liquido_atual = pl_atual + divida_bruta
        if ativo_liquido_anterior is not None:
            ativo_liquido_medio = (ativo_liquido_anterior + ativo_liquido_atual) / 2
        else:
            ativo_liquido_medio = ativo_liquido_atual
        if Passivo_financeiro_anterior is not None:
            Passivo_financeiro_medio = (divida_bruta+Passivo_financeiro_anterior)/2
        else:
            Passivo_financeiro_medio = divida_bruta 
        despesas_financeiras_liquida =df_receitas[df_receitas['DS_CONTA'].str.contains('Despesas Financeiras')]['VL_CONTA'].sum()*(1-0.34)
        lucro_do_ativo = lucro_liquido - despesas_financeiras_liquida
        margem_liquida_ajustada = (lucro_do_ativo/receita_liquida)*100
        giro_do_ativo_liquido = receita_liquida/ativo_liquido_medio
        roic = lucro_do_ativo/ativo_liquido_medio*100
        KD = abs((despesas_financeiras_liquida/Passivo_financeiro_medio)*100)
        alavancagem_com_divida = (Passivo_financeiro_medio/pl_medio)*100
        spread = roic - KD
        contribuicao_da_alavancagem = spread * alavancagem_com_divida/100
        roe = roic + contribuicao_da_alavancagem
        ativo_liquido = pl_atual + divida_bruta
        DuPont_Ajust2 = pd.DataFrame({
                                        'Ano': [ano],
                                        'Lucro do Ativo DuPont A': [lucro_do_ativo],
                                        'Ativo Liquido DuPont A': [ativo_liquido],
                                        'Margem Liquida Ajustada DuPont A': [margem_liquida_ajustada],
                                        'Giro do Ativo Liquido DuPont A': [giro_do_ativo_liquido],
                                        'ROIC DuPont A': [roic],
                                        'Custo da Dívida DuPont A': [KD],
                                        'Spread DuPont A': [spread],
                                        'Alavancagem com dívida DuPont A': [alavancagem_com_divida],
                                        'Contribuição da Alavancagem DuPont A': [contribuicao_da_alavancagem],
                                        'ROE DuPont A': [roe]})
        DuPont_Ajustada = pd.concat([DuPont_Ajustada, DuPont_Ajust2], ignore_index=True)
        pl_anterior = pl_atual
        ativo_liquido_anterior = ativo_liquido_atual
        Passivo_financeiro_anterior = divida_bruta
        DuPont_Ajustada.set_index('Ano', inplace = True)
        return DuPont_Ajustada
    


def graficos(year_range): 
    financial_data = analises(year_range)
    for column in financial_data.columns:
        plt.figure(figsize=(10, 5))
        plt.plot(financial_data.index, financial_data[column], marker='o', linestyle='-')
        plt.title(column)
        plt.xlabel('Year')
        plt.ylabel(column)
        plt.grid(True)
        plt.show()
    for column in DuPont_Tradicional.columns:
        plt.figure(figsize=(10, 5))
        plt.plot(DuPont_Tradicional.index, DuPont_Tradicional[column], marker='o',
        linestyle='-')
        plt.title(column)
        plt.xlabel('Year')
        plt.ylabel(column)
        plt.grid(True)
        plt.show()
    for column in DuPont_Ajustada.columns:
        plt.figure(figsize=(10, 5))
        plt.plot(DuPont_Ajustada.index, DuPont_Ajustada[column], marker='o', linestyle='-')
        plt.title(column)
        plt.xlabel('Year')
        plt.ylabel(column)
        plt.grid(True)
        plt.show()


BPAs = {} 
year_range = range(2017, 2024) 
    
for year in year_range: 
    BPA = relatorio_cias_abertas(year, 'BPA', 'DFP', 'con')
    BPA = BPA.loc[BPA['CD_CVM'] == '022470']
    BPA.drop(columns=['CNPJ_CIA', 'VERSAO', 'DENOM_CIA', 'CD_CVM','GRUPO_DFP', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'ST_CONTA_FIXA'],inplace=True)
    BPA.set_index(['DT_REFER'], inplace=True)
    BPA = BPA.loc[BPA['VL_CONTA'] != '0.0000000000']
    BPA = BPA.sort_values(by='DT_FIM_EXERC')
    BPA['VL_CONTA'] = BPA['VL_CONTA'].apply(pd.to_numeric, errors='coerce')
    BPA['VL_CONTA'] = BPA['VL_CONTA'].round(0)
    BPAs[year] = BPA
    del BPA
    BPPs = {} 
    for year in year_range: 
        BPP = relatorio_cias_abertas(year, 'BPP', 'DFP', 'con')
        BPP = BPP.loc[BPP['CD_CVM'] == '022470']
        BPP.drop(columns=['CNPJ_CIA', 'VERSAO', 'DENOM_CIA', 'CD_CVM',
        'GRUPO_DFP', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'ST_CONTA_FIXA'],
        inplace=True)
        BPP.set_index(['DT_REFER'], inplace=True)
        BPP = BPP.loc[BPP['VL_CONTA'] != '0.0000000000']
        BPP = BPP.sort_values(by='DT_FIM_EXERC')
        BPP['VL_CONTA'] = BPP['VL_CONTA'].apply(pd.to_numeric, errors='coerce')
        BPP['VL_CONTA'] = BPP['VL_CONTA'].round(0)
        BPPs[year] = BPP
        del BPP


DFC_MIs = {}

for year in year_range:
    DFC_MI = relatorio_cias_abertas(year, 'DFC_MI', 'DFP', 'con')
    DFC_MI = DFC_MI.loc[DFC_MI['CD_CVM'] == '022470']
    DFC_MI.drop(columns=['CNPJ_CIA', 'VERSAO', 'DENOM_CIA', 'CD_CVM','GRUPO_DFP', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'ST_CONTA_FIXA'],inplace=True)
    DFC_MI.set_index(['DT_REFER'], inplace=True)
    DFC_MI = DFC_MI.loc[DFC_MI['VL_CONTA'] != '0.0000000000']
    DFC_MI = DFC_MI.sort_values(by='DT_FIM_EXERC')
    DFC_MI['VL_CONTA'] = DFC_MI['VL_CONTA'].apply(pd.to_numeric, errors='coerce')
    DFC_MI['VL_CONTA'] = DFC_MI['VL_CONTA'].round(0)
    DFC_MIs[year] = DFC_MI
    del DFC_MI
DREs = {}


for year in year_range:
    DRE = relatorio_cias_abertas(year, 'DRE', 'DFP', 'con')
    DRE = DRE.loc[DRE['CD_CVM'] == '022470']
    DRE.drop(columns=['CNPJ_CIA', 'VERSAO', 'DENOM_CIA', 'CD_CVM','GRUPO_DFP', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'ST_CONTA_FIXA'], inplace=True)
    DRE.set_index(['DT_REFER'], inplace=True)
    DRE = DRE.loc[DRE['VL_CONTA'] != '0.0000000000']
    DRE = DRE.sort_values(by='DT_FIM_EXERC')
    DRE['VL_CONTA'] = DRE['VL_CONTA'].apply(pd.to_numeric, errors='coerce')
    DRE['VL_CONTA'] = DRE['VL_CONTA'].round(0)
    DREs[year] = DRE
    del DRE


DuPont_Tradicional = DuPont_Tradicional(year_range) 
DuPont_Ajustada = DuPont_Ajustada(year_range)
Indicadores = analises(year_range) 
graficos(year_range) 
del year
del year_range


