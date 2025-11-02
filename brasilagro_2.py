import re
import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile
import matplotlib.pyplot as plt

# =========================================================
# CONFIGURAÇÕES
# =========================================================
CODIGO_CVM = "20036"        # BrasilAgro
ANOS = range(2022, 2024)    # ajuste aqui os anos que quer puxar
USA_ITR = False             # se True, pega ITR; se False, pega DFP


# =========================================================
# 1. FUNÇÕES DE DOWNLOAD (SITE DA CVM)
# =========================================================
def montar_url_cvm(ano: int, tipo_periodo: str) -> str:
    """
    tipo_periodo: 'DFP' ou 'ITR'
    Estrutura atual da CVM (dados abertos):
    https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2024.zip
    https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_2024.zip
    """
    tipo_lower = tipo_periodo.lower()
    return f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo_periodo}/DADOS/{tipo_lower}_cia_aberta_{ano}.zip"


def read_csv_from_zip(url: str, nome_arquivo: str, sep=";") -> pd.DataFrame:
    """
    Faz o download do ZIP da CVM e abre somente o CSV pedido.
    """
    resp = requests.get(url)
    resp.raise_for_status()
    zf = ZipFile(BytesIO(resp.content))
    with zf.open(nome_arquivo) as f:
        # os CSVs da CVM vêm em ISO-8859-1
        linhas = [i.strip().decode("ISO-8859-1") for i in f.readlines()]
    valores = [l.replace("\n", "").strip().split(sep) for l in linhas]
    df = pd.DataFrame(valores[1:], columns=valores[0])
    return df


def carregar_demonstrativo(ano: int,
                           demonstrativo: str,   # 'BPA', 'BPP', 'DRE', 'DFC_MI', ...
                           tipo_periodo: str,    # 'DFP' ou 'ITR'
                           tipo_demo: str,       # 'con' ou 'ind'
                           cod_cvm: str = CODIGO_CVM,
                           colunas_para_remover=None) -> pd.DataFrame:
    """
    Baixa do site da CVM e já filtra pela companhia.
    """
    if colunas_para_remover is None:
        colunas_para_remover = [
            "CNPJ_CIA", "VERSAO", "DENOM_CIA", "CD_CVM",
            "GRUPO_DFP", "ESCALA_MOEDA", "ORDEM_EXERC", "ST_CONTA_FIXA"
        ]

    url = montar_url_cvm(ano, tipo_periodo)
    tipo_lower = tipo_periodo.lower()
    # exemplo de nome de arquivo:
    # dfp_cia_aberta_DRE_con_2024.csv
    nome_arquivo = f"{tipo_lower}_cia_aberta_{demonstrativo}_{tipo_demo}_{ano}.csv"

    df = read_csv_from_zip(url, nome_arquivo)

    # filtra companhia
    df = df[df["CD_CVM"] == cod_cvm]

    # limpa colunas
    for c in colunas_para_remover:
        if c in df.columns:
            df.drop(columns=c, inplace=True)

    # converte datas e valores
    if "DT_FIM_EXERC" in df.columns:
        df["DT_FIM_EXERC"] = pd.to_datetime(df["DT_FIM_EXERC"], errors="coerce")
        df = df[df["DT_FIM_EXERC"].dt.year == ano]

    if "VL_CONTA" in df.columns:
        df["VL_CONTA"] = pd.to_numeric(df["VL_CONTA"], errors="coerce").fillna(0)
        df = df[df["VL_CONTA"] != 0]

    return df


# =========================================================
# 2. CÁLCULO DOS INDICADORES (MESMA LÓGICA)
# =========================================================
def calcular_indicadores(df_ativos, df_passivos, df_dre,
                         pl_ant, estoque_ant, receb_ant, forn_ant):
    metrics = {}

    # PL
    pl_atual = df_passivos[df_passivos["DS_CONTA"]
                           .str.contains("Patrimônio Líquido", case=False, na=False)]["VL_CONTA"].sum()
    pl_medio = (pl_ant + pl_atual) / 2 if pl_ant is not None else pl_atual

    # Ativo circulante / Caixa / Estoque / Contas a receber
    ativo_circ = df_ativos[df_ativos["DS_CONTA"]
                           .str.contains("Ativo Circulante", case=False, na=False)]["VL_CONTA"].sum()
    caixa = df_ativos[df_ativos["DS_CONTA"]
                      .str.contains("Caixa e Equivalentes", case=False, na=False)]["VL_CONTA"].sum()
    estoque_atual = df_ativos[df_ativos["DS_CONTA"]
                              .str.contains("Estoque", case=False, na=False)]["VL_CONTA"].sum()
    estoque_medio = (estoque_ant + estoque_atual) / 2 if estoque_ant is not None else estoque_atual
    receb_atual = df_ativos[df_ativos["DS_CONTA"]
                            .str.contains("Contas a Receber|Clientes", case=False, na=False)]["VL_CONTA"].sum()
    receb_medio = (receb_ant + receb_atual) / 2 if receb_ant is not None else receb_atual

    # Passivo
    passivo_circ = df_passivos[df_passivos["DS_CONTA"]
                               .str.contains("Passivo Circulante", case=False, na=False)]["VL_CONTA"].sum()
    passivo_total = df_passivos[df_passivos["DS_CONTA"]
                                .str.contains("Passivo", case=False, na=False)]["VL_CONTA"].sum()

    # Fornecedores
    forn_df = df_passivos[df_passivos["DS_CONTA"].str.contains("Fornecedores", case=False, na=False)]
    if not forn_df.empty:
        forn_atual = forn_df["VL_CONTA"].sum()
    else:
        forn_atual = df_passivos.loc[df_passivos["CD_CONTA"] == "2.01.02", "VL_CONTA"].sum()
    forn_medio = (forn_ant + forn_atual) / 2 if forn_ant is not None else forn_atual

    # Liquidezes
    if passivo_circ != 0:
        metrics["liq_corrente"] = round(ativo_circ / passivo_circ, 2)
        metrics["liq_imediata"] = round(caixa / passivo_circ, 2)
        metrics["liq_seca"] = round((ativo_circ - estoque_atual) / passivo_circ, 2)
    else:
        metrics["liq_corrente"] = metrics["liq_imediata"] = metrics["liq_seca"] = float("inf")

    # DRE: receita, custo, despesa, lucro
    df_dre["VL_CONTA"] = pd.to_numeric(df_dre["VL_CONTA"], errors="coerce").fillna(0)
    receita = df_dre[df_dre["DS_CONTA"].str.contains("Receita de Venda", case=False, na=False)]["VL_CONTA"].sum()
    custo = df_dre[df_dre["DS_CONTA"].str.contains("Custo dos Bens|Custo dos Serviços", case=False, na=False)]["VL_CONTA"].sum()
    outras_desp = df_dre[df_dre["DS_CONTA"].str.contains("Despesa", case=False, na=False)]["VL_CONTA"].sum()
    lucro_liq = df_dre[df_dre["DS_CONTA"].str.contains("Lucro/Prejuízo do Período", case=False, na=False)]["VL_CONTA"].sum()

    # EBIT (ajuste simples: receita + custo - despesas)
    ebit = receita + custo - outras_desp
    metrics["EBIT"] = round(ebit, 2)

    # Depreciação / Amortização
    dep = df_dre[df_dre["DS_CONTA"].str.contains("Deprecia|Amortiza", case=False, na=False)]["VL_CONTA"].sum()
    metrics["EBITDA"] = round(ebit - dep, 2)

    # Margens
    if receita != 0:
        metrics["margem_bruta"] = round(((receita + custo) / receita) * 100, 1)
        metrics["margem_ebit"] = round((ebit / receita) * 100, 1)
        metrics["margem_liq"] = round((lucro_liq / receita) * 100, 1)
    else:
        metrics["margem_bruta"] = metrics["margem_ebit"] = metrics["margem_liq"] = 0

    # ROE
    metrics["ROE"] = round((lucro_liq / pl_medio) * 100, 1) if pl_medio != 0 else 0

    # PMRE (estoque)
    if custo != 0:
        pmre = (estoque_medio / abs(custo)) * 365
    else:
        pmre = float("inf")
    metrics["PMRE"] = round(pmre, 0)

    # PMRV (recebíveis)
    if receita != 0:
        pmrv = (receb_medio / receita) * 365
    else:
        pmrv = float("inf")
    metrics["PMRV"] = round(pmrv, 0)

    # Compras p/ PMPF
    if estoque_ant is not None:
        compras = (estoque_atual - estoque_ant + abs(custo))
    else:
        compras = abs(custo)
    if compras != 0:
        pmpf = abs((forn_medio / compras) * 365)
    else:
        pmpf = float("inf")
    metrics["PMPF"] = round(pmpf, 0)

    # Ciclos
    ciclo_op = pmre + pmrv
    metrics["ciclo_operacional"] = round(ciclo_op, 0)
    metrics["ciclo_caixa"] = round(ciclo_op - pmpf, 0)

    # devolve também os saldos pra usar de base no ano seguinte
    return metrics, pl_atual, estoque_atual, receb_atual, forn_atual


# =========================================================
# 3. LOOP PRINCIPAL
# =========================================================
def rodar_analise(anos=ANOS, cod_cvm=CODIGO_CVM, usar_itr=USA_ITR):
    tipo = "ITR" if usar_itr else "DFP"
    resultados = []
    pl_ant = estoque_ant = receb_ant = forn_ant = None

    for ano in anos:
        df_bpa = carregar_demonstrativo(ano, "BPA", tipo, "con", cod_cvm=cod_cvm)
        df_bpp = carregar_demonstrativo(ano, "BPP", tipo, "con", cod_cvm=cod_cvm)
        df_dre = carregar_demonstrativo(ano, "DRE", tipo, "con", cod_cvm=cod_cvm)

        metrics, pl_ant, estoque_ant, re
        metrics, pl_ant, estoque_ant, receb_ant, forn_ant = calcular_indicadores(
            df_bpa, df_bpp, df_dre, pl_ant, estoque_ant, receb_ant, forn_ant
        )