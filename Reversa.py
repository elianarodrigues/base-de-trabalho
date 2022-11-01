import pyautogui
import os
from selenium import webdriver
import pandas as pd
from datetime import datetime, timedelta
import time
from workalendar.america import BrazilBankCalendar
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import numpy as np
import shutil
import subprocess
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
import pymysql

cal = BrazilBankCalendar()
cal.include_ash_wednesday = False                       #tirar a quarta de cinzas, pra gente é dia normal
cal.include_christmas = True                            # considera natal como feriado
cal.include_christmas_eve = True                        # considera natal como feriado
cal.include_new_years_day = True                        # considera ano novo como feriado
cal.include_new_years_eve = False                        # considera ano novo como feriado

print(datetime.today().strftime('%H:%M'))
start_time = time.time()

#######################################################################################################################
#                                                   FUNÇÕES DEF
#######################################################################################################################

def diaUtil_entredatas(menordia, maiordia):
    a = menordia
    b = maiordia
    resp = blank
    if pd.isnull(menordia) or pd.isnull(maiordia):
        return blank
    else:
        a = pd.to_datetime(menordia, infer_datetime_format=True, dayfirst=True)
        b = pd.to_datetime(maiordia, infer_datetime_format=True, dayfirst=True)
        if b.weekday() in [5, 6]:
            if b > a:
                resp = (cal.get_working_days_delta(a, b))+1
            else:
                resp = (cal.get_working_days_delta(a, b))
                resp = resp * -1
        else:
            resp = (cal.get_working_days_delta(a, b))
            if a > b:
                return -resp
    return resp

def ajustaLetras(base):
    baseajustada = base
    time.sleep(5)
    baseajustada = baseajustada.replace('á', 'a', regex=True)
    baseajustada = baseajustada.replace('Á', 'A', regex=True)
    baseajustada = baseajustada.replace('ã', 'a', regex=True)
    baseajustada = baseajustada.replace('Ã', 'A', regex=True)
    baseajustada = baseajustada.replace('â', 'a', regex=True)
    baseajustada = baseajustada.replace('Â', 'A', regex=True)
    baseajustada = baseajustada.replace('à', 'a', regex=True)
    baseajustada = baseajustada.replace('À', 'A', regex=True)
    baseajustada = baseajustada.replace('é', 'e', regex=True)
    baseajustada = baseajustada.replace('ê', 'e', regex=True)
    baseajustada = baseajustada.replace('É', 'E', regex=True)
    baseajustada = baseajustada.replace('Ê', 'E', regex=True)
    baseajustada = baseajustada.replace('í', 'i', regex=True)
    baseajustada = baseajustada.replace('î', 'i', regex=True)
    baseajustada = baseajustada.replace('Í', 'i', regex=True)
    baseajustada = baseajustada.replace('Î', 'i', regex=True)
    baseajustada = baseajustada.replace('Ó', 'O', regex=True)
    baseajustada = baseajustada.replace('Ô', 'O', regex=True)
    baseajustada = baseajustada.replace('Õ', 'O', regex=True)
    baseajustada = baseajustada.replace('ó', 'o', regex=True)
    baseajustada = baseajustada.replace('ô', 'o', regex=True)
    baseajustada = baseajustada.replace('õ', 'o', regex=True)
    baseajustada = baseajustada.replace('û', 'u', regex=True)
    baseajustada = baseajustada.replace('ú', 'u', regex=True)
    baseajustada = baseajustada.replace('Ú', 'U', regex=True)
    baseajustada = baseajustada.replace('Û', 'U', regex=True)
    baseajustada = baseajustada.replace('ç', 'c', regex=True)
    baseajustada = baseajustada.replace('Ç', 'C', regex=True)
    baseajustada = baseajustada.replace('ª', 'a', regex=True)
    baseajustada = baseajustada.replace('º', 'o', regex=True)

    return baseajustada


def agingcoleta(flagscoleta, dlcoleta, hoje, dlefetivada):
    if flagscoleta == "Coleta em aberto" and dlcoleta != blank:
        aging = diaUtil_entredatas(dlcoleta, hoje)
    elif flagscoleta == "Coletado":
	    aging = diaUtil_entredatas(dlefetivada, dlcoleta)
    else:
	    aging = blank
    return	aging

def agingdev(dlefetivada, cx, dldevolução, hoje):
    if pd.isnull(dlefetivada) and cx != "Coleta Realizada":
        agingdev = blank
    elif pd.isnull(dlefetivada) and cx == "Coleta Realizada":
        agingdev = ((dldevolução - hoje).days)
    else:
	    agingdev = ((dldevolução - dlefetivada).days)
    return	agingdev

def agingUlt(cx, ultat, dlefetivada):
    if cx == "Em Transferência":
        agingUlt = (ultat - dlefetivada).days
    else:
        agingUlt = blank
    return agingUlt

def data_limitedev(dia, prazo):
    try:
        if pd.isnull(dia):
            dl = pd.NaT

        else:
            dl = pd.to_datetime(dia) + timedelta(days=prazo)

        return dl
    except:
        return pd.NaT

def clusterDUTILColeta(flag, aging):
    if flag == "Coleta em aberto":
        if aging >= 1:
            return "No Prazo"
        elif aging == 0:
            return "Vence Hoje"
        elif aging < 0:
            return "Atraso"
    else:
        return blank_str

def clusterDUTILDev(cx, aging):
    if cx == "Coleta Realizada":
        if aging > 1:
            return "No Prazo"
        elif aging == 0:
            return "Vence Hoje"
        elif aging < 0:
            return "Atraso"
    else:
        return blank

def flagAtraso(cx, aging):
    if cx == "Coleta Pendente":
        if aging < 0:
            flag = 1
        else:
            flag = 0
    else:
        flag = blank
    return flag

def flagAtraso5(cx, aging):
    if cx == "Coleta Pendente":
        if aging <=-5:
            flag = 1
        else:
            flag = 0
    else:
        flag = blank
    return flag

def flagAtrasoDev(cx, aging):
    if cx == "Coleta Realizada":
        if aging < 0:
            flag = 1
        else:
            flag = 0
    else:
        flag = blank
    return flag

def flagSituacaoColeta(cx,dsoliticacao,dcefetivada,ddefetivada):
    if cx == "Coleta Pendente" or cx == "Problema de Coleta":
        flag = "Coleta em aberto"
    elif pd.isnull(dsoliticacao) and cx != "Solicitação de Devolução":
        flag = "Cancelado na solicitação"
    elif pd.isnull(dcefetivada) and not(pd.isnull(dsoliticacao)) and pd.isnull(ddefetivada):
        flag = "Cancelado na Coleta"
    elif not(pd.isnull(dcefetivada)) or not(pd.isnull(ddefetivada)):
        flag = "Coletado"
    else:
        flag = cx

    return flag

def slaColeta(dcefetivada, cx,dlcoleta, hoje):
    if pd.isnull(dcefetivada) and cx != "Coleta Pendente":
        slacol = blank
    elif pd.isnull(dcefetivada) and cx == "Coleta Pendente":
        a = pd.to_datetime(dlcoleta, infer_datetime_format=True, dayfirst=True)
        b = pd.to_datetime(hoje, infer_datetime_format=True, dayfirst=True)
        if a >= b:
            slacol = 1
        else:
             slacol = 0
    else:
        a = pd.to_datetime(dlcoleta, infer_datetime_format=True, dayfirst=True)
        b = pd.to_datetime(dcefetivada, infer_datetime_format=True, dayfirst=True)
        if a >= b:
            slacol = 1
        else:
            slacol = 0
    return slacol

def slaDev(ddefetivada, cx, dldev, hoje):
    if pd.isnull(ddefetivada) and cx != "Coleta Realizada":
        slaDev = blank
    elif pd.isnull(ddefetivada) and cx == "Coleta Realizada":
        a = pd.to_datetime(dldev, infer_datetime_format=True, dayfirst=True)
        b = pd.to_datetime(hoje, infer_datetime_format=True, dayfirst=True)
        if a >= b:
            slaDev = 1
        else:
             slaDev = 0
    else:
        a = pd.to_datetime(dldev, infer_datetime_format=True, dayfirst=True)
        b = pd.to_datetime(ddefetivada, infer_datetime_format=True, dayfirst=True)
        if a >= b:
            slaDev = 1
        else:
            slaDev = 0
    return slaDev

def slaColA(cx, dlcoleta, hoje):
    if cx != "Coleta Pendente":
        slacol = blank
    else:
        a = pd.to_datetime(dlcoleta, infer_datetime_format=True, dayfirst=True)
        b = pd.to_datetime(hoje, infer_datetime_format=True, dayfirst=True)
        if a >= b:
            slacol = 1
        else:
            slacol = 0
    return slacol

def slaDevAberto(ddefetivada, cx, dldev, hoje):
    if pd.isnull(ddefetivada) and cx == "Coleta Realizada":
            a = pd.to_datetime(dldev, infer_datetime_format=True, dayfirst=True)
            b = pd.to_datetime(hoje, infer_datetime_format=True, dayfirst=True)
            if a >= b:
                slaDevAberto = 1
            else:
                slaDevAberto = 0
    else:
        slaDevAberto = blank
    return slaDevAberto

def agingsolicitacao(cx, datasoli, datacria, hoje):
    if cx == "Excluido":
        aging = blank
    else:
        if pd.isnull(datasoli):
            a = pd.to_datetime(datacria, infer_datetime_format=True, dayfirst=True)
            b = pd.to_datetime(hoje, infer_datetime_format=True, dayfirst=True)
            aging = (cal.get_working_days_delta(a, b))

        else:
            a = pd.to_datetime(datacria, infer_datetime_format=True, dayfirst=True)
            b = pd.to_datetime(datasoli, infer_datetime_format=True, dayfirst=True)
            aging = (cal.get_working_days_delta(a, b))
    return aging

def chaveProblemaPV(dproblema,pv):
    if pd.isnull(dproblema):
        resp = str(pv).strip()
    else:
        resp = dproblema.strftime('%d%m%Y') + str(pv)
    return str(resp).strip()

def main():
    local = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\03. ReversaAuxiliares\Ocr - Reversa.xlsx'
    base_geral = pd.read_excel(local)
    # base_LU = pd.read_excel(local,sheet_name="Problemas - MagaLU")
    # base_Midias = pd.read_excel(local,sheet_name="Problemas - RAMidias")

    #num_colunas = base_geral.shape[1]
    #base_geral.insert(loc=num_colunas, column='Data_problema&PV', value=base_geral['Data do Problema de Coleta'].astype(str) + base_geral['PV'].astype(str))
    print(base_geral.shape[0], "linhas")
    print("Data_problema&PV", " - %s" % (time.time() - start_time), " - segundos")

    # base_geral = base_geral.append(base_LU)
    # base_geral = base_geral.append(base_Midias)

    # CRIA CHAVE
    base_geral['Data_problema&PV'] = base_geral.apply(lambda row: chaveProblemaPV(row['Data do Problema de Coleta'], row['PV']), axis=1)
    base_geral['Data_problema&PV'] = base_geral["Data_problema&PV"].astype(str)

    #RENOMEIA PV
    base_geral.rename(columns={"PV": "Problema_pedido"}, inplace=True)

    #CLASSIFICA DOS MAIS NOVOS PARA OS MAIS VELHOS
    base_geral = base_geral.sort_values(by=['Data do Problema de Coleta'], ascending=False)

    #DROPA OS PROBLEMAS REPETIDOS
    base_geral = base_geral.drop_duplicates(subset=['Data_problema&PV'], keep='first', ignore_index=True)

    # lista = ["Data do Problema de Coleta", "Data da Finalização Ocorrência", "Início Prazo Ativo", "Data 1a Tratativa",
    #          "Data Finalização Ativo"]
    # for d in lista:
    #     # pd.to_datetime(df['Dates'], format='%y%m%d')
    #     base_geral[d] = pd.to_datetime(base_geral[d], infer_datetime_format=True, dayfirst=True)


    base_geral.to_excel(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\03. ReversaAuxiliares\DOCS.xlsx', index=False)

    print('teste')
    return

def agingproblema (cx, tagplanilha, tagfinalizado, hoje, problinicio, problfim):
 try:
    if cx == "Problema de Coleta" and tagplanilha != "Novo":
        aging = blank
    else:
        if tagfinalizado == "Não":
            if pd.isnull(problinicio)or problinicio == 1239999:
                aging = blank
            else:
                a = pd.to_datetime(problinicio, infer_datetime_format=True, dayfirst=True)
                b = pd.to_datetime(hoje, infer_datetime_format=True, dayfirst=True)
                aging = (cal.get_working_days_delta(a, b))
        else:
            if pd.isnull(problfim) or problfim == 1239999:
                aging = blank
            else:
                a = pd.to_datetime(problfim, infer_datetime_format=True, dayfirst=True)
                b = pd.to_datetime(problinicio, infer_datetime_format=True, dayfirst=True)
                aging = (cal.get_working_days_delta(a, b))
    return aging
 except:
     return blank

# def agingproblema (cx, tagplanilha, tagfinalizado, hoje, problinicio, problfim):
#     if cx == "Problema de Coleta" and tagplanilha != "Novo":
#         if tagfinalizado == "Não":
#             aging = diaUtil_entredatas(problinicio, hoje)
#         else:
#             aging = diaUtil_entredatas(problfim, problinicio)
#     else:
#         aging = blank_str
#     return  aging

def problemainicio (cx,tagpro, iniciopz):
    if cx == "Problema de Coleta" and tagpro != "Novo":
        problemainicio = pd.to_datetime(iniciopz, infer_datetime_format=True, dayfirst=True)
    else:
        problemainicio = pd.NaT
    return problemainicio

def agingprobnovo (cx, tagplanilha, hoje, datacria):
    if cx == "Problema de Coleta" and tagplanilha == "Novo":
        agingprob = diaUtil_entredatas(datacria, hoje)
    else:
        agingprob = blank

    return agingprob

def stprotheus (cx, prtherro, prthstatus):
    if cx == 'Solicitação de devolução':
        if (prtherro == 1 or prthstatus) == "Erro":
            stprotheus = "ERRO PROTHEUS"
        elif prthstatus == "AGUA":
            stprotheus = "Aguardando Protheus"
        else:
            stprotheus = blank_str
    else:
        stprotheus = blank_str
    return stprotheus


def problemadepara (cx, tagplanilha, probposi, obs):
    if cx == 'Problema de Coleta' and tagplanilha != 'Novo':
        if pd.isnull(probposi):
            problema = "Aguardando retorno"
        elif probposi == "Cancelar coleta" and obs == "Aguar":
            problema = 'Cancelar Coleta - Aguardando TP'
        else:
            problema = probposi
    elif cx == 'Problema de Coleta' and tagplanilha == 'Novo':
        problema = "Novo"
    else:
        problema = blank_str
    return problema


def probreativar(cx, probdepara, obs, obs3):
    if cx == 'Problema de Coleta' and probdepara == "Coleta Agendada" and obs == 'Agendado: ':
            problema = obs3
    else:
        problema = pd.NaT
    return problema

def agingpconsolidado(cx, tgpro, agingnovo, posicaodepara, agingtotal, agingaten):
    if cx == 'Problema de Coleta':
        if tgpro == 'Novo':
            agingp = agingnovo
        elif posicaodepara == 'Cancelar Coleta - Aguardando TP' or posicaodepara == 'Cancelar coleta' or posicaodepara == 'Coleta Agendada' or posicaodepara == 'Solução Interna':
            return agingtotal
        else:
            agingp = agingaten
    else:
        agingp = blank
    return agingp

def transpCep(cep):
    aruja = 'Bulky Arujá'
    cajamar = 'CAJAMAR - BULKY'
    if cep == blank:
        return blank_str
    if 1000000 <= cep and 3999999 >= cep:
        return aruja
    elif 4000000 <= cep and 4499999 >= cep:
        return aruja
    elif 4500000 <= cep and 4799999 >= cep:
        return cajamar
    elif 4800000 <= cep and 4899999 >= cep:
        return cajamar
    elif 4900000 <= cep and 4999999 >= cep:
        return cajamar
    elif 5000000 <= cep and 5899999 >= cep:
        return cajamar
    elif 8000000 <= cep and 8499999 >= cep:
        return aruja
    else:
        transpCep = blank_str
    return transpCep

def tpSelecionada (selectp, tprev2, tprev1):
    if selectp != blank_str:
        return selectp
    elif tprev2 == blank_str:
            return tprev1
    else:
        tpselec = tprev2
    return tpselec

def slasolicitacao(aging):
    if pd.isnull(aging):
        sla = blank
    elif aging > 1:
        sla = 0
    else:
        sla = 1
    return sla

#[tag]Problema_novo
def tagProblemaNovo(cx,p_pv):


    if cx == "Problema de Coleta":
        if p_pv == blank_str:
            resp = "Novo"
        else:
            resp = "Planilha"
    else:
        resp = blank_str
    return resp

def transpDepara(tp,tp_parceiro):
    if tp == blank_str:
        resp = blank_str
    elif tp == "BULKY LOG - JUN":
        if tp_parceiro == blank_str:
            resp = "BULKY LOG - JUN"
        else:
            resp = tp_parceiro
    elif tp_parceiro != blank_str:
        resp = tp_parceiro
    else:
        resp = tp
    return resp

def statusVencimento(data_hoje, data_limite):
    try:

        if pd.isnull(data_limite):
            return "00. Sem Data"
        else:
            d_limite = pd.to_datetime(data_limite, infer_datetime_format=True, dayfirst=True)
            d_hoje = pd.to_datetime(data_hoje, infer_datetime_format=True, dayfirst=True)
            resp = (cal.get_working_days_delta(d_limite, d_hoje))
            if d_hoje > d_limite:
                resp = -resp
            semana_d_limite = int(d_limite.strftime("%U"))
            semana_d_hoje = int(d_hoje.strftime("%U"))
            # 0 é domingo e 6 é sabado
            weekday_limite = int(d_limite.strftime("%w"))
            weekday_hoje = int(d_hoje.strftime("%w"))

        if d_limite < (d_hoje):
            return "01. Atraso"
        # elif d_limite == (d_hoje - timedelta(days=1)):
        #    return "02. Atraso Ontem"
        elif d_limite == d_hoje:
            return "02. Vence Hoje"
        elif d_limite == (d_hoje + timedelta(days=1)):
            return "03. Vence Amanha"
        elif semana_d_limite == (semana_d_hoje):
            return "04. Vence Semana"
        elif (weekday_hoje >= 4 or weekday_hoje == 0) and weekday_limite == 1 and semana_d_limite == (semana_d_hoje + 1):
            return "05. Vence Segunda"
        elif semana_d_limite == 1  and semana_d_hoje == 52: ## feito para virada de ano
            return "06. Vence S+1"
        elif semana_d_limite == (semana_d_hoje + 1):
            return "06. Vence S+1"
        else:
            return "07. Vence S++"
    except:
        return pd.NaT

def transplm(tparceiro, tlm):
    if tparceiro == blank_str:
        return tlm
    else:
        return tparceiro

#######################################################################################################################
#                                                   FINS DEF                                                          #
#######################################################################################################################

#######################################################################################################################
#                                                  CONSULTA SQL                                                       #
#######################################################################################################################

print('inicia consulta sql', datetime.today().strftime('%H:%M'))
start_time = time.time()

user = 'eliana.rodrigues'
senha = 'wpRv%miB#zZlmbFvXW'
engine_string = 'mysql+pymysql://%s:%s@portal-ro3.madeiramadeira.com.br:3306/brain' % (user,senha)
engine_solicitacao = sqlalchemy.create_engine(engine_string)
query ='''
SELECT
    pv.cliente_cep AS CEP,
	crs.descricao AS Caixa,
	cct.descricao AS Caso_Critico,
	c.descricao AS Cidade,
	DATE(cr.data_entrega) AS Data_Bip_CD,
	DATE(cr.data_coleta) AS Data_Coleta_Efetivada,
	DATE(cr.data_criacao) AS Data_Criacao,
	DATE(cr.data_estimada_coleta) AS Data_Limite_Coleta,
	DATE(crp.data_criacao) As Data_Problema_Criacao,
	DATE(cr.data_solicitacao) AS Data_Solicitacao,
	c.uf AS Estado,
	cr.valor_frete AS Frete_Nota,
	cr.id as ID,
	if(g.descricao is null,"MadeiraMadeira",g.descricao) AS Marketplace,
	sm.descricao AS Motivo,
	cr.numero_nota AS NFD,
	cr.numero_nota_venda AS NF_venda,
	cr.idfk_pedido_venda AS PV,
    cr.peso AS Peso,
    ipv.preco_unitario AS Preco_un,
	-- crp.descricao as Problema_Descricao,
    rpm.descricao as Problema_Motivo,
	cr.erro_protheus as Protheus_erro,
	cr.retorno_recompra_protheus AS Protheus_status,
	cr.idfk_sac AS Reclamacao,
	rm.descricao AS Regiao_Entrega,
	cri.codigo_produto AS SKU,
    cr.serie_nota AS Serie_NFD,
	cr.serie_nota_venda as Serie_NFVenda,
	t.nome_reduzido AS Transportadora,
	DATE(cr.updated_at) AS Ultima_atualizacao,
	cr.valor_despesa + cr.valor_frete AS Valor_NFD,
	pv.cliente_telefone,
	cr.codigo_postagem,
	cri.quantidade AS qtd_SKU,
	refat.nota_fiscal_venda as refaturamento_NF,
	refat.finalizado as refaturamento_finalizado,
    replace(replace(crp.descricao,'\n',''),'\r','')  as Problema_Descricao
    FROM
	portal_coleta_reversa_item cri
		LEFT JOIN
	portal_coleta_reversa cr on cr.id = cri.idfk_coleta_reversa
		LEFT JOIN 
    portal_coleta_reversa_status crs on crs.id = cr.idfk_status
        LEFT JOIN
    portal_transportadora t ON cr.idfk_transportadora_filial = t.id
         INNER JOIN
    sac_item si ON si.idfk_item = cri.idfk_item_pedido_venda
        LEFT JOIN
    sac_reclamacao sr ON sr.id = cr.idfk_sac
        INNER JOIN
    portal_item_pedido_venda ipv ON si.idfk_item = ipv.id
        INNER JOIN
    portal_pedido_venda pv ON pv.id = ipv.idfk_pedido_venda
        LEFT JOIN
    portal_market_place_loja l ON l.id = pv.idfk_loja
        LEFT JOIN
    portal_market_place_grupo g ON g.id = l.idfk_grupo
        LEFT JOIN
    sac_reclamacao_caso_critico cc ON cc.idfk_reclamacao = sr.id
        LEFT JOIN
    sac_reclamacao_caso_critico_tipo AS cct ON cct.id = cc.idfk_tipo
        LEFT JOIN
    portal_cidade c ON c.id = pv.idfk_cidade
        LEFT JOIN
    sac_motivo sm ON sm.id = sr.idfk_motivo
        LEFT JOIN
    portal_regiao_micro AS rm ON rm.id = c.idfk_regiao_micro
        LEFT JOIN
    portal_item_faturamento AS ifat ON ifat.idfk_item_pedido_venda = ipv.id
        LEFT JOIN
    portal_faturamento AS fat ON fat.id = ifat.idfk_faturamento
        LEFT JOIN
	portal_coleta_reversa_refaturamento as refat on refat.idfk_coleta_reversa = cr.id
		LEFT JOIN
    portal_filial_madeira AS pfm ON pfm.id = fat.idfk_filial_madeira
		LEFT JOIN 
	portal_coleta_reversa_problema crp ON crp.idfk_coleta_reversa = cr.id
		AND crp.id = 
		(
           SELECT MAX(id)
           FROM portal_coleta_reversa_problema crpm
           WHERE crpm.idfk_coleta_reversa = crp.idfk_coleta_reversa
        )
		LEFT JOIN
	portal_reversa_problema_motivo rpm on crp.idfk_problema_motivo = rpm.id
WHERE
    cr.idfk_status IN (1, 2, 3, 5, 4, 6, 7, 8, 10)
        AND cr.idfk_tipo = 1
        AND (cr.data_solicitacao >= DATE('20210101') OR cr.data_coleta >= DATE('20210101') OR cr.data_entrega >= DATE('20210101') OR DATE(cr.data_criacao) >= DATE('20210101'))
		AND crs.descricao = "Coleta Pendente" OR crs.descricao = "Problema de Coleta" OR crs.descricao = "Coleta Realizada"
GROUP BY cri.id
ORDER BY cr.data_criacao DESC
'''
solicitacao = pd.read_sql_query(query,engine_solicitacao)
reversa_df = solicitacao.copy()
del solicitacao

#df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Reversa_data.csv', sep=';', index=False)
print(reversa_df.head())

print('fim consulta sql', datetime.today().strftime('%H:%M'))
start_time = time.time()

#######################################################################################################################
#                                                  CONSULTA SQL                                                       #
#######################################################################################################################

endereco = r'C:\Users\eliana.rodrigues\Downloads'

main()

#######################################################################################################################
#                                                     FIM CONSULTA SQL                                                #
#######################################################################################################################

#######################################################################################################################
#                                                       LÊ DF REVERSA
#######################################################################################################################


print(reversa_df[reversa_df['CEP'].isnull()])
reversa_df['CEP'] = pd.to_numeric(reversa_df['CEP'], errors='coerce')
reversa_df = reversa_df.dropna(subset=['CEP'])
reversa_df['CEP'] = reversa_df['CEP'].astype(int)

# reversa_df['CEP'] = reversa_df['CEP'].replace('X', int(0), regex=True)
# reversa_df["CEP"] = reversa_df["CEP"].astype(int)

reversa_df = reversa_df.convert_dtypes()
reversa_df = reversa_df.convert_dtypes()


time.sleep(30)

blank = 1239999
blank_str = '1239999'

#######################################################################################################################
#                                                  CONVERTE COLUNAS EM DATAS                                          #
#######################################################################################################################

d = datetime.today().strftime('%d/%m/%y')
reversa_df['Hoje'] = d

lista = ["Data_Criacao", "Data_Solicitacao", "Data_Coleta_Efetivada","Data_Limite_Coleta",
         "Ultima_atualizacao", "Data_Problema_Criacao", "Data_Bip_CD", "Hoje"]
for d in lista:
    # pd.to_datetime(df['Dates'], format='%y%m%d')
    reversa_df[d] = pd.to_datetime(reversa_df[d], infer_datetime_format=True, dayfirst=True)

linhas = reversa_df.shape[0]
linhas = int(linhas) - 1

#######################################################################################################################
#                                                   COMEÇA MERGES
#######################################################################################################################


print('Iniciando PROC')

reversa_df['Cidade'] = reversa_df['Cidade'].str.upper()

#criando coluna Chave PV+SKU
num_colunas = reversa_df.shape[1]
reversa_df.insert(loc=num_colunas, column='Chave PV+SKU', value=reversa_df['PV'].astype(str) + reversa_df['SKU'].astype(str))
print(reversa_df.shape[0], "linhas")
print("Chave PV+SKU", " - %s" % (time.time() - start_time), " - segundos" )


#criando coluna Chave ID+SKU
num_colunas = reversa_df.shape[1]
reversa_df.insert(loc=num_colunas, column='Chave ID+SKU', value=reversa_df['ID'].astype(str) + reversa_df['SKU'].astype(str))
print(reversa_df.shape[0], "linhas")
print("Chave ID+SKU", " - %s" % (time.time() - start_time), " - segundos" )

#criando coluna Chave DataProblema + PV
reversa_df['Data_problema&PV'] = reversa_df.apply(lambda row: chaveProblemaPV(row['Data_Problema_Criacao'],row['PV']),axis=1)
reversa_df['Data_problema&PV'] = reversa_df['Data_problema&PV'].astype(str)
print("DataProblema + PV", " - %s" % (time.time() - start_time), " - segundos" )


#criando coluna Chave Parceiros
num_colunas = reversa_df.shape[1]
reversa_df.insert(loc=num_colunas, column='Chave_Parceiros', value=reversa_df['Transportadora'].astype(str) + reversa_df['Cidade'].astype(str) + reversa_df['Estado'].astype(str))
print(reversa_df.shape[0], "linhas")
print("Chave Parceiros", " - %s" % (time.time() - start_time), " - segundos" )


#PROC PrazoDevolução
list = ['Estado', 'Prazo Devolucao']
prazoDev = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\03. ReversaAuxiliares\PrazoDevolução.csv", sep=';', usecols=list)
reversa_df = pd.merge(reversa_df, prazoDev, on='Estado', how='left')
print("PROC PrazoDevolução", " - %s" % (time.time() - start_time), " - segundos" )

#PROC Base Doc's
list = ['Data do Problema de Coleta', 'Finalizado?', 'Data da Finalização Ocorrência', 'Responsável Reversa', 'Problema_pedido', 'Tratativa', 'Observação', 'Início Prazo Ativo', 'Aging Atend.', 'Posição Final', 'Data Finalização Ativo', 'Responsável Ativo', 'Data_problema&PV']
docs = pd.read_excel(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\03. ReversaAuxiliares\DOCS.xlsx", usecols=list)
reversa_df = pd.merge(reversa_df, docs, on='Data_problema&PV', how='left')
print("PROC Base Doc's", " - %s" % (time.time() - start_time), " - segundos" )

print(reversa_df.dtypes)



#Proc com Parceiros_Bulky)
list = ['Chave_Parceiros', 'Transp Reversa']
parceiros_bulky_df = pd.read_excel(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\03. ReversaAuxiliares\Transp_Parceiros.xlsx", usecols=list)

parceiros_bulky_df.rename(columns={"Transp Reversa": "Transp_parceiros"}, inplace=True)

reversa_df = pd.merge(reversa_df, parceiros_bulky_df, on='Chave_Parceiros', how='left')
print(reversa_df['Transp_parceiros'])



reversa_df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\reversa_teste.csv', sep=';')


#chave para proc Transp_Cidade
num_colunas = reversa_df.shape[1]
reversa_df.insert(loc=num_colunas, column='chave_ufcidade', value=(reversa_df['Estado'].astype(str) + reversa_df['Cidade'].astype(str)))
reversa_df['chave_ufcidade'] = reversa_df['chave_ufcidade'].str.upper()


#Proc com Transp_Cidade
list = ['chave_ufcidade','Transp Reversa2']
parceiros_bulky_df = pd.read_excel(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\03. ReversaAuxiliares\Transp_Cidade.xlsx",usecols=list)
parceiros_bulky_df['chave_ufcidade'] = parceiros_bulky_df['chave_ufcidade'].str.upper()
reversa_df = pd.merge(reversa_df, parceiros_bulky_df, on='chave_ufcidade', how='left')

#Proc com Transp_Estado
list = ['Estado', 'Transp Reversa1']
parceiros_bulky_df = pd.read_excel(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\03. ReversaAuxiliares\Transp_Estado.xlsx", usecols=list)
parceiros_bulky_df['Estado'] = parceiros_bulky_df['Estado'].str.upper()
reversa_df = pd.merge(reversa_df, parceiros_bulky_df, on='Estado', how='left')


#criando coluna 'Chave Pedido e Nota'
num_colunas = reversa_df.shape[1]
reversa_df.insert(loc=num_colunas, column='Chave Pedido e Nota', value=(reversa_df['PV'].astype(str) + reversa_df['NF_venda'].astype(str)))
reversa_df['Chave Pedido e Nota'] = reversa_df['Chave Pedido e Nota'].str.upper()

#Proc - diário de bordo (reversa)
list = ['Chave', 'Data de Entrada', 'Caixa Mudou?', 'Tratativa', 'Motivo', 'Observação', 'Vencimento']
base_diario = pd.read_excel(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Rotina_Atualização_Geral.xlsx", sheet_name="Reversa" , usecols=list)
base_diario.rename(columns = {'Chave': 'Chave Pedido e Nota' }, inplace=True)
base_diario.rename(columns = {'Data de Entrada': 'Data de Entrada - Diario' }, inplace=True)
base_diario.rename(columns = {'Caixa Mudou?': 'Caixa Mudou? - Diario' }, inplace=True)
base_diario.rename(columns = {'Tratativa': 'Tratativa - Diario' }, inplace=True)
base_diario.rename(columns = {'Motivo': 'Motivo - Diario' }, inplace=True)
base_diario.rename(columns = {'Observação': 'Observacao - Diario' }, inplace=True)
base_diario.rename(columns = {'Vencimento': 'Vencimento - Diario' }, inplace=True)
base_diario = base_diario.convert_dtypes() #ajustando os tipos da colunas
reversa_df = pd.merge(reversa_df, base_diario, on='Chave Pedido e Nota', how='left')
base_diario = pd.DataFrame()


print('fim proc, inicio calculadas')

#######################################################################################################################
#                                                  MERGES ACABAM AQUI                                                 #
#######################################################################################################################


#reversa_df = reversa_df.drop_duplicates(subset=['Chave PV+SKU'], keep='first', ignore_index=True)

reversa_df = reversa_df.convert_dtypes()

for y in reversa_df.columns:
    if (is_string_dtype(reversa_df[y])):
        reversa_df[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(reversa_df[y])):
        reversa_df[y].fillna(blank, inplace=True)

#######################################################################################################################
#                             C O M E Ç A R   O S   C A L C U L O S   A Q U I
#######################################################################################################################

#Data_Devolucao_Efetivada

reversa_df['Data_Devolucao_Efetivada'] = np.where(reversa_df['Caixa'] == "Em Transferência", reversa_df['Ultima_atualizacao'],
                                                  reversa_df['Data_Bip_CD'])
print("Data_Devolucao_Efetivada", " - %s" % (time.time() - start_time), " - segundos" )

#Protheus_status
reversa_df["Observação"] = reversa_df["Observação"].astype(str)


reversa_df['Teste2'] = reversa_df['Observação'].str[:10]
reversa_df['Teste3'] = np.where(reversa_df['Teste2'] == "Agendado: ", reversa_df['Observação'].str[10:15]+"/21", pd.NaT)

reversa_df['Teste'] = reversa_df['Protheus_status'].str[:4]

#Protheus_status_corrigido
reversa_df['Protheus_status_corrigido'] = reversa_df.apply(lambda  row: stprotheus(row['Caixa'], row['Protheus_erro'], row ['Teste']), axis=1)
print("Protheus_status_corrigido", " - %s" % (time.time() - start_time), " - segundos" )

reversa_df = reversa_df.drop(columns=['Teste'])

#[Flag] Situação Coleta
reversa_df['Flag - Situação Coleta'] = reversa_df.apply(lambda  row: flagSituacaoColeta(row['Caixa'],
                                                                                        row['Data_Solicitacao'],
                                                                                        row['Data_Coleta_Efetivada'],
                                                                                        row['Data_Devolucao_Efetivada']), axis=1)

print("Flag - Situaçao Coleta", " - %s" % (time.time() - start_time), " - segundos" )

#Aging Coleta
reversa_df['Aging Coleta'] = reversa_df.apply(lambda row: agingcoleta(row['Flag - Situação Coleta'], row['Hoje'],row['Data_Limite_Coleta'], row['Data_Coleta_Efetivada'] ),  axis=1)

print("Aging Coleta", " - %s" % (time.time() - start_time), " - segundos" )

#Data_Limite_Devolução
reversa_df['Data_Limite_Devolucao'] = reversa_df.apply(lambda row: data_limitedev(row['Data_Coleta_Efetivada'], row['Prazo Devolucao']), axis=1)

print("Data_Limite_Devolução", " - %s" % (time.time() - start_time), " - segundos" )

#Aging Devolução
reversa_df['Aging Devolucao'] = reversa_df.apply(lambda row: agingdev(row['Data_Devolucao_Efetivada'], row['Caixa'],row['Data_Limite_Devolucao'], row['Hoje']),  axis=1)

print("Aging Devolução", " - %s" % (time.time() - start_time), " - segundos" )


#Aging Ultima Atualização
reversa_df['Aging Ult. Atualizacao'] = reversa_df.apply(lambda  row: agingUlt(row['Caixa'], row['Ultima_atualizacao'], row['Data_Coleta_Efetivada']), axis=1)
print("Aging Ultima Atualização", " - %s" % (time.time() - start_time), " - segundos" )

#Cluster devolução
reversa_df["Cluster Devolucao"] = reversa_df.apply(lambda row: clusterDUTILDev(row ['Caixa'], row["Aging Devolucao"]), axis=1)

print("Cluster Devolucao", " - %s" % (time.time() - start_time), " - segundos" )



#[Flag]Atraso na coleta
reversa_df['Flag - Atraso Coleta'] = reversa_df.apply(lambda row: flagAtraso(row ['Caixa'], row["Aging Coleta"]), axis=1)

print("[Flag]Atraso na coleta", " - %s" % (time.time() - start_time), " - segundos" )


#Cluster Coleta
reversa_df["Cluster Coleta"] = reversa_df.apply(lambda row: clusterDUTILColeta(row ['Flag - Situação Coleta'],
                                                                               row["Aging Coleta"]), axis=1)

print("Cluster Coleta", " - %s" % (time.time() - start_time), " - segundos" )

#[Flag]Atraso na coleta (5+)
reversa_df['Flag - Atraso Coleta (5+)'] = reversa_df.apply(lambda row: flagAtraso5(row ['Caixa'], row["Aging Coleta"]), axis=1)

print("[Flag]Atraso Coleta (5+)", " - %s" % (time.time() - start_time), " - segundos" )

#[Flag]Atraso na devolução
reversa_df['Flag - Atraso Devolucao'] = reversa_df.apply(lambda row: flagAtrasoDev(row ['Caixa'], row["Aging Devolucao"]), axis=1)

print("[Flag]Atraso Devolucao", " - %s" % (time.time() - start_time), " - segundos" )

#SLA Coleta
reversa_df['SLA Coleta'] = reversa_df.apply(lambda row: slaColeta(row ['Data_Coleta_Efetivada'], row["Caixa"], row['Data_Limite_Coleta'], row['Hoje']), axis=1)
print("SLA Coleta", " - %s" % (time.time() - start_time), " - segundos" )

#Aging Solicitação
reversa_df['Aging Solicitacao'] = reversa_df.apply(lambda  row: agingsolicitacao(row["Caixa"], row['Data_Solicitacao'], row['Data_Criacao'], row ['Hoje']), axis=1)
print("Aging Solicitacao", " - %s" % (time.time() - start_time), " - segundos" )


#SLA Solicitação
reversa_df['SLA Solicitacao'] = np.where(reversa_df["Caixa"]!="Excluido",reversa_df.apply(lambda row: slasolicitacao(row['Aging Solicitacao']), axis=1),blank)


#SLA Devolução
reversa_df['SLA Devolução'] = reversa_df.apply(lambda row: slaDev(row ['Data_Devolucao_Efetivada'], row["Caixa"], row['Data_Limite_Devolucao'], row['Hoje']), axis=1)
print("SLA Devolução", " - %s" % (time.time() - start_time), " - segundos" )

#SLA Coletas em aberto
reversa_df['SLA Coleta em Aberto'] = reversa_df.apply(lambda row: slaColA(row ['Caixa'], row['Data_Limite_Coleta'], row['Hoje']), axis=1)
print("SLA Coleta em Aberto", " - %s" % (time.time() - start_time), " - segundos" )

#SLA Devolução aberto
reversa_df['SLA Devolucao Aberto'] = reversa_df.apply(lambda row: slaDevAberto(row ['Data_Devolucao_Efetivada'], row["Caixa"], row['Data_Limite_Devolucao'], row['Hoje']), axis=1)
print("SLA Devolução Aberto", " - %s" % (time.time() - start_time), " - segundos" )


#[tag]Problema_novo
reversa_df['[tag]Problema_novo'] = reversa_df.apply(lambda row: tagProblemaNovo(row['Caixa'],row['Problema_pedido']),axis=1)
print("[tag]Problema_novo", " - %s" % (time.time() - start_time), " - segundos" )


#Aging_problema_atendimento
reversa_df['Aging_problema_atendimento'] = reversa_df.apply(lambda row: agingproblema( row['Caixa'], row['[tag]Problema_novo'], row['Finalizado?'], row ['Hoje'], row ['Início Prazo Ativo']
                                                                                       , row ['Data Finalização Ativo']), axis=1)
print("Aging_problema_atendimento", " - %s" % (time.time() - start_time), " - segundos" )

#Aging_problema_total
reversa_df['Aging_problema_total'] = reversa_df.apply(lambda  row: agingproblema(row['Caixa'], row['[tag]Problema_novo'], row['Finalizado?'], row ['Hoje'], row ['Início Prazo Ativo']
                                                                                       , row ['Data da Finalização Ocorrência']), axis=1)
print("Aging_problema_atendimento", " - %s" % (time.time() - start_time), " - segundos" )

#Aging_problema_novos
reversa_df['Aging_problema_novos'] = reversa_df.apply(lambda  row: agingprobnovo(row['Caixa'], row['[tag]Problema_novo'], row['Hoje'], row ['Data_Problema_Criacao']), axis=1)
print("Aging_problema_novos", " - %s" % (time.time() - start_time), " - segundos" )


#Filial NFVenda
reversa_df['Filial NFVenda'] = np.where(reversa_df['NFD'] == blank, blank_str,
                                               np.where(reversa_df['Serie_NFVenda'] == 1, '010102',
                                                "01010"+reversa_df['Serie_NFVenda'].astype(str)))

print("Filial NFVenda", " - %s" % (time.time() - start_time), " - segundos")

#Problema_depara_posicao

reversa_df['Teste5'] = reversa_df['Observação'].str[:5]
reversa_df['Problema_depara_posicao'] = reversa_df.apply(lambda  row: problemadepara(row['Caixa'], row['[tag]Problema_novo'], row ['Posição Final'], row['Teste5']), axis=1)
print("Problema_depara_posicao", " - %s" % (time.time() - start_time), " - segundos" )

reversa_df = reversa_df.drop(columns=['Teste5'])


#Data_problema_reativar
reversa_df['Data_problema_reativar'] = reversa_df.apply(lambda  row: probreativar(row['Caixa'], row['Problema_depara_posicao'], row ['Teste2'], row['Teste3']), axis=1)
print("Data_problema_reativar", " - %s" % (time.time() - start_time), " - segundos" )

reversa_df = reversa_df.drop(columns=['Teste2'])
reversa_df = reversa_df.drop(columns=['Teste3'])

#Aging_problema_consolidado
reversa_df['Aging_problema_consolidado'] = reversa_df.apply(lambda  row: agingpconsolidado(row['Caixa'], row['[tag]Problema_novo'], row['Aging_problema_novos'], row ['Problema_depara_posicao'], row['Aging_problema_total'], row['Aging_problema_atendimento']), axis=1)

print("Aging_problema_consolidado", " - %s" % (time.time() - start_time), " - segundos" )


#selecionaTransp_cep
reversa_df['selecionaTransp_cep'] = reversa_df.apply(lambda  row: transpCep(row['CEP']), axis=1)

print("selecionaTransp_cep", " - %s" % (time.time() - start_time), " - segundos" )

#Transportadora Selecionada
reversa_df['Transportadora Selecionada'] = reversa_df.apply(lambda  row: tpSelecionada(row['selecionaTransp_cep'], row['Transp Reversa2'], row['Transp Reversa1']), axis=1)

#Transp_depara_parceiros
reversa_df['Transportadora_depara_parceiros'] = reversa_df.apply( lambda row: transpDepara(row['Transportadora'],row['Transp_parceiros']),axis=1)

print("Transportadora Selecionada", " - %s" % (time.time() - start_time), " - segundos")

#Cluster Vencimento
reversa_df['Cluster Vencimento'] = reversa_df.apply( lambda row: statusVencimento(row['Hoje'],row['Data_Limite_Coleta']),axis=1)



# #######################################################################################################################
# #                             O S   C A L C U L O S   A C A B A R A M   A Q U I
# #######################################################################################################################

#reversa_df['Número de registros'] = 1

#substitui acentos
#reversa_df = ajustaLetras(reversa_df)

reversa_df = reversa_df.convert_dtypes()
reversa_df = reversa_df.convert_dtypes()


reversa_df = reversa_df.drop(columns=['Transp Reversa1'])

# Proc com TP de Para
transportadora_entrega_df = pd.read_excel(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\03. ReversaAuxiliares\Transportadora_DePara.xlsx")
transportadora_entrega_df.rename(columns={"DePara Transportadora": "Transportadora_depara_parceiros"}, inplace=True)
print(transportadora_entrega_df)
reversa_df['Transportadora_depara_parceiros'] = reversa_df['Transportadora_depara_parceiros'].str.upper()
reversa_df = pd.merge(reversa_df, transportadora_entrega_df, on='Transportadora_depara_parceiros', how='left')
print('Proc TP de_para')
print(reversa_df.head())

#chave para proc unidades
reversa_df.insert(loc=num_colunas, column='chave_unidade_transp', value=reversa_df['Transportadora Last Mile'].astype(str) + reversa_df['Cidade'].astype(str) + reversa_df['Estado'].astype(str))
print(reversa_df.shape[0], "linhas")
print("chave_unidade_transp", " - %s" % (time.time() - start_time), " - segundos" )


#Proc com o unidades
list = ['chave_unidade_transp', 'Unidade']
#types = {'chave_unidade_transp': "string" , 'Unidade': "string" }
unidades_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Unidades.csv", sep=';', usecols=list)
print("abriu csv", " - %s" % (time.time() - start_time), " - segundos" )
#unidades_df = ColunasCalculadas.ajustaLetras(unidades_df)
unidades_df = unidades_df.convert_dtypes() #ajustando os tipos da colunas
unidades_df['chave_unidade_transp'] = unidades_df['chave_unidade_transp'].str.upper()
base_df = pd.merge(reversa_df, unidades_df, on='chave_unidade_transp', how='left')
unidades_df= pd.DataFrame()


#CLASSIFICA POR ID E MANTEM APENAS A PRIMEIRA OBSERVAÇÃO DE CADA
reversa_df = reversa_df.sort_values(by=['ID','SKU'])
reversa_df = reversa_df.drop_duplicates(subset=['ID'], keep='first', ignore_index=True)

################################################################################################
reversa_df = reversa_df.replace(blank, np.nan, regex=True)
reversa_df = reversa_df.replace(str(blank), '', regex=True)
print(reversa_df.dtypes)

#criando coluna com o Dia D
h = datetime.today().strftime('%H')
num_colunas = reversa_df.shape[1]
reversa_df.insert(loc=num_colunas, column='Dia (D)', value='D_0 - '+h+'h')

reversa_df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Reversa.csv', sep=';', decimal=',',  encoding="utf-8-sig",index=False)
reversa_df.to_excel(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads\Reversa.xlsx',  encoding="utf-8-sig",index=False)


# #Copiando o arquivo para a rede
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads'
old_file_path = os.path.join(endereco, "Reversa.csv")
new_file_path = os.path.join(pasta_nova, "Reversa.csv")
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")
time.sleep(3)

h = datetime.today().strftime('%H')
##cria arquivo D0
partialFileName = "D_0"
endereco = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\02. BasesDia - Reversa'
newlist = []
time.sleep(5)
items = os.listdir(endereco)
for names in items:
    if names.startswith(partialFileName):
        newlist.append(names)
print(newlist)

if len(newlist) == 0:
    nome = 'D_0 - 08h.csv'
else:
    nome = 'D_0 - ' + h + 'h.csv'

reversa_df.to_csv('C:/Users/eliana.rodrigues/Downloads/'+nome, sep=';', index=False)
print("Arquivo Criado")


#Copia arquivo para a rede

fileDir = r"C:\Users\eliana.rodrigues\Downloads"
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\02. BasesDia - Reversa'

old_file_path = os.path.join(fileDir, nome)
new_file_path = os.path.join(pasta_nova, nome)
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")



print("--- %s seconds TOTAL ---" % (time.time() - start_time))
print(datetime.today().strftime('%H:%M'))

print("Finalizado")
