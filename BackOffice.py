# importando bibliotecas
import time
import pandas as pd
import os
import shutil
from datetime import datetime, timedelta
import numpy as np
from workalendar.america import BrazilBankCalendar
import ColunasCalculadas
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import xlrd
import sqlalchemy

print(datetime.today().strftime('%H:%M'))
start_time = time.time()

cal = BrazilBankCalendar()
cal.include_ash_wednesday = False  # tirar a quarta de cinzas, pra gente é dia normal
blank = 12349999
blank_str = str(blank)

pd.set_option('display.max_rows', None)
cal = BrazilBankCalendar()
cal.include_ash_wednesday = False  # tirar a quarta de cinzas, pra gente é dia normal

cal.include_ash_wednesday = False  # tirar a quarta de cinzas, pra gente é dia normal
cal.include_christmas = True  # considera natal como feriado
cal.include_christmas_eve = True  # considera natal como feriado
cal.include_new_years_day = True  # considera ano novo como feriado
cal.include_new_years_eve = True  # considera ano novo como feriado
cal.include_new_years = False  # considera ano novo como feriado


#######################################################################################################################
#                                                   FUNÇÕES DEF
#######################################################################################################################

def analistasac(deparacd, fase, analista):
    if deparacd == 'FULFILLMENT - B' and fase == 'EM REDESPACHO':
        return 'Lucas Steffens'
    elif deparacd == 'FULFILLMENT - B' and fase != 'EM REDESPACHO':
        return 'Nicholas Vieira'
    else:
        return analista


def validaT2_T3(tlm, tultima, tlm1, tultima1, tlm2, tultima2):
    if tlm == tultima:
        return 1
    elif tlm == 'FCB BRASIL':
        if tlm1 == tultima1:
            return 1
        else:
            return 0
    elif tlm == "PR DLOG":
        if tlm1 == tultima1:
            return 1
        else:
            return 0
    elif tlm == "MG - DOMINALOG":
        if tlm2 == tultima2:
            return 1
    else:
        return 0


def transportadora(split, dataexped, origemleft, dataentregue, fornecedor, ctrct3, reversa, oc, origem, databip, teagle,
                   tultima, tlm, tcoleta):
    # print(split, dataexped, origemleft, dataentregue, reversa, oc, origem, databip, teagle, tultima, tlm,tcoleta)

    if pd.isnull(dataexped) or dataexped == blank and origemleft == "CD" and pd.isnull(dataentregue):
        return "FULFILLMENT - B"
    elif pd.isnull(dataexped) and reversa != blank_str and pd.notnull(ctrct3):
        return "FULFILLMENT - B"
    elif oc != blank and origem == "CD JDI Fulfillment" and pd.isnull(dataexped) and (
            pd.notnull(databip) or reversa != blank_str):
        return "FULFILLMENT - B"
    elif teagle != blank_str:
        return teagle
    elif tultima != blank_str:
        return tultima
    elif tlm != blank_str:
        return tlm
    elif tcoleta != blank_str:
        return tcoleta
    else:
        return blank_str


## def para calculo de dia útil entre duas datas
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
                resp = (cal.get_working_days_delta(a, b)) + 1
            else:
                resp = (cal.get_working_days_delta(a, b))
                resp = resp * -1
        else:
            resp = (cal.get_working_days_delta(a, b))
            if a > b:
                return -resp
    return resp


## se a data fechamento for diferente de branco, se o motivo for = carta de correção e o botão da carta de corr for igual a vazio
# retorna carta de correção - sem botão. se a data fechamento for igual a vazio retorna aberto, caso contrario aberto
def reclamacaoAberto(Data_fechamento, motivo, data_botaoCCE):
    if pd.isnull(Data_fechamento) == False:
        return "Fechado"
    elif motivo == "Carta de correção" and pd.isnull(data_botaoCCE):
        return "Carta de correção - Sem Botao"
    elif pd.isnull(Data_fechamento):
        return "Aberto"
    else:
        return "Aberto"


def clusterCarta(flag, datacriacao, hoje):
    if flag == "Carta de correção - Sem Botao":
        Dias = diaUtil_entredatas(datacriacao, hoje)
        if int(Dias) == int(blank):
            return blank
        if Dias == 0:
            return "1. D0"
        elif Dias == 1:
            return "2. D-1"
        elif Dias == 2:
            return "3. D-2"
        elif Dias <= 5:
            return "4. D-5"
        elif Dias <= 10:
            return "5. D6 - D10"
        elif Dias < 20:
            return "6. D > 10"
        elif Dias <= 30:
            return "7. D >= 20"
        elif Dias > 30:
            return "8. D > 30"

        return 'Cluster nao definido'
    else:
        return ""


def clusterDUTIL(Dias):
    if int(Dias) == int(blank):
        return blank
    resp = Dias * (-1)
    if Dias == 0:
        return "1. D0"
    elif Dias == 1:
        return "2. D-1"
    elif Dias == 2:
        return "3. D-2"
    elif Dias <= 5:
        return "4. D-5"
    elif Dias <= 10:
        return "5. D6 - D10"
    elif Dias < 20:
        return "6. D > 10"
    elif Dias <= 30:
        return "7. D >= 20"
    elif Dias > 30:
        return "8. D > 30"

    return 'Cluster nao definido'


def maiorData(primeiradata, segundadata):
    if pd.isnull(primeiradata) and pd.isnull(segundadata):
        return pd.NaT
    elif pd.isnull(primeiradata) == False and pd.isnull(segundadata):
        a = pd.to_datetime(primeiradata, infer_datetime_format=True, dayfirst=True)
        return a
    elif pd.isnull(primeiradata) and pd.isnull(segundadata) == False:
        b = pd.to_datetime(segundadata, infer_datetime_format=True, dayfirst=True)
        return b
    else:
        a = pd.to_datetime(primeiradata, infer_datetime_format=True, dayfirst=True)
        b = pd.to_datetime(segundadata, infer_datetime_format=True, dayfirst=True)
        if a >= b:
            return a
        else:
            return b


def diasemana(dia):
    if pd.isnull(dia):
        return blank
    else:
        weekday = int(dia.strftime("%w"))
        return weekday


def clusterRa(diasRa, prazosla):
    if int(diasRa) == blank:
        cluster_ra = blank_str
    elif int(diasRa) > prazosla:
        cluster_ra = "Atraso"
    elif int(diasRa) == prazosla:
        cluster_ra = "Vence Hoje"
    else:
        cluster_ra = "No Prazo"
    return cluster_ra


def clustersac(passou, motivo, datacce, casoc, clustterra, dias, prazosla, clusteratendimento):
    if passou == 1:
        cluster_sac = 'Atraso'
    elif motivo == "Carta de correção" and pd.isnull(datacce) == False and casoc == 'Reclame Aqui':
        cluster_sac = clustterra
    elif dias == blank:
        cluster_sac = blank
    elif dias > prazosla:
        cluster_sac = "Atraso"
    elif clusteratendimento == "Atraso D-1":
        cluster_sac = "Atraso"
    elif dias == prazosla:
        cluster_sac = "Vence Hoje"
    else:
        cluster_sac = "No Prazo"
    return cluster_sac


def flagatraso(cluster):
    if cluster == 'Atraso':
        flag = 1
    else:
        flag = 0
    return flag


def dexpara_tp(tpuni, depara):
    if tpuni == blank_str or tpuni == blank:
        return "Sem transportadora"
    else:
        return depara


def dias(flag, dcriacao, dfechamento, dbotaocce, motivo, hoje):
    if flag == "Fechado":
        dia = diaUtil_entredatas(dcriacao, dfechamento)
    elif flag == "Fechado" and pd.isnull(dbotaocce) == False:
        dia = diaUtil_entredatas(dbotaocce, dfechamento)
    elif motivo == "Carta de correção" and pd.isnull(dbotaocce) == False:
        dia = diaUtil_entredatas(dbotaocce, hoje)
    elif flag == "Aberto" or flag == "Carta de correção - Sem Botao":
        dia = diaUtil_entredatas(dcriacao, hoje)
    elif pd.isnull(dbotaocce):
        dia = diaUtil_entredatas(dcriacao, hoje)
    else:
        dia = diaUtil_entredatas(dbotaocce, dfechamento)
    return dia


def clustercce(datacriacao, hoje, prazosla):
    dia = diaUtil_entredatas(datacriacao, hoje)
    if dia > prazosla:
        return "Atraso"
    elif dia == prazosla:
        return "Vence Hoje"
    elif dia < prazosla:
        return "Vence Futuro"
    else:
        return blank_str


def dias_RA(caso_critico, flag_ab_fe, hoje, datamaxra, d_fechamento):
    if caso_critico == 'Reclame Aqui':
        if flag_ab_fe == 'Aberto':
            diasRA = diaUtil_entredatas(hoje, datamaxra)
        else:
            diasRA = diaUtil_entredatas(d_fechamento, datamaxra)
    else:
        diasRA = blank
    return diasRA


def passouontem(flag_ab_fe, dias, prazosla):
    if flag_ab_fe == 'Aberto':
        if int(dias) == (int(prazosla) + 1):
            flag = 1
        else:
            flag = 0

    else:
        flag = 0

    return flag


def slafechado(flag, dias, prazosla):
    if flag == "Fechado":
        if dias == blank or prazosla == blank:
            return blank
        else:
            if prazosla < dias:
                return 0
            else:
                return 1
    else:
        return blank


def regraexped(dataexped, setor):
    if pd.isnull(dataexped) and setor != "Transportadora":
        return "Pendencia TC"
    else:
        return blank_str


def firstpendencia(regraexped, giro, pedfinalizar):
    if regraexped == 'Pendencia TC':
        return 'Pendencia TC'
    elif giro != 1 and pedfinalizar == 1:
        return 'Pendencia TC'
    else:
        return blank_str


def finalizacoes(ediatualizado, alegnaoentregue, atrasolink):
    if ediatualizado != blank_str:
        return ediatualizado
    elif alegnaoentregue != blank_str:
        return alegnaoentregue
    elif atrasolink != blank_str:
        return atrasolink
    else:
        return blank_str


def diascorridos(menordia, maiordia):
    try:
        blank = 1239999
        a = menordia
        b = maiordia
        resp = blank
        if pd.isnull(menordia) or pd.isnull(maiordia):
            return blank
        else:
            a = pd.to_datetime(menordia, infer_datetime_format=True, dayfirst=True)
            b = pd.to_datetime(maiordia, infer_datetime_format=True, dayfirst=True)
            resp = abs((a - b).days)
            # if a > b:
            return resp
    except:
        return resp


def alegacao(motivo, dias):
    if motivo == 'Conferência de Entrega' and dias > 7:
        return 'Conferência fora do prazo [Finalizar]'
    elif motivo == 'Pedido com alegação de não entregue' and dias > 2:
        return 'Alegação fora do prazo [Finalizar]'
    else:
        return blank_str


def duplicadasfinalizar(motivo, diascorridos):
    if motivo == "Atraso na Entrega" or motivo == "Sem acesso ao rastreio do pedido" and diascorridos == 0.0 or diascorridos == 1.0 or diascorridos == 2.0 or diascorridos == 3.0:
        return "Nota possui movimentação [Finalizar]"
    else:
        return blank_str


def peddupliF(pedidos, flag):
    if flag != 'Fechado' and pedidos >= 2:
        return 1
    else:
        return 0


def atrasolink(pedifinalizar, motivo, dtentregue):
    if pedifinalizar == 1:
        return 'Ocorrência duplicada [Finalizar]'
    if motivo == "Atraso na Entrega" or motivo == "Sem acesso ao rastreio do pedido" and pd.isnull(dtentregue) == False:
        return "Finalizar nota entregue"
    else:
        return blank_str


def motivoentregues(motivo, dataentregue):
    if motivo in ["Atraso na Entrega", "Sem acesso ao rastreio do pedido", "Adiar entrega",
                  "Alteração de número/complemento no endereço NF já faturada",
                  "Atraso Criação OC(Avaria / Extravio)", "Carta de correção",
                  "Mercadoria Retida na Fiscalização"] and pd.isnull(dataentregue) == False:
        return "Nota Entregue [Finalizar]"
    else:
        return blank_str


def confereentregue(motivo, dias):
    if motivo == 'Conferência de Entrega' and dias > 9:
        return 'Conferência fora do prazo [Finalizar]'
    elif motivo == 'Pedido com alegação de não entregue' and dias > 2:
        return 'Alegação fora do prazo [Finalizar]'
    else:
        return blank_str


def ediatualizado(aging, motivo):
    if aging == blank:
        return blank_str
    elif motivo in ['Atraso na Entrega', 'Sem acesso ao rastreio do pedido'] and aging <= 3:
        return 'Nota possui movimentação [Finalizar]'
    else:
        return blank_str


def prodcancelado(splicancelado, sacfinalizacao, motivo):
    if splicancelado == blank_str and sacfinalizacao == blank_str:
        return 'Produto/pedido cancelado [Finalizar]'
    else:
        return blank_str


def alegacaoextraviada(motivo, statusportal):
    if motivo == 'Pedido com alegação de não entregue' and statusportal == 'EXTRAVIADO':
        return 'Finalizar'
    else:
        return blank_str


def dbnew(motivo, tratativa):
    if motivo != "Atraso na Entrega" and tratativa == "Plano de Entregas":
        return "Analisar - Nota no plano <> Atraso"
    else:
        return tratativa


## DATA APÓS N DIAS UTEIS

def diaUtil_aposNDias(data, ndias):
    if pd.isnull(data) or pd.isnull(ndias):
        return pd.NaT
    else:
        data = pd.to_datetime(data, infer_datetime_format=True, dayfirst=True)
        resp = cal.add_working_days(data, ndias)
    return resp


def getNdays_asString(n):
    resp = pd.date_range(end=datetime.today(), periods=n).tolist()
    resp = pd.Series(pd.to_datetime(resp, infer_datetime_format=True, format='%Y%m%d')).sort_values(ascending=False)
    resp = resp.dt.strftime('%Y_%m_%d').tolist()
    return resp


def finalizar_prazoTUY(hoje, data_expedicao, motivo, data_tuy):
    hoje = datetime.strptime(hoje, '%d/%m/%y')
    delta = hoje - data_expedicao
    if (delta.days >= 2) or (pd.isna(data_expedicao) or pd.isna(data_tuy) or not (
            motivo in ['Atraso na Entrega', 'Sem acesso ao rastreio do pedido'])):
        return np.nan
    else:
        return "Concluir transportadora"


def finalizar_alegacaoantiga(motivo, data_entregue):
    if motivo != 'Pedido com alegação de não entregue' or pd.isna(data_entregue):
        return np.nan
    data_entregue = pd.to_datetime(data_entregue, infer_datetime_format=True, dayfirst=True)
    data_limite = datetime.strptime('01012022', '%d%m%Y')
    if data_entregue < data_limite:
        return "Finalizar"
    return np.nan


def ocinvalida(motivo, status_portal):
    if pd.isna(motivo) or pd.isna(status_portal):
        return np.nan
    elif motivo == 'Atraso Criação OC (Avaria/Extravio)' and not (status_portal in ['AVARIADO', 'EXTRAVIADO']):
        return 'Retornar ao Analista'
    else:
        return np.nan


def bot_finaliza(ultimoedi, motivo, statusportal, motivoentregue, prodcancelado,
                 alegacaooext, tratativadiario, vencimentodiario, hoje, planoentregaroger,
                 motivosacfdata, ediatualizado, prazo_tuy, alegacao_antiga, ocinvalida):
    if pd.notna(alegacao_antiga):
        return "[Finalizar]Alegação com data entregue muito antiga"
    if ultimoedi != "Produto retido para fiscalização. Por favor, aguarde. Em breve, o status será atualizado." and motivo == "Mercadoria Retida na Fiscalização":
        return "Erro Depara Edi, mercadoria não está retida [Finalizar]"
    elif motivo in ["Atraso na Entrega", "Sem acesso ao rastreio do pedido", "Avaria no produto em transporte",
                    "Alteração de número/complemento no endereço NF já faturada",
                    "Mercadoria Retida na Fiscalização", "Carta de correção"] and statusportal == "ENTREGUE":
        return "Nota com status entregue [Finalizar]"
    elif motivo in ["Atraso na Entrega", "Sem acesso ao rastreio do pedido",
                    "Avaria no produto em transporte"] and statusportal in ["EXTRAVIADO", "AVARIADO"]:
        return "Nota extraviada/avariada, recompra gerada [Finalizar]"
    elif motivo in ["Atraso na Entrega", "Sem acesso ao rastreio do pedido", "Avaria no produto em transporte",
                    "Alteração de número/complemento no endereço NF já faturada", "Mercadoria Retida na Fiscalização",
                    "Carta de correção"] and statusportal == "CANCELADO":
        return "Status CANCELADO [Finalizar]"
    elif pd.notna(ocinvalida):
        return "[Finalizar] Atraso OC com status invalido"
    elif motivoentregue != blank_str:
        return motivoentregue
    elif prodcancelado != blank_str:
        return prodcancelado
    elif alegacaooext != blank_str:
        return 'Alegação de não entregue EXTRAVIADA [Finalizar]'
    elif tratativadiario == "Plano de Entregas" and vencimentodiario >= pd.to_datetime(hoje):
        return "Plano de entrega [Finalizar]"
    elif planoentregaroger == blank and motivosacfdata == 1:
        return '[Finalizar] Plano de entrega Roger'
    elif ediatualizado != blank_str:
        return ediatualizado
    elif pd.notna(prazo_tuy):
        return "[Finalizar] Prazo Tuy"
    else:
        return blank_str


def bot_acoes(finalizacoesbot):
    if finalizacoesbot in ["Nota extraviada/avariada, recompra gerada [Finalizar]", "Status CANCELADO [Finalizar]",
                           "Erro Depara Edi, mercadoria não está retida [Finalizar]",
                           "Nota com status entregue [Finalizar]",
                           "[Finalizar]Alegação com data entregue muito antiga"]:
        return "BOT Finalizar"
    elif finalizacoesbot in ["[Finalizar] Plano de entrega Roger", "Plano de entrega [Finalizar]",
                             '[Finalizar] Prazo Tuy']:
        return "BOT Concluir Transportadora"
    elif finalizacoesbot in ["Nota Entregue [Finalizar]", "Nota possui movimentação [Finalizar]",
                             "Produto/pedido cancelado [Finalizar]", "Alegação de não entregue EXTRAVIADA [Finalizar]"]:
        return 'BOT Finalizar'
    elif finalizacoesbot in ['[Finalizar] Atraso OC com status invalido']:
        return 'BOT Retornar ao Analista'
    else:
        return blank_str


def bot_justifica(finalizacoesbot):
    if finalizacoesbot == "Nota extraviada/avariada, recompra gerada [Finalizar]":
        return "Foi apontado avaria/extravio da nota, recompra gerada"
    elif finalizacoesbot == "Nota com status entregue [Finalizar]":
        return "Encerrando protocolo, nota foi entregue"
    elif finalizacoesbot == "Status CANCELADO [Finalizar]":
        return "Encerrando protocolo, nota cancelada"
    elif finalizacoesbot == "Erro Depara Edi, mercadoria não está retida [Finalizar]":
        return "Mercadoria não está retida na fiscalização, caso esteja em atraso na entrega, favor abrir o protocolo 'Atraso na entrega"
    elif finalizacoesbot == "Nota possui movimentação [Finalizar]":
        return 'Edi atualizado, nota seguindo para entrega'
    elif finalizacoesbot == "Nota possui movimentação [Finalizar]":
        return 'EDI atualizado'
    elif finalizacoesbot == "Produto/pedido cancelado [Finalizar]":
        return "Pedido Cancelado"
    elif finalizacoesbot == "Nota Entregue [Finalizar]":
        return "Nota entregue"
    elif finalizacoesbot == 'Alegação de não entregue EXTRAVIADA [Finalizar]':
        return "Nota extraviada"
    elif finalizacoesbot == '[Finalizar] Prazo Tuy':
        return 'Nota expedida, previsão de entrega estipulada'
    elif finalizacoesbot == "[Finalizar]Alegação com data entregue muito antiga":
        return 'Prazo para alegação encerrado, nota entregue antes de 2022'
    elif finalizacoesbot == "[Finalizar] Atraso OC com status invalido":
        return 'Criação OC sem Extravio/Avaria'
    else:
        return blank_str


def bot_dataentrega(planoentregaroger, vencimentodiario, prazo_tuy, hoje):
    try:
        if (pd.isnull(planoentregaroger) or planoentregaroger == blank) and pd.notna(vencimentodiario):
            return vencimentodiario
        if pd.notna(planoentregaroger):
            return pd.to_datetime(planoentregaroger) + timedelta(days=4)
        elif pd.notna(prazo_tuy):
            return prazo_tuy
        else:
            return pd.NaT
    except:
        return pd.NaT


def pendenciatc(motivo, datacce):
    if motivo == "Atraso Criação OC (Avaria/Extravio)":
        return "Pendencia TC"
    elif motivo == "Carta de correção" and pd.isnull(datacce):
        return "Pendencia TC"
    else:
        return blank_str


def agchegadaccd(motivo, datacce, emissaocce):
    if motivo == "Carta de correção" and pd.isnull(datacce) == False and pd.isnull(emissaocce):
        return "Aguardando chegada LM"
    else:
        return blank_str


def planoT3(baseroger, hoje):
    try:
        if baseroger == "EM ROTA DE ENTREGA":
            plano = hoje
        elif baseroger == "TRATATIVA T2":
            plano = pd.NaT
        else:
            plano = baseroger
        return plano
    except:
        return pd.NaT


def problemaseagle(motivo, dataemissao, dispeagle, eagletp):
    if motivo == "Carta de correção" and pd.isnull(dataemissao) == False and dispeagle == blank:
        return "Pendencia TC"
    elif eagletp == "tp" and dispeagle == blank:
        return "Pendencia TC"
    else:
        return blank_str


def visaotp(dispeagle):
    if dispeagle == 1:
        return "Transportadora"
    else:
        return "Fora Visão Tp"


def responsabilidade(pendenciatc, aguardandochegada, problemasealge, setor):
    if pendenciatc != blank_str:
        return pendenciatc
    elif aguardandochegada != blank_str:
        return aguardandochegada
    elif problemasealge != blank_str:
        return problemasealge
    elif setor != "Transportadora":
        return "Transportadora fora do Eagle"
    else:
        return "Transportadora"


def prazoatendimento(cce, dias, prazosla, dataemissaocce, motivo, validacao, databotao, setor):
    if cce == "Carta de correcao aguardando step nota LAST MILE" and pd.isnull(dataemissaocce) and pd.isnull(databotao):
        return "Aguardando emissão CCE (Aguardando chegar LM)"
    elif pd.isnull(dataemissaocce) and motivo == "Carta de correção" and validacao == 0:
        return "Aguardando emissão CCE (Aguardando chegar LM)"
    elif pd.isnull(databotao) == False and (motivo == "Carta de correção" and pd.isnull(dataemissaocce)):
        return "Solicitada CCE/Não emitida [BUG]"
    elif pd.isnull(dataemissaocce) and motivo == "Carta de correção":
        return "Aguardando emissão CCE"
    elif pd.isnull(databotao) and motivo == "Carta de correção" and setor == "Transportadora":
        return "Aguardando emissão CCE (Aguardando chegar LM)"
    elif (dias - prazosla) == 1:
        return "Atraso D-1"
    elif dias > prazosla:
        return "Atraso"
    elif dias == prazosla:
        return "Vence Hoje"
    elif dias < prazosla:
        return "Vence Futuro"
    else:
        return blank_str


def cartasem(dataemissao, motivo):
    if pd.isnull(dataemissao) and motivo == "Carta de correção":
        return 0
    else:
        return 1


def chaveProblemaPV(dproblema, pv):
    if pd.isnull(dproblema):
        resp = str(pv).strip()
    else:
        resp = dproblema.strftime('%d%m%Y') + str(pv)
    return str(resp).strip()


def tpdepara(depara, parceiros, dataexped):
    if depara == "Sem transportadora":
        return "FULFILLMENT - B"
    elif parceiros == blank_str or pd.isnull(dataexped):
        return depara
    else:
        return parceiros


def deparaCD(tpdepara, expedicao, sacetapa, teagle, fornecedor, cluster, tplm):
    if tpdepara == 'FULFILLMENT - B' and pd.isnull(expedicao) and sacetapa == "T3":
        return "FULFILLMENT - B SEM_EXPEDICAO_DIRETAS"
    elif tpdepara == "FULFILLMENT - B" and pd.isnull(expedicao):
        return 'FULFILLMENT - B SEM_EXPEDICAO'
    elif teagle == 'A N TRANSPORTES' or teagle == 'TRANSNOSSA' and fornecedor == 'FAMA MOVEIS' and pd.isnull(
            expedicao) and sacetapa == "T3":
        return "FULFILLMENT - B SEM_EXPEDICAO_DIRETAS"
    elif teagle == 'A N TRANSPORTES' or teagle == 'TRANSNOSSA' and fornecedor == 'FAMA MOVEIS' and pd.isnull(expedicao):
        return 'FULFILLMENT - B SEM_EXPEDICAO'
    elif teagle == 'A N TRANSPORTES' or teagle == 'TRANSNOSSA' and fornecedor == 'FAMA MOVEIS':
        return "FULFILLMENT - B"
    elif tpdepara in ["PR - MGF - KUWABATA MARINGA", "PR - LDB - JF TRANSPORTE",
                      "PR - GPB - CRISSI TRANSPORTE"] and tplm == "DOMINALOG":
        return tplm
    elif cluster == "Aguardando emissão CCE" or cluster == "Aguardando emissão CCE (Aguardando chegar LM)":
        return tplm
    else:
        return tpdepara


def ccet2(tcoleta, tultima):
    if tcoleta == "FAVORITA":
        return "T3"
    elif tcoleta == tultima:
        return "T2"
    else:
        return "T3"


def planoroger(motivo, auxplano):
    if motivo == "Atraso na Entrega":
        return auxplano
    else:
        return blank_str


def ccesembot(dataemissao, motivo, ccet2, databotao):
    if pd.isnull(dataemissao) and (motivo == "Carta de correção" and ccet2 == "T2"):
        return "Aguardando emissão CCE (Aguardando chegar LM)"
    elif pd.isnull(databotao) and motivo == "Carta de correção" and pd.isnull(dataemissao):
        return "Solicitada CCE/Não emitida [BUG]"
    elif pd.isnull(databotao) and motivo == "Carta de correção":
        return "Sem botão";
    else:
        return "Solicitado"


#######################################################################################################################
#                                           LÊ E MODIFICA SAC PENDENCIAS EAGLE, E CSV TABELAU                         #
#######################################################################################################################

def main():
    workbook = xlrd.open_workbook(r'C:\Users\eliana.rodrigues\Downloads\listagem-pendencias.xls',
                                  ignore_workbook_corruption=True)
    base_geral = pd.read_excel(workbook)

    print(base_geral.shape[0], "linhas")
    base_geral['Split'] = base_geral['Split'].astype(str)

    # CRIA split sem #
    base_geral['Split_corrigido'] = base_geral['Split'].str[1:]
    print('Cirou coluna com split corrigido')
    base_geral['No Eagle'] = 1
    print('criou contador')
    # renomeia as colunas
    base_geral.rename(columns={"Reclamação": "Chave_Reclamacao"}, inplace=True)

    base_geral.to_excel(
        r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\listagem-pendencias.xlsx',
        index=False)

    print('Listagem de Pendências SAC')

    print('fim main')
    return


#######################################################################################################################
#                                                  CONSULTA SQL                                                       #
#######################################################################################################################

print('inicia consulta sql - base backoffice', datetime.today().strftime('%H:%M'))
start_time = time.time()

user = '#######'
senha = '#######'
engine_string = 'mysql+pymysql://%s:%s@portal-ro3.madeiramadeira.com.br:3306/brain' % (user, senha)
engine_solicitacao = sqlalchemy.create_engine(engine_string)
query = '''
SELECT
	CONCAT('M', sra.id) AS Chave_Reclamacao,
    sra.id AS Split,
    teste.descricao as cce,  
    sra.idfk_solucao as Solucao,
    sra.idfk_setor as Idfk_Setor,
    sm.id as Id,
    ad.id as AD_ID,
	sr.id AS Reclamacao,
    pv.id AS Pedido,
    ipv.codigo_sku as SKU,
    c.id as OC,
    ipv.preco_total as valor_total,
    fc.descricao  as Status_Portal,
	fat.numero_nota_fiscal AS NF_Madeira,
	fis.numero AS NF_Fornecedor,
	pf.nome_fantasia AS Fornecedor,
   indv.descricao as Log_Split,
    sm.descricao AS Motivo,
	IF(hist.sac_etapa_transporte IS NULL, 'T3', hist.sac_etapa_transporte) AS SAC_Etapa_Transporte,
	thist.nome_reduzido Transportadora_Responde_EAGLE,
    t.nome_reduzido AS Transportadora_Last_Mile,
   DATE(ret.data_criacao) as Data_ultima_rota,
    CASE
        WHEN ptc.nome_reduzido IS NULL THEN t.nome_reduzido
    END AS Transportadora_Coleta,
    ptu.nome_reduzido AS Transportadora_Ultima_Movimentacao,
    COALESCE(IF(sras.id = 17
		OR sm.descricao LIKE '%%cance%%',
		'Automatico',
		ad.nome),
		ad.nome,
	su.nome) Responsavel,    
    ss.descricao AS Setor,    
	IF(g.descricao IS NULL,
        'MadeiraMadeira',
	g.descricao) AS Loja,
    rcct.descricao AS Caso_Critico,
	pfi.descricao as Origem,
    rm.descricao AS Destino,
    pv.cliente_cidade AS Cliente_Cidade,
    pv.cliente_uf AS UF,
    pv.cliente_cep AS Cliente_CEP,
    edi.descricao AS Ultimo_EDI,
    ret.data_hora AS Data_Hora_Ultimo_EDI,
    if(sra.idfk_setor = 15 AND (eg.id is not NULL OR sra.observacao is not NULL OR indv.idfk_motivo is not NULL OR (COALESCE(log.descricao, log1.descricao) LIKE '%%Encerrado automaticamente: Houve atualização do link do link nos ultimos%%') AND COALESCE(log.idfk_admin, log1.idfk_admin) = 10), 1, 0) AS Acao_EAGLE,
    if(indv.idfk_motivo is not NULL, 1, 0) AS Indevidos,
    CASE
		WHEN sm.descricao IN ('Carta de correção', 'Adiar entrega', 'Alteração de número/complemento no endereço NF já faturada', 'Atraso Criação OC (Avaria/Extravio)', 'Atraso na Entrega', 'Avaria no produto em transporte', 'Mercadoria Retida na Fiscalização') THEN 1
		WHEN sm.descricao IN ('Conferência de Entrega', 'Entrega errada - inversão', 'Entrega no endereço errado', 'Prática indevida da transportadora ') THEN 5
		WHEN sm.descricao IN ('Sem acesso ao rastreio do pedido', 'Pedido com alegação de não entregue') THEN 2
    END AS Prazo_SLA,
	DATE(rota.data_hora) AS Data_ultima_rota,
	DATE(pv.data_entregue) AS Data_entregue,
	DATE(pv.data_entrega)                  as Data_Limite_Cliente_Pediddo,
    DATE(ipv.data_entrega)                 as Data_Limite_Cliente_Item,
    DATE(ipv.data_entregue)                as Data_ENTREGUE_item,
    DATE(sra.data_criacao) as data_criacao_split,
    DATE(tc.data_remetida) AS Data_Expedicao,
    date(scce.data_hora) AS Data_Botao_CCe,
    date(ecce.data_hora) AS Data_Emissao_CCe,
    c.data_entrega_cd as Data_BIP,
    ipv.origem_reserva as Reserva,
    DATE(IF(rcc.idfk_tipo = 2, rcc.data_criacao, '')) AS Data_criacao_reclame_aqui,
    DATE(sra.data_criacao) as Data_criacao,
    DATE_FORMAT(sra.data_criacao, '%%H:%%i:%%s') AS Hora_criacao,
    DATE(sra.data_fechamento) AS Data_fechamento,
    DATE_FORMAT(sra.data_fechamento, '%%H:%%i:%%s') AS Hora_fechamento,
    date(sra.data_promessa_entrega) AS Data_Prometida,
	if(DATE_FORMAT(sra.data_fechamento, '%%H:%%i:%%s') < '03:00:00' AND DATE_FORMAT(sra.data_fechamento, '%%H:%%i:%%s') > '00:00:00' AND sm.id = 54,1,0) AS Tipo_Finalização,
    tc.id "ID da coleta"
FROM
    sac_reclamacao_andamento AS sra
        INNER JOIN
    sac_reclamacao_andamento_item AS srai ON srai.idfk_reclamacao_andamento = sra.id
		INNER JOIN
    sac_item AS si ON si.id = srai.idfk_reclamacao_item
		INNER JOIN
    sac_reclamacao AS sr ON sr.id = sra.idfk_reclamacao
        INNER JOIN
    sac_setor AS ss ON ss.id = sra.idfk_setor
		INNER JOIN
    portal_item_pedido_venda AS ipv ON si.idfk_item = ipv.id
        INNER JOIN
    portal_pedido_venda AS pv ON pv.id = ipv.idfk_pedido_venda
        LEFT JOIN
    portal_item_pedido_compra AS ic ON ic.idfk_item_pedido_venda = ipv.id
        LEFT JOIN
	portal_item_pedido_venda_status_fc fc ON ipv.novo_status_fc = fc.id
         LEFT JOIN
    portal_pedido_compra AS c ON c.id = ic.idfk_pedido_compra
        INNER JOIN
    portal_fornecedor AS pf ON pf.id = ipv.idfk_fornecedor
        LEFT JOIN
    portal_market_place_loja AS l ON l.id = pv.idfk_loja
        LEFT JOIN
    portal_market_place_grupo AS g ON g.id = l.idfk_grupo
        LEFT JOIN
    sac_reclamacao_caso_critico AS rcc ON rcc.idfk_reclamacao = sr.id
        LEFT JOIN
    sac_reclamacao_caso_critico_tipo AS rcct ON rcct.id = rcc.idfk_tipo
        LEFT JOIN
    sac_usuario AS su ON su.id = sra.idfk_usuario
        LEFT JOIN
    sac_acao_reclamacao AS sar ON sar.idfk_reclamacao = sr.id
        LEFT JOIN
    sac_acao AS sa ON sa.id = sar.idfk_acao
        LEFT JOIN
    portal_item_faturamento AS ifat ON ifat.id = (SELECT
            ifat2.id
        FROM
            portal_item_faturamento ifat2
        WHERE
            ifat2.idfk_item_pedido_venda = ipv.id
        ORDER BY ifat2.id DESC
        LIMIT 1)
        LEFT JOIN
    portal_faturamento AS fat ON ifat.idfk_faturamento = fat.id
        INNER JOIN
    sac_motivo AS sm ON sm.id = sr.idfk_motivo
        INNER JOIN
    sac_reclamacao_andamento_status sras ON sra.idfk_status = sras.id
        LEFT JOIN
    portal_transportadora_coleta tc ON tc.idfk_faturamento = fat.id
        LEFT JOIN
    eagle.eagle_log_alteracao_transportadora elat ON elat.idfk_transportadora_coleta = tc.id
        LEFT JOIN
    portal_transportadora_filial fi ON tc.idfk_transportadora = fi.id
        LEFT JOIN
    portal_transportadora t ON fi.idfk_transportadora = t.id
        LEFT JOIN
    portal_admin ad ON sra.idfk_admin = ad.id
        LEFT JOIN
    portal_admin ad2 ON t.idfk_admin_analista = ad2.id
        LEFT JOIN
    portal_admin ad3 ON t.idfk_admin_dropshipping = ad3.id
        LEFT JOIN
    portal_comprador ad4 ON pf.idfk_comprador = ad4.id
        LEFT JOIN
    portal_admin ad5 ON ad5.id = t.idfk_admin_analista_sac
        LEFT JOIN
    portal_item_nf_oc inoc ON ic.id = inoc.idfk_item_pedido_compra
        LEFT JOIN
    portal_item_nota_fiscal inf ON inoc.idfk_item_nota_fiscal = inf.id
        LEFT JOIN
    portal_transportadora ptc ON ptc.id = tc.idfk_transportadora_coleta
        LEFT JOIN
    portal_transportadora_coleta_edi_retorno retu ON retu.id = tc.idfk_ultimo_retorno
        LEFT JOIN
    portal_transportadora_coleta_edi_retorno AS ret ON ret.id = (SELECT
            MAX(id) AS id
        FROM
            portal_transportadora_coleta_edi_retorno
        WHERE
            idfk_transportadora_coleta = tc.id)
        LEFT JOIN
        portal_transportadora_coleta_edi_retorno AS rota ON rota.id = (SELECT
		id
	from
		portal_transportadora_coleta_edi_retorno edi
	WHERE
		edi.idfk_transportadora_ocorrencia = 6
	AND edi.idfk_transportadora_coleta = tc.id
    AND DATE(edi.data_hora) <= DATE(sra.data_criacao)
	order by edi.id desc
    limit 1
	)
        LEFT JOIN
    portal_transportadora_coleta_edi_ocorrencia AS edi ON ret.idfk_transportadora_ocorrencia = edi.id
        LEFT JOIN
    portal_transportadora_coleta_edi_retorno AS ret2 ON ret2.id = (SELECT
            MAX(id) AS id
        FROM
            portal_transportadora_coleta_edi_retorno
        WHERE
            idfk_transportadora_coleta = tc.id
                AND DATE(data_hora) < DATE(sr.data_criacao))
        LEFT JOIN
    portal_transportadora_coleta_edi_ocorrencia AS edi2 ON ret2.idfk_transportadora_ocorrencia = edi2.id
        LEFT JOIN
    portal_transportadora ptu ON ptu.id = retu.idfk_transportadora_filial
        LEFT JOIN
    portal_nota_fiscal fis ON inf.idfk_nota_fiscal = fis.id
        LEFT JOIN
    portal_cidade pc ON pc.id = pv.idfk_cidade
        LEFT JOIN
    portal_regiao_micro rm ON rm.id = pc.idfk_regiao_micro
        LEFT JOIN
	sac_reclamacao_andamento_historico indv ON indv.id = (SELECT
		id 
	FROM
		sac_reclamacao_andamento_historico logg
	WHERE
		logg.idfk_reclamacao_andamento = sra.id
        AND logg.idfk_tipo = 27
	ORDER BY logg.id DESC
    LIMIT 1)
		LEFT JOIN
	sac_reclamacao_andamento_historico as scce on scce.id = (SELECT
			id
		FROM
			sac_reclamacao_andamento_historico srah
		WHERE
			srah.descricao = 'Solicitado Carta de correção para o Protheus'
            and idfk_reclamacao_andamento = sra.id
		order by srah.id desc
        limit 1)
         LEFT JOIN
	sac_reclamacao_andamento_historico as ecce on ecce.id = (SELECT
			id
		FROM
			sac_reclamacao_andamento_historico srah
		WHERE
			srah.descricao LIKE 'Carta de correção gerada com sucesso.%%' 
			-- OR srah.descricao LIKE 'Carta de correcao aguardando step nota LAST MILE'
            and idfk_reclamacao_andamento = sra.id
		order by srah.id desc
        limit 1)
          LEFT JOIN
      #####REGRA CCE EMISSAO########    
	sac_reclamacao_andamento_historico as teste on teste.id = (SELECT id FROM sac_reclamacao_andamento_historico srah WHERE
																	(srah.descricao LIKE 'Carta de correção gerada com sucesso.%%'
																		OR 
																	srah.descricao LIKE 'Solicitado Carta de correção para o Protheus' 
																		OR 
																	srah.descricao LIKE 'Carta de correcao aguardando step nota LAST MILE')
																		and idfk_reclamacao_andamento = sra.id
																	ORDER BY srah.id DESC
																	LIMIT 1)
	 ###############################                                                                       
        LEFT JOIN
    sac_reclamacao_andamento_historico log1 ON log1.id = (SELECT
            log2.id
        FROM
            sac_reclamacao_andamento_historico log2
        WHERE
            log2.idfk_reclamacao_andamento = sra.id
        ORDER BY log2.id DESC
        LIMIT 1)
        LEFT JOIN
    sac_reclamacao_andamento_historico log2 ON log2.id = (SELECT id
        FROM sac_reclamacao_andamento_historico
        WHERE idfk_reclamacao_andamento = sra.id
        ORDER BY data_hora ASC
    LIMIT 1)
        LEFT JOIN
    sac_reclamacao_andamento_historico AS log ON log.id = (SELECT
            log2.id
        FROM
            sac_reclamacao_andamento_historico log2
        WHERE
            log2.idfk_reclamacao_andamento = sra.id
                AND log2.idfk_admin = ad.id
        ORDER BY log2.id DESC
        LIMIT 1)
        LEFT JOIN
    portal_cancelamento pca ON pca.idfk_sac_andamento = sra.id
        LEFT JOIN
    portal_transportadora_coleta_avaria AS tca ON tca.idfk_coleta = tc.id
        LEFT JOIN
    eagle.sac_acao_log AS eg ON sra.id = eg.id_andamento
        LEFT JOIN
    (SELECT
        sra.idfk_andamento_original AS id
    FROM
        sac_reclamacao_andamento sra
    INNER JOIN sac_reclamacao sr ON sr.id = sra.idfk_reclamacao
    WHERE
        sra.idfk_andamento_original IS NOT NULL
            AND sr.data_criacao >= '20190107'
            AND sra.idfk_setor IN (15)
    GROUP BY sra.id) AS duplicados ON duplicados.id = sra.id
        LEFT JOIN
    portal_coleta_reversa_item AS cri ON cri.idfk_item_pedido_venda = ipv.id
        LEFT JOIN
    portal_coleta_reversa AS reversa ON reversa.id = cri.idfk_coleta_reversa
        LEFT JOIN
    eagle.eagle_usuario AS usuario_requisicao ON usuario_requisicao.id = reversa.eagle_idfk_usuario_requisicao
		LEFT JOIN
    sac_transportadora_movimentacao_historico hist ON hist.idfk_andamento = sra.id AND hist.ativo = 1
		LEFT JOIN
    portal_transportadora thist ON thist.id = hist.idfk_transportadora
		LEFT JOIN
	portal_filial pfi on pfi.id = fat.idfk_filial
WHERE
	sra.data_criacao >= DATE('20210101') 
	AND sra.idfk_solucao is NULL
	AND sra.idfk_setor IN (7,8, 15, 16)
	AND sm.id in (114, 19, 149, 119, 187, 120, 27, 54, 183, 20, 9, 121, 186)
    GROUP BY sra.id;

'''
solicitacao = pd.read_sql_query(query, engine_solicitacao)
df = solicitacao.copy()
del solicitacao

df.to_csv(
    r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\Consulta_BackOffice_-_NOVO_data.csv',
    sep=';', index=False)
print(df.head())

print('fim consulta sql - base backoffice', datetime.today().strftime('%H:%M'))

print('inicio consulta sql - base cancelados', datetime.today().strftime('%H:%M'))
query = '''
SELECT

 CONCAT(fat.numero_nota_fiscal, pv.id) AS Chave_Split,
	if (CASE WHEN edi.descricao IN ('Autorização de Devolução','Devolução Autorizada Mi','Mercadoria devolvida ao cliente origem') then'Devolução Autorizada'
		 WHEN edi.descricao IN ('Extravio','Avaria') THEN 'Extravio/Avaria'
		 WHEN edi.descricao IN ('Entregue') THEN 'Entregue'
         WHEN fc.descricao IN ('CANCELADO') THEN 'CANCELADO'
	END = 'Devolução Autorizada' and fc.descricao = 'REMETIDO', 'DEVOLUÇÃO N',CASE WHEN edi.descricao IN ('Autorização de Devolução','Devolução Autorizada Mi','Mercadoria devolvida ao cliente origem') then'Devolução Autorizada'
		 WHEN edi.descricao IN ('Extravio','Avaria') THEN 'Extravio/Avaria'
		 WHEN edi.descricao IN ('Entregue') THEN 'Entregue'
         WHEN fc.descricao IN ('CANCELADO') THEN 'Cancelado'
	END)  AS Status_Bot,

   -- sm.estorno              as Estorno,
   -- DATE(ipv.data_entregue) as Data_Entregue_Item,

	CONCAT(pv.id,fat.numero_nota_fiscal) AS PedidoNOTAMM,
    edi.descricao as Status_Nota,
    fc.descricao as Status_Portal,
	sra.id AS Split,
	sr.id AS reclamacao,
    ss.descricao AS Setor,
	pv.id AS Pedido,
    ipv.codigo_sku as SKU,
	fat.numero_nota_fiscal AS NF_Madeira,
	tc.numero_nota_fiscal AS nf_fornecedor,
	pf.nome_fantasia AS Fornecedor,
--     case
-- 		when (tc.numero_nota_fiscal is null OR pf.nome_fantasia is null) then fat.numero_nota_fiscal
-- 		when (tc.numero_nota_fiscal is NOT NULL AND pf.nome_fantasia is NOT NULL) THEN concat(tc.numero_nota_fiscal,'/',pf.nome_fantasia)
-- 	end as 'Nota_Busca_Eagle',
	sm.descricao AS Motivo,
	-- rcct.descricao AS caso_critico,
    -- DATE(pv.data_entrega_cliente) AS Data_Limite_cliente,
    -- DATE(IF(rcc.idfk_tipo = 2, rcc.data_criacao, '')) AS data_criacao_reclame_aqui,
    -- DATE(sra.data_criacao) as data_criacao,
	-- pfi.descricao as Origem,
	-- pv.cliente_cidade AS Cliente_Cidade
    pv.cliente_cep AS Cliente_CEP
FROM
    sac_reclamacao_andamento AS sra
        INNER JOIN
    sac_reclamacao AS sr ON sr.id = sra.idfk_reclamacao
        INNER JOIN
    sac_setor AS ss ON ss.id = sra.idfk_setor
        INNER JOIN
    sac_item AS si ON si.idfk_sac = sr.id
        INNER JOIN
    portal_item_pedido_venda AS ipv ON si.idfk_item = ipv.id
        INNER JOIN
    portal_pedido_venda AS pv ON pv.id = ipv.idfk_pedido_venda
		LEFT JOIN
	portal_item_pedido_venda_status_fc fc ON ipv.novo_status_fc = fc.id
        INNER JOIN
    portal_fornecedor AS pf ON pf.id = ipv.idfk_fornecedor
        LEFT JOIN
    portal_market_place_loja AS l ON l.id = pv.idfk_loja
        LEFT JOIN
    portal_market_place_grupo AS g ON g.id = l.idfk_grupo
        LEFT JOIN
    sac_reclamacao_caso_critico AS rcc ON rcc.idfk_reclamacao = sr.id
        LEFT JOIN
    sac_reclamacao_caso_critico_tipo AS rcct ON rcct.id = rcc.idfk_tipo
        LEFT JOIN
    sac_usuario AS su ON su.id = sra.idfk_usuario
        LEFT JOIN
    sac_acao_reclamacao AS sar ON sar.idfk_reclamacao = sr.id
        LEFT JOIN
    sac_acao AS sa ON sa.id = sar.idfk_acao
        LEFT JOIN
    portal_item_faturamento AS ifat ON ifat.id = (SELECT
            ifat2.id
        FROM
            portal_item_faturamento ifat2
        WHERE
            ifat2.idfk_item_pedido_venda = ipv.id
        ORDER BY ifat2.id DESC
        LIMIT 1)
        LEFT JOIN
    portal_faturamento AS fat ON ifat.idfk_faturamento = fat.id
        INNER JOIN
    sac_motivo AS sm ON sm.id = sr.idfk_motivo
        INNER JOIN
    sac_reclamacao_andamento_status sras ON sra.idfk_status = sras.id
        LEFT JOIN
    portal_transportadora_coleta tc ON tc.idfk_faturamento = fat.id
        LEFT JOIN
    portal_transportadora_filial fi ON tc.idfk_transportadora = fi.id
        LEFT JOIN
    portal_transportadora t ON fi.idfk_transportadora = t.id
        LEFT JOIN
    portal_transportadora ptc ON ptc.id = tc.idfk_transportadora_coleta
        LEFT JOIN
    portal_transportadora_coleta_edi_retorno retu ON retu.id = tc.idfk_ultimo_retorno
        LEFT JOIN
    portal_transportadora_coleta_edi_retorno AS ret ON ret.id = (SELECT
            MAX(id) AS id
        FROM
            portal_transportadora_coleta_edi_retorno
        WHERE
            idfk_transportadora_coleta = tc.id)
        LEFT JOIN
    portal_transportadora_coleta_edi_ocorrencia AS edi ON ret.idfk_transportadora_ocorrencia = edi.id
        LEFT JOIN
    portal_transportadora ptu ON ptu.id = retu.idfk_transportadora_filial
        LEFT JOIN
    portal_cidade pc ON pc.id = pv.idfk_cidade
		LEFT JOIN
	portal_filial pfi on pfi.id = fat.idfk_filial

WHERE
		((sra.idfk_setor IN (7, 8, 15, 16, 4, 17) AND sra.idfk_status in (1, 2, 7, 9, 14)) OR ((sra.idfk_setor IN (4, 17) AND sra.idfk_status in (4))))
        AND sra.data_criacao > DATE('20220101')
        AND sm.id in (123, 15, 108, 22, 140, 147, 26, 30, 87, 88, 90, 91, 111, 137, 131, 106, 131, 106)
GROUP BY sra.id
'''
solicitacao = pd.read_sql_query(query, engine_solicitacao)
cancelamentos = solicitacao.copy()
del solicitacao

cancelamentos.to_csv(
    r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\Cancelamentos.csv',
    sep=';', index=False)
print(cancelamentos.head())

print('fim consulta sql - base cancelados', datetime.today().strftime('%H:%M'))

#######################################################################################################################
#                                                  DEFS ACABAM AQUI                                                   #
#######################################################################################################################

#######################################################################################################################
#                                                   LÊ CSV BASE BACKOFFICE
#######################################################################################################################


endereco = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares'

old_file = os.path.join(endereco, "Consulta_BackOffice_-_NOVO_data.csv")
new_file = os.path.join(endereco, "Back_Office_Novo.csv")
shutil.copy(old_file, new_file)
##os.rename(old_file, new_file)
print("Arquivo Renomeado")

main()

# DATABASE - BACK
print("Backoffice")

backOffice_df = pd.read_csv(
    r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\Back_Office_Novo.csv',
    sep=';')

d = datetime.today().strftime('%d/%m/%y')
backOffice_df['Hoje'] = d
backOffice_df['Contador'] = 1
print('Gera Contador e Coluna com a data de hoje')

#######################################################################################################################
#                                                  CONVERTE COLUNAS EM DATAS                                          #
#######################################################################################################################

lista = ["Data_criacao", "Data_Emissao_CCe", "Data_Botao_CCe", "Data_Expedicao", "Data_Limite_Cliente_Item", "Data_BIP",
         "Data_Limite_Cliente_Pediddo", "Data_Prometida", "Data_criacao", "Data_criacao_reclame_aqui", "Data_entregue",
         "Data_fechamento", "Data_ultima_rota", "Hora_criacao", "Hora_fechamento",
         "data_criacao_split"]

for d in lista:
    backOffice_df[d] = pd.to_datetime(backOffice_df[d], infer_datetime_format=True, dayfirst=True)

# backOffice_df['Data_Botao_CCe'] = pd.to_datetime(backOffice_df['Data_Botao_CCe']).strftime('%d/%m/%y')

for y in backOffice_df.columns:
    if (is_string_dtype(backOffice_df[y])):
        backOffice_df[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(backOffice_df[y])):
        backOffice_df[y].fillna(blank, inplace=True)

# renomeia as colunas
# backOffice_df.rename(columns = {"Transportadora_Last_Mile": "24. Transp. Entrega" } , inplace = True)

#######################################################################################################################
#                                                   COMEÇA MERGES
#######################################################################################################################

backOffice_df = backOffice_df.convert_dtypes()
backOffice_df = backOffice_df.convert_dtypes()

# Proc com TP de Para
list = ['transportadora_responde_chamado', 'De para']
transportadora_entrega_df = pd.read_excel(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\deParaTP.xlsx",
    usecols=list)
transportadora_entrega_df.rename(columns={'transportadora_responde_chamado': 'Transportadora_Last_Mile'}, inplace=True)
transportadora_entrega_df = ColunasCalculadas.ajustaLetras(transportadora_entrega_df)
backOffice_df = pd.merge(backOffice_df, transportadora_entrega_df, on='Transportadora_Last_Mile', how='left')
print('Proc TP de_para')

print(transportadora_entrega_df)

# preenchendo os espaços vazios de nota com um numero qualquer
backOffice_df['NF_Fornecedor'] = backOffice_df['NF_Fornecedor'].fillna(12399999999)
backOffice_df['NF_Madeira'] = backOffice_df['NF_Madeira'].fillna(12399999999)

# criando coluna Chave PV+SKU
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='Pedido & SKU',
                     value=backOffice_df['Pedido'].astype(str) + backOffice_df['SKU'].astype(str))
print(backOffice_df.shape[0], "linhas")

# criando coluna OrigemCidade
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='OrigemCidade',
                     value=backOffice_df['Origem'].astype(str) + backOffice_df['Cliente_Cidade'].astype(str))
print(backOffice_df.shape[0], "linhas")

# criando coluna Chave Pedido+NF+SPLIT
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='Chave Pedido+NF+SPLIT',
                     value=backOffice_df['Pedido'].astype(str) + backOffice_df['NF_Madeira'].astype(str) +
                           backOffice_df['Split'].astype(str))

# criando coluna Chave pedido e nota
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='Chave Pedido e Nota',
                     value=backOffice_df['Pedido'].astype(str) + backOffice_df['NF_Madeira'].astype(str))
print(backOffice_df.shape[0], "linhas")

# chave para proc Parceiros_Bulky
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='chave_parceiros', value=(
            backOffice_df['Cliente_Cidade'].astype(str) + backOffice_df['UF'].astype(str) + backOffice_df[
        'Transportadora_Last_Mile'].astype(str)))

# Proc com Parceiros_Bulky)
list = ['chave_parceiros', 'Transportadora Parceiro']
parceiros_bulky_df = pd.read_csv(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Parceiros_Bulky.csv",
    sep=';', usecols=list)
parceiros_bulky_df = ColunasCalculadas.ajustaLetras(parceiros_bulky_df)
backOffice_df = pd.merge(backOffice_df, parceiros_bulky_df, on='chave_parceiros', how='left')

# Proc - diário de bordo (GIRO - DB ENTREGAS)
list = ['Chave', 'Data de Entrada', 'Tratativa', 'Observação', 'Vencimento']
base_diario = pd.read_excel(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\00- Diários_Novos\Rotina_Atualização_Geral.xlsm",
    sheet_name="Giro", usecols=list)
base_diario.rename(columns={'Chave': 'Chave Pedido e Nota'}, inplace=True)
base_diario.rename(columns={'Data de Entrada': 'Data de Entrada - Diario'}, inplace=True)
base_diario.rename(columns={'Tratativa': 'Tratativa - Diario'}, inplace=True)
base_diario.rename(columns={'Observação': 'Observacao - Diario'}, inplace=True)
base_diario.rename(columns={'Vencimento': 'Vencimento - Diario'}, inplace=True)
base_diario = base_diario.convert_dtypes()  # ajustando os tipos da colunas
backOffice_df = pd.merge(backOffice_df, base_diario, on='Chave Pedido e Nota', how='left')
base_diario = pd.DataFrame()

# Proc com Motivo Finaliza SAC
motivosac_df = pd.read_csv(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\Motivos_SAC.csv",
    sep=';')
motivosac_df = ColunasCalculadas.ajustaLetras(motivosac_df)
motivosac_df.rename(columns={'Sac Finalização': 'Pedidos para Finalizar [bater com o giro]'}, inplace=True)
backOffice_df = pd.merge(backOffice_df, motivosac_df, on='Motivo', how='left')

# Proc com base - cancelamentos
list = ['PedidoNOTAMM', 'Split']
cancel_sac_df = pd.read_csv(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\Cancelamentos.csv",
    sep=';', usecols=list)
cancel_sac_df = ColunasCalculadas.ajustaLetras(cancel_sac_df)
cancel_sac_df.rename(columns={'PedidoNOTAMM': 'Chave Pedido e Nota'}, inplace=True)
cancel_sac_df.rename(columns={'Split': 'Split (Cancelamentos)'}, inplace=True)
backOffice_df = pd.merge(backOffice_df, cancel_sac_df, on='Chave Pedido e Nota', how='left')


# Proc com - Listagens de Pendencias Eagle
list = ['Split_corrigido', 'No Eagle']
listagem_df = pd.read_excel(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\listagem-pendencias.xlsx",
    usecols=list)
listagem_df = ColunasCalculadas.ajustaLetras(listagem_df)
listagem_df.rename(columns={'Split_corrigido': 'Split'}, inplace=True)
listagem_df.rename(columns={'No Eagle': '[Flag]Disp. Eagle'}, inplace=True)
backOffice_df = pd.merge(backOffice_df, listagem_df, on='Split', how='left')

# Proc com Fase da Nota
list = ['Chave Pedido e Nota', 'Fase Nota', 'Contador', '46. Data Ultimo EDI', '54. Data CTRC Emitido T3']
fasenota_df = pd.read_csv(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\02. BasesDia\D_0 - 08h.csv", sep=';',
    usecols=list)
fasenota_df.rename(columns={'Contador': 'Conta'}, inplace=True)
fasenota_df.rename(columns={'46. Data Ultimo EDI': 'Data_Ultima Movimentação'}, inplace=True)
fasenota_df = ColunasCalculadas.ajustaLetras(fasenota_df)
backOffice_df = pd.merge(backOffice_df, fasenota_df, on='Chave Pedido e Nota', how='left')
print('Proc Fase Nota')

### MERGE COM BASE DE PRAZOS TUY ###

##ABRE A ULTIMA PLANILHA DE PRAZOS DO TUY NOS ULTIMOS 30 DIAS
datas_tuy = getNdays_asString(30)
columns_direta = ['origem', 'cidade', 'prazo', 'transp_t3']
columns_redespacho = ['origem', 'cidade', 'prazo', 'prazo_redespacho', 'transp_t3']
tuy_diretas = 0
for data in datas_tuy:
    try:
        file_path = r'G:\Drives compartilhados\MM - Logística  - PCT\14 - PCT\03 - Planejamento\4.0 Prazos + Conting e BL\Prazos\Histórico de prazos prometidos\Relatório Tuy 08-2021\Faturamento_' + data + ' Relat Prazos_NV.xlsx'
        tuy_diretas = pd.read_excel(file_path, usecols=columns_direta, sheet_name='Diretas')
        print("leu diretas")
        tuy_redespacho = pd.read_excel(file_path, usecols=columns_redespacho, sheet_name='Redespacho')
        print("leu", data)
        break
    except:
        continue

###CRIA COLUNA CHAVE NA BASE DO TUY - DIRETAS
tuy_diretas['chave_join'] = (tuy_diretas['origem'] + tuy_diretas['cidade'] + tuy_diretas['transp_t3']).str.upper()
tuy_diretas.drop_duplicates('chave_join', inplace=True)
tuy_diretas['Prazo Tuy Direta'] = tuy_diretas['prazo']
tuy_diretas.drop(columns=columns_direta, inplace=True)

###CRIA COLUNA CHAVE NA BASE DO TUY - REDESPACHO
tuy_redespacho['chave_join'] = (
        tuy_redespacho['origem'] + tuy_redespacho['cidade'] + tuy_redespacho['transp_t3']).str.upper()
tuy_redespacho.drop_duplicates('chave_join', inplace=True)
tuy_redespacho['Prazo Tuy Redespacho'] = tuy_redespacho['prazo'] + tuy_redespacho['prazo_redespacho']
tuy_redespacho.drop(columns=columns_redespacho, inplace=True)

###CRIA COLUNA CHAVE NA BASE DE SACS
backOffice_df['chave_join'] = (backOffice_df['Origem'] + backOffice_df['Cliente_Cidade'] + backOffice_df[
    'Transportadora_Last_Mile']).str.upper()
# MERGE COM DIRETAS
backOffice_df = pd.merge(backOffice_df, tuy_diretas, how='left', on=['chave_join'])
# MERGE COM REDESPACHO
backOffice_df = pd.merge(backOffice_df, tuy_redespacho, how='left', on=['chave_join'])
# CRIA A COLUNA DEFINITIVA DE PRAZO
backOffice_df['Prazo_Pos_Expedicao'] = pd.Series(
    np.where(pd.isna(backOffice_df['Prazo Tuy Redespacho']), backOffice_df['Prazo Tuy Direta'],
             backOffice_df['Prazo Tuy Redespacho']), dtype='Int32')
backOffice_df['Prazo_Pos_Expedicao'] = backOffice_df.apply(
    lambda row: diaUtil_aposNDias(row['Hoje'], row['Prazo_Pos_Expedicao']), axis=1)
# ALTERADO #
backOffice_df['Prazo_Pos_Expedicao'] = backOffice_df['Prazo_Pos_Expedicao'].astype('datetime64')
# EXCLUI COLUNAS DESNECESSÁRIAS
backOffice_df.drop(columns=['chave_join', 'Prazo Tuy Redespacho', 'Prazo Tuy Direta'], inplace=True)

# RELEASE MEMORY
tuy_diretas = None
tuy_redespacho = None

#######################################################################################################################
#                                                  MERGES ACABAM AQUI                                                 #
#######################################################################################################################


for y in backOffice_df.columns:
    if (is_string_dtype(backOffice_df[y])):
        backOffice_df[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(backOffice_df[y])):
        backOffice_df[y].fillna(blank, inplace=True)

backOffice_df = backOffice_df.drop_duplicates(subset=['Chave Pedido+NF+SPLIT'], keep='first', ignore_index=True)

#######################################################################################################################
#                             C O M E Ç A R   O S   C A L C U L O S   A Q U I
#######################################################################################################################
# calculo -- flag abertofechado
backOffice_df['flag aberto fechado'] = backOffice_df.apply(lambda row: reclamacaoAberto(row['Data_fechamento'],
                                                                                        row['Motivo'],
                                                                                        row['Data_Botao_CCe']), axis=1)

# Data Max RA --- CONSIDERA A MAIOR DATA ENTRE DATA RECLAME AQUI E DATA CRIAÇÃO SAC
backOffice_df["Data Max RA"] = backOffice_df.apply(lambda row: maiorData(row["Data_criacao_reclame_aqui"],
                                                                         row["Data_criacao"]), axis=1)
print("Data Max RA")

# origem só com as 2 primeiras letras
backOffice_df['OrigemLeft'] = backOffice_df['Origem'].str[:2]
backOffice_df['OrigemLeft'] = backOffice_df['OrigemLeft'].astype(str)

# [Flag]Etapa Transporte
backOffice_df['[Flag]Etapa Transporte'] = np.where((pd.isnull(backOffice_df['Data_Expedicao'])), 'Sem expedição',
                                                   np.where(backOffice_df['SAC_Etapa_Transporte'] == "T2", "Redespacho",
                                                            "Last Mile"))
print('[Flag]Etapa Transporte')

# DePara Transportadora -- TRAZ A TP SAC
backOffice_df["TRANSPORTADORA_unificada"] = backOffice_df.apply(lambda row: transportadora(
    row['Split'],
    row['Data_Expedicao'],
    row['OrigemLeft'],
    row['Fornecedor'],
    row['54. Data CTRC Emitido T3'],
    row['Data_entregue'],
    row['Reserva'],
    row['OC'],
    row['Origem'],
    row['Data_BIP'],
    row['Transportadora_Responde_EAGLE'],
    row['Transportadora_Ultima_Movimentacao'],
    row['Transportadora_Last_Mile'],
    row['Transportadora_Coleta']
), axis=1)

print("TRANSPORTADORA_unificada")

# transpdepara, igual o calculo anterior mas retorna "sem transportadora" para o que for NULL
backOffice_df['TRANSPORTADORA_DEPARA_aux'] = backOffice_df['TRANSPORTADORA_unificada'].copy()
print("TRANSPORTADORA_DEPARA_aux")

# Proc com TP de Para
list = ['transportadora_responde_chamado', 'De para']
transportadora_entrega_df2 = pd.read_excel(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\deParaTP.xlsx",
    usecols=list)
transportadora_entrega_df2.rename(columns={'transportadora_responde_chamado': 'TRANSPORTADORA_DEPARA_aux'},
                                  inplace=True)
transportadora_entrega_df2.rename(columns={'De para': 'De para (2)'}, inplace=True)
transportadora_entrega_df2 = ColunasCalculadas.ajustaLetras(transportadora_entrega_df2)
backOffice_df = pd.merge(backOffice_df, transportadora_entrega_df2, on='TRANSPORTADORA_DEPARA_aux', how='left')
print('Proc TP de_para2')

print(transportadora_entrega_df2)

for y in backOffice_df.columns:
    if (is_string_dtype(backOffice_df[y])):
        backOffice_df[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(backOffice_df[y])):
        backOffice_df[y].fillna(blank, inplace=True)

# transpdepara, igual o calculo anterior mas retorna "sem transportadora" para o que for NULL
backOffice_df['TRANSPORTADORA_DEPARA_aux'] = backOffice_df['TRANSPORTADORA_unificada']
print("TRANSPORTADORA_DEPARA_aux")

# transpdepara, igual o calculo anterior mas retorna "sem transportadora" para o que for NULL
backOffice_df['TRANSPORTADORA_DEPARA'] = backOffice_df.apply(
    lambda row: dexpara_tp(row['TRANSPORTADORA_unificada'], row['De para (2)']), axis=1)
print("TRANSPORTADORA_DEPARA")

# criando coluna Cidade Transportadora
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='CidadeTransportadora',
                     value=backOffice_df['Cliente_Cidade'].astype(str) + backOffice_df['TRANSPORTADORA_DEPARA'].astype(
                         str))
print(backOffice_df.shape[0], "linhas")
# print("Chave CidadeTransportadora", " - %s" % (time.time() - start_time), " - segundos" )

# TP_depara_parceiros
backOffice_df['TP_depara_parceiros'] = backOffice_df.apply(
    lambda row: tpdepara(row['TRANSPORTADORA_DEPARA'], row['Transportadora Parceiro'], row['Data_Expedicao']), axis=1)
print("TP_depara_parceiros")

# Proc com TP de Para
list = ['transportadora_responde_chamado', 'Analista Ultima TP']
transportadora_entrega_dfp = pd.read_excel(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\deParaTP.xlsx",
    usecols=list)
transportadora_entrega_dfp.rename(columns={'transportadora_responde_chamado': 'TP_depara_parceiros'}, inplace=True)
transportadora_entrega_dfp = ColunasCalculadas.ajustaLetras(transportadora_entrega_dfp)
backOffice_df = pd.merge(backOffice_df, transportadora_entrega_dfp, on='TP_depara_parceiros', how='left')
print('Proc TP de_para_Parceiros')

print(transportadora_entrega_dfp)
print(backOffice_df['Analista Ultima TP'])

backOffice_df['Dias'] = backOffice_df.apply(
    lambda row: dias(row['flag aberto fechado'], row['Data_criacao'], row['Data_fechamento'], row['Data_Botao_CCe'],
                     row['Motivo'], row['Hoje']), axis=1)
print("Dias")

backOffice_df['Dias RA'] = backOffice_df.apply(lambda row: dias_RA(row['Caso_Critico'], row['flag aberto fechado'],
                                                                   row['Hoje'], row['Data Max RA'],
                                                                   row['Data_fechamento']), axis=1)
print("Dias RA")

backOffice_df['Cluster'] = backOffice_df.apply(lambda row: clusterDUTIL(row['Dias']), axis=1)
print("Cluster")

backOffice_df['Cluster Ra'] = backOffice_df.apply(lambda row: clusterRa(row['Dias RA'], row['Prazo_SLA']), axis=1)
print("Cluster Ra")

backOffice_df['Confere_finsdesemana'] = backOffice_df.apply(lambda row: diasemana(row['Data Max RA']), axis=1)
print("Confere_finsdesemana")

backOffice_df['PassouOntem'] = backOffice_df.apply(lambda row: passouontem(row['flag aberto fechado'],
                                                                           row['Dias'], row['Prazo_SLA']), axis=1)
print("PassouOntem")

# clusterCarta(flag, datacriacao, hoje):
backOffice_df['Cluster Carta Correcao'] = backOffice_df.apply(lambda row: clusterCarta(row['flag aberto fechado'],
                                                                                       row['Data_criacao'],
                                                                                       row['Hoje']), axis=1)
print("Cluster Carta Correcao")

# clusterCarta(flag, datacriacao, hoje):
backOffice_df['SLA FECHADO'] = backOffice_df.apply(lambda row: slafechado(row['flag aberto fechado'],
                                                                          row['Dias'],
                                                                          row['Prazo_SLA']), axis=1)
print("SLA FECHADO")

backOffice_df["Responsavel"] = backOffice_df["Responsavel"].str.title()
# Proc com TP de Para
analista_df = pd.read_csv(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\AnalistaBO.csv",
    sep=';')
backOffice_df = pd.merge(backOffice_df, analista_df, on='Responsavel', how='left')
print('Proc Analista BO')

for y in backOffice_df.columns:
    if (is_string_dtype(backOffice_df[y])):
        backOffice_df[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(backOffice_df[y])):
        backOffice_df[y].fillna(blank, inplace=True)

# criando coluna Cidade&UF&TP
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='Cidade&UF&TP',
                     value=backOffice_df['Cliente_Cidade'].astype(str) + backOffice_df['UF'].astype(str) +
                           backOffice_df['De para'].astype(str))
print(backOffice_df.shape[0], "linhas")

print(backOffice_df['Analista Ultima TP'])

## Definindo Analista Responsavel
backOffice_df['Analista Responsavel'] = np.where(backOffice_df["SAC_Etapa_Transporte"] == "T2",
                                                 backOffice_df["Analista Ultima TP"],
                                                 np.where(backOffice_df["Analista BO"] == 1,
                                                          backOffice_df["Responsavel"],
                                                          backOffice_df["Analista Ultima TP"]))
print(backOffice_df['Analista Responsavel'])

backOffice_df = backOffice_df.drop(columns=['Analista BO'])
backOffice_df = backOffice_df.convert_dtypes()
backOffice_df = backOffice_df.convert_dtypes()

############################################################### NOVA REGRA BACK #######################################################################

# Chave CidadeTransportadora
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='Cidade Transportadora',
                     value=backOffice_df['Cliente_Cidade'].astype(str) + backOffice_df['UF'].astype(str) +
                           backOffice_df['De para'].astype(str))
print(backOffice_df.shape[0], "linhas")

# [Finalizar] Motivos Entregues
backOffice_df['[Finalizar] Motivos Entregues'] = backOffice_df.apply(lambda row: motivoentregues(row["Motivo"],
                                                                                                 row["Data_entregue"]),
                                                                     axis=1)
print('Motivos Entregues - ok')

# [Finalizar] Conferencia/Nentregue
backOffice_df['[Finalizar] Conferencia/Nentregue'] = backOffice_df.apply(lambda row: confereentregue(row["Motivo"],
                                                                                                     row["Dias"]),
                                                                         axis=1)
print('[Finalizar] Conferencia/Nentregue - ok')

# Motivos sac para finalizar com data
backOffice_df['Motivos sac para finalizar com data'] = np.where((backOffice_df["Motivo"] == 'Atraso na Entrega') |
                                                                (backOffice_df[
                                                                     "Motivo"] == 'Sem acesso ao rastreio do pedido'),
                                                                1, 0)
print('motivos para finalizar - ok')

# flag aberto
backOffice_df['[flag]Aberto'] = np.where(pd.isnull(backOffice_df["Data_fechamento"]), 1, 0)
print('flag aberto')

# Regra Expedição -- se a data da expedição for vazia e o setor for diferente de transportadora, traz "Pendencia TC"
backOffice_df["Regra Expedicao"] = backOffice_df.apply(lambda row: regraexped(row["Data_Expedicao"],
                                                                              row["Setor"]), axis=1)

backOffice_df['SLA Invertido'] = np.where(backOffice_df["SLA FECHADO"] == 1, 0, 1)

##TP EAGLE
backOffice_df['tp eagle'] = np.where(backOffice_df['Acao_EAGLE'] == 1, "TP", blank_str)

# Alegação de não entregue + Conferencia > prazo [FINALIZAR]
backOffice_df['Alegação de não entregue + Conferencia > prazo [FINALIZAR]'] = backOffice_df.apply(
    lambda row: alegacao(row["Motivo"], row['Dias']), axis=1)
print('alegação de n entregue ok')

# Data ultima movimentação SEM DIAS ÚTEIS [info: Base do Giro]
backOffice_df['Aging Ult. Movimentacao [DC]'] = backOffice_df.apply(
    lambda row: diascorridos(row["Hoje"], row['Data_Ultima Movimentação']), axis=1)
backOffice_df['Aging Ult. Movimentacao [DC]'] = backOffice_df['Aging Ult. Movimentacao [DC]'].astype('Int64')
print('dias corridos')

backOffice_df1 = backOffice_df.drop(backOffice_df[(backOffice_df['flag aberto fechado'] == 'Fechado')].index)
backOffice_df['Pedidos Duplicados'] = backOffice_df['Pedido & SKU'].map(backOffice_df1['Pedido & SKU'].value_counts())
# print(backOffice_df)

# Pedidos duplicados para finalizar
backOffice_df['Pedidos Duplicados [FINALIZAR]'] = backOffice_df.apply(
    lambda row: peddupliF(row["Pedidos Duplicados"], row['flag aberto fechado']), axis=1)
print('pds com reclamações duplicadas2')

# Atraso + Link Entregues [FINALIZAR]
backOffice_df['Atraso + Link Entregues [FINALIZAR]'] = backOffice_df.apply(
    lambda row: atrasolink(row['Pedidos Duplicados [FINALIZAR]'], row['Motivo'], row['Data_entregue']), axis=1)

print('Atraso + Link Entregues [FINALIZAR]')

# [Finalizar]EDI Atualizado
backOffice_df['[Finalizar]EDI Atualizado'] = backOffice_df.apply(
    lambda row: ediatualizado(row["Aging Ult. Movimentacao [DC]"], row['Motivo']), axis=1)
print('EDI Atualizado Finalizar')

# [Finalizar]Produto Cancelado
backOffice_df['[Finalizar]Produto Cancelado'] = backOffice_df.apply(
    lambda row: prodcancelado(row["Split (Cancelamentos)"], row['Pedidos para Finalizar [bater com o giro]'],
                              row['Motivo']), axis=1)
print('Produto Cancelado')

# [Finalizar] Alegação extraviada
backOffice_df['[Finalizar] Alegação extraviada'] = backOffice_df.apply(
    lambda row: alegacaoextraviada(row["Motivo"], row['Status_Portal']), axis=1)
print('Alegação extraviada')

# Tratativa NEW
backOffice_df['[DB ]TRATATIVA NEW'] = backOffice_df.apply(lambda row: dbnew(row["Motivo"], row['Tratativa - Diario']),
                                                          axis=1)
print('[DB ]TRATATIVA NEW')

# [Finalizar] Plano de entrega do Roger
backOffice_df['Plano [Base Roger]'] = pd.NaT
print(backOffice_df['Plano [Base Roger]'].dtype)

# #Plano [Base Roger]
# backOffice_df['Plano [Base Roger]'] = np.where(backOffice_df['Motivo'] == "Atraso na Entrega", backOffice_df['Aux. Plano (Roger)'], blank_str)
# print('Plano [Base Roger]')

# [Finalizar] Plano de entrega do Roger
backOffice_df['[Finalizar] Plano de entrega do Roger'] = backOffice_df.apply(
    lambda row: planoT3(row["Plano [Base Roger]"], row['Hoje']), axis=1)
print(backOffice_df['[Finalizar] Plano de entrega do Roger'].dtype)
print('[Finalizar] Plano de entrega do Roger')

##### COLUNAS LUIZ #######

# [Finalizar] Expedido com data TUY
backOffice_df['[Finalizar] Expedido com data TUY'] = backOffice_df.apply(
    lambda row: finalizar_prazoTUY(row['Hoje'], row['Data_Expedicao'], row['Motivo'], row['Prazo_Pos_Expedicao']),
    axis=1)
print('[Finalizar] Expedido com data TUY')

# [Finalizar] Alegação extraviada
backOffice_df['[Finalizar] Alegação entregue 2021'] = backOffice_df.apply(
    lambda row: finalizar_alegacaoantiga(row["Motivo"], row['Data_entregue']), axis=1)
print('Alegação extraviada')

# [Finalizar] Criação OC sem Extravio/Avaria
backOffice_df['[Finalizar]Criação OC com status inválido'] = backOffice_df.apply(
    lambda row: ocinvalida(row['Motivo'], row['Status_Portal']), axis=1)
print('Criação OC')

# Finalizações [BOT]
backOffice_df['Finalizações [BOT]'] = backOffice_df.apply(
    lambda row: bot_finaliza(row['Ultimo_EDI'], row['Motivo'], row['Status_Portal'],
                             row['[Finalizar] Motivos Entregues'], row['[Finalizar]Produto Cancelado'],
                             row['[Finalizar] Alegação extraviada'], row['[DB ]TRATATIVA NEW'],
                             row['Vencimento - Diario'], row['Hoje'],
                             row['[Finalizar] Plano de entrega do Roger'], row['Motivos sac para finalizar com data'],
                             row['[Finalizar]EDI Atualizado'],
                             row['[Finalizar] Expedido com data TUY'], row['[Finalizar] Alegação entregue 2021'],
                             row['[Finalizar]Criação OC com status inválido']), axis=1)
print('Finalizações [BOT]')

# [Finalizar] Ações (BOT)
backOffice_df['[Finalizar] Ações (BOT)'] = backOffice_df.apply(lambda row: bot_acoes(row["Finalizações [BOT]"]), axis=1)
print('[Finalizar] Ações (BOT)')

# [Finalizar] Justificativa (BOT)
backOffice_df['[Finalizar] Justificativa (BOT)'] = backOffice_df.apply(
    lambda row: bot_justifica(row["Finalizações [BOT]"]), axis=1)
print('[Finalizar] Justificativa (BOT)')

# [Finalizar] Data entrega (BOT)
backOffice_df['[Finalizar] Data entrega (BOT)'] = backOffice_df.apply(
    lambda row: bot_dataentrega(row['[Finalizar] Plano de entrega do Roger'], row['Vencimento - Diario'],
                                row['Prazo_Pos_Expedicao'], row['Hoje']), axis=1)
print('Data entrega (BOT)')

# [Flag]Pendencia TC
backOffice_df['[Flag]Pendencia TC'] = backOffice_df.apply(lambda row: pendenciatc(row['Motivo'], row['Data_Botao_CCe']),
                                                          axis=1)
print('[Flag]Pendencia TC')

# [Flag]Aguardando chegada CCE
backOffice_df['[Flag]Aguardando chegada CCE'] = backOffice_df.apply(
    lambda row: agchegadaccd(row['Motivo'], row['Data_Botao_CCe'], row['Data_Emissao_CCe']), axis=1)
print('Flag]Aguardando chegada CCE')

# [Flag]Problemas Eagle
backOffice_df['[Flag]Problemas Eagle'] = backOffice_df.apply(
    lambda row: problemaseagle(row['Motivo'], row['Data_Emissao_CCe'], row['[Flag]Disp. Eagle'], row['Eagle_Tp']),
    axis=1)
print('[Flag]Problemas Eagle')

# Responsabilidade
backOffice_df['Responsabilidade'] = backOffice_df.apply(
    lambda row: responsabilidade(row['[Flag]Pendencia TC'], row['[Flag]Aguardando chegada CCE'],
                                 row['[Flag]Problemas Eagle'], row['Setor']), axis=1)
print('Responsabilidade')

# Carta de correção [Sem Botão]
backOffice_df['Carta de correção [Sem Botão]'] = backOffice_df.apply(
    lambda row: cartasem(row['Data_Emissao_CCe'], row['Motivo']), axis=1)
print('Carta de correção [Sem Botão]')

# Transportadora
backOffice_df['Transportadora'] = backOffice_df.apply(lambda row: visaotp(row['[Flag]Disp. Eagle']), axis=1)
print('Transportadora')

# Finalizações
backOffice_df['Finalizações'] = backOffice_df.apply(lambda row: finalizacoes(row['[Finalizar]EDI Atualizado'], row[
    'Alegação de não entregue + Conferencia > prazo [FINALIZAR]'], row['Atraso + Link Entregues [FINALIZAR]']), axis=1)

print('finalizações')

backOffice_df['Cluster CCE [Aguardando solicitação]'] = backOffice_df.apply(
    lambda row: clustercce(row['Data_criacao'], row['Hoje'], row['Prazo_SLA']), axis=1)
print("Cluster CCE [Aguardando solicitação]")

backOffice_df['tplm1'] = backOffice_df['Transportadora_Last_Mile'].str[:3]
backOffice_df['tpmov1'] = backOffice_df['Transportadora_Ultima_Movimentacao'].str[3:]
backOffice_df['tplm2'] = backOffice_df['Transportadora_Last_Mile'].str[:5]
backOffice_df['tpmov2'] = backOffice_df['Transportadora_Ultima_Movimentacao'].str[5:]

backOffice_df['Validação [redespacho/Lastmile]'] = backOffice_df.apply(
    lambda row: validaT2_T3(row['Transportadora_Last_Mile'], row['Transportadora_Ultima_Movimentacao'],
                            row['tplm1'], row['tpmov1'],
                            row['tplm2'], row['tpmov2']), axis=1)
print("Validação [redespacho/Lastmile]")

backOffice_df['cce-t2'] = backOffice_df.apply(
    lambda row: ccet2(row['Transportadora_Coleta'], row['Transportadora_Ultima_Movimentacao']), axis=1)
print("cce-t2")

backOffice_df['Carta de correção [Sem Botão]'] = backOffice_df.apply(
    lambda row: ccesembot(row['Data_Emissao_CCe'], row['Motivo'], row['cce-t2'], row['Data_Botao_CCe']), axis=1)
print("Carta de correção [Sem Botão]")

# [Cluster]Prazo Atendimento
backOffice_df['[Cluster]Prazo Atendimento'] = backOffice_df.apply(
    lambda row: prazoatendimento(row['cce'], row['Dias'], row['Prazo_SLA'], row['Data_Emissao_CCe'],
                                 row['Motivo'], row['Validação [redespacho/Lastmile]'],
                                 row['Data_Botao_CCe'], row['Setor']), axis=1)
print('[Cluster]Prazo Atendimento')

backOffice_df['Cluster SAC'] = backOffice_df.apply(lambda row: clustersac(row['PassouOntem'],
                                                                          row['Motivo'],
                                                                          row['Data_Botao_CCe'],
                                                                          row['Caso_Critico'],
                                                                          row['Cluster Ra'],
                                                                          row['Dias'],
                                                                          row['Prazo_SLA'],
                                                                          row['[Cluster]Prazo Atendimento']), axis=1)
print("Cluster SAC")

# flag atraso
backOffice_df['Flag Atraso'] = backOffice_df.apply(lambda row: flagatraso(row['Cluster SAC']), axis=1)

# SAC ETAPA + CCE
backOffice_df['SAC ETAPA + CCE'] = np.where(backOffice_df['[Cluster]Prazo Atendimento'] == "CCe aguard. chegada LM",
                                            "T2",
                                            backOffice_df['SAC_Etapa_Transporte'])
print('SAC ETAPA + CCE')

# DePara +  CD FF
backOffice_df['DePara +  CD FF'] = backOffice_df.apply(
    lambda row: deparaCD(row['TP_depara_parceiros'], row['Data_Expedicao'], row['SAC ETAPA + CCE'],
                         row['Transportadora_Responde_EAGLE'], row['Fornecedor'], row['[Cluster]Prazo Atendimento'],
                         row['De para']), axis=1)
print('DePara +  CD FF')

# NOVO ANALISTA
list = ['transportadora_responde_chamado', 'Analista Ultima TP']
novo_analista = pd.read_excel(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\deParaTP.xlsx",
    usecols=list)
novo_analista.rename(columns={'transportadora_responde_chamado': 'DePara +  CD FF'}, inplace=True)
novo_analista.rename(columns={'Analista Ultima TP': 'Novo Analista1'}, inplace=True)
novo_analista = ColunasCalculadas.ajustaLetras(novo_analista)
backOffice_df = pd.merge(backOffice_df, novo_analista, on='DePara +  CD FF', how='left')
print('novo_analista')

print(backOffice_df['Novo Analista1'])

# Analista SAC
backOffice_df['Novo Analista'] = backOffice_df.apply(
    lambda row: analistasac(row['DePara +  CD FF'], row['Fase Nota'], row['Novo Analista1']), axis=1)
print('Analista SAC')

# TRANSP LM
list = ['transportadora_responde_chamado', 'Transp. LM']
novo_analista = pd.read_excel(
    r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares\deParaTP.xlsx",
    usecols=list)
novo_analista.rename(columns={'transportadora_responde_chamado': 'DePara +  CD FF'}, inplace=True)
novo_analista = ColunasCalculadas.ajustaLetras(novo_analista)
backOffice_df = pd.merge(backOffice_df, novo_analista, on='DePara +  CD FF', how='left')
print('Transp. LM')

# #######################################################################################################################
# #                             O S   C A L C U L O S   A C A B A R A M   A Q U I
# #######################################################################################################################

print(backOffice_df['Data_Expedicao'])
print(backOffice_df['data_criacao_split'])
backOffice_df = backOffice_df.convert_dtypes()
backOffice_df = backOffice_df.replace(blank, np.nan, regex=True)
backOffice_df = backOffice_df.replace(str(blank), ' ', regex=False)

backOffice_df = backOffice_df.drop(
    columns=['Finalizações', 'Atraso + Link Entregues [FINALIZAR]', 'Pedidos Duplicados [FINALIZAR]',
             'Pedidos Duplicados',
             'Alegação de não entregue + Conferencia > prazo [FINALIZAR]', 'Regra Expedicao', 'De para (2)',
             'TRANSPORTADORA_DEPARA_aux', 'Analista Responsavel',
             'Analista Ultima TP', 'tplm1', 'tplm2', 'tpmov1', 'tpmov2', 'cce-t2', 'Novo Analista1'])

print(backOffice_df['TRANSPORTADORA_unificada'].loc[backOffice_df['Split'] == 5463627])
print(backOffice_df['DePara +  CD FF'].loc[backOffice_df['Split'] == 5459320])
print(backOffice_df['Data_Expedicao'].dtype)
backOffice_df = backOffice_df.drop_duplicates(subset=['Chave Pedido+NF+SPLIT'], keep='first', ignore_index=True)

# criando coluna com o Dia D
h = datetime.today().strftime('%H')
num_colunas = backOffice_df.shape[1]
backOffice_df.insert(loc=num_colunas, column='Dia (D)', value='D_0 - ' + h + 'h')

backOffice_df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Back_Office_Novo.csv', decimal=',', encoding="utf-8-sig",
                     sep=';', index=False)

#########################################################################################################################
list = ['Chave Pedido e Nota', 'Caso_Critico', "flag aberto fechado"]
teste_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\Back_Office_Novo.csv', sep=';', decimal=',',
                       encoding="utf-8-sig", usecols=list)

linhas = teste_df.shape[0]
for i in range(0, linhas):
    somaSACaberto = sum(teste_df[teste_df['Chave Pedido e Nota'] == teste_df['Chave Pedido e Nota'][i]][
                            'flag aberto fechado'] == 'Aberto')
    teste_df.at[i, "SACs em Aberto"] = somaSACaberto

    somaSACfechado = sum(teste_df[teste_df['Chave Pedido e Nota'] == teste_df['Chave Pedido e Nota'][i]][
                             'flag aberto fechado'] == 'Fechado')
    teste_df.at[i, "SACs Fechado"] = somaSACfechado

    somaSACcarta = sum(teste_df[teste_df['Chave Pedido e Nota'] == teste_df['Chave Pedido e Nota'][i]][
                           'flag aberto fechado'] == 'Carta de correção - Sem Botao')
    teste_df.at[i, "SACs Carta de Correcao"] = somaSACcarta

    teste_df.at[i, "Qtde SAC"] = somaSACfechado + somaSACaberto + somaSACcarta
    if somaSACaberto > 0:
        teste_df.at[i, "Flag SAC-aberto"] = 1

teste_df = ColunasCalculadas.ajustaLetras(teste_df)
teste_df = teste_df.convert_dtypes()
teste_df = teste_df.convert_dtypes()
list = ['flag aberto fechado']
teste_df = teste_df.drop(columns=list, axis=1)
teste_df = teste_df.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)
list = 0
teste_df = teste_df[["Chave Pedido e Nota", "Flag SAC-aberto", "Caso_Critico", "SACs em Aberto", "SACs Fechado",
                     "SACs Carta de Correcao", "Qtde SAC"]]
teste_df.to_csv(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\SAC.csv',
                sep=';', index=False)

print("Finalizado")

#########################################################################################################################


#######################################################################################################################
#                             COPIA ARQUIVO PRA REDE E CRIA BASE D_1
#######################################################################################################################


# #Copia arquivo para a rede
fileDir = r"C:\Users\eliana.rodrigues\Downloads"
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads'

old_file_path = os.path.join(fileDir, "Back_Office_Novo.csv")
new_file_path = os.path.join(pasta_nova, "Back_Office_Novo.csv")
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")

h = datetime.today().strftime('%H')
##cria arquivo D0
partialFileName = "D_0"
endereco = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\02. BasesDia - BackOffice'
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

backOffice_df.to_csv('C:/Users/eliana.rodrigues/Downloads/' + nome, decimal=',', encoding="utf-8-sig", sep=';',
                     index=False)
print("Arquivo Criado")
#
#
# Copia arquivo para a rede

fileDir = r"C:\Users\eliana.rodrigues\Downloads"
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\02. BasesDia - BackOffice'

old_file_path = os.path.join(fileDir, nome)
new_file_path = os.path.join(pasta_nova, nome)
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")

print(datetime.today().strftime('%H:%M'))
start_time = time.time()

print("Finalizado")
