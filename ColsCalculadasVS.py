# importando bibliotecas
import numpy as np
import pandas as pd
import time
from datetime import datetime, timedelta
from workalendar.america import BrazilBankCalendar
import unicodedata
import re

cal = BrazilBankCalendar()
cal.include_ash_wednesday = False  # tirar a quarta de cinzas, pra gente é dia normal
cal.include_christmas = True                            # considera natal como feriado
cal.include_christmas_eve = True                        # considera natal como feriado
cal.include_new_years_day = True                        # considera ano novo como feriado
cal.include_new_years = False                            # considera ano novo como feriado
blank = 12349999
blank_str = str(blank)
blankdate = time.strptime('01/01/1990', '%d/%m/%Y')

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


def comparaData(primeiradata, segundadata):
    if pd.isnull(primeiradata) or pd.isnull(segundadata):
        return blank
    a = pd.to_datetime(primeiradata, infer_datetime_format=True, dayfirst=True)
    b = pd.to_datetime(segundadata, infer_datetime_format=True, dayfirst=True)
    if a >= b:
        return "primeiradata"
    else:
        return "segundadata"


def menorData(primeiradata, segundadata):
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
            return b
        else:
            return a


def clusterDUTIL(aging):
    if int(aging) == int(blank):
        return blank
    resp = aging * (-1)
    if resp > 5:
        return "9. D++"
    elif resp == 5:
        return "8. D+5"
    elif resp == 4:
        return "7. D+4"
    elif resp == 3:
        return "6. D+3"
    elif resp == 2:
        return "5. D+2"
    elif resp == 1:
        return "4. D+1"
    elif resp == 0:
        return "3. D+0"
    elif resp == -1:
        return "2. D-1"
    elif resp == -2:
        return "1. D-2"
    elif resp < -2:
        return "0. D--"
    else:
        return blank


def clusterDUTIL_EDI(data_hoje, data_edi):
    if pd.isnull(data_edi) or pd.isnull(data_hoje):
        return blank
    else:
        data_edi = pd.to_datetime(data_edi, infer_datetime_format=True, dayfirst=True)
        data_hoje = pd.to_datetime(data_hoje, infer_datetime_format=True, dayfirst=True)
        resp = (cal.get_working_days_delta(data_edi, data_hoje))
    if int(resp) > int(5):
        return "Aging > 5"
    else:
        return "Aging <= 5"


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


def viradaErrada(tpedi, tplm, tprd, flag, tipotp, tp24, tp22, tp23, tp43, uf, regiao):
    tp_edi = tpedi
    tp_t3 = tplm
    tp_t2 = tprd
    edi_consolidado = flag
    tipo_tp = tipotp
    tp_24 = tp24
    tp_22 = tp22
    tp_23 = tp23
    tp_43 = tp43
    uf1 = uf
    regiao1 = regiao

    if tipo_tp == "T3":
        # verdadeiro
        if tp_edi == tp_t3:
            # verdadeiro
            return blank_str
        # falso
        else:
            return "Virada Errada"

    # falso
    elif tp_22 == "PINHAIS - BULKY" and tp_24 != "ARAPON-BULKY" and tp_43 == "ARAPON-BULKY":
        # verdadeiro
        return blank_str

    # falso
    elif tp_43 == "ARAPON-BULKY" and tp_t2 == "ARAPON-BULKY":
        # verdadeiro
        return blank_str

    # falso
    elif tp_43 == "FULFILLMENT - B" and uf1 != "SP":
        # verdadeiro
        return blank_str

    elif tp_43 == "FULFILLMENT - B" and tp_22 == "FULFILLMENT - B":
        # verdadeiro
        return blank_str

    # falso
    elif tp_23 == "PINHAIS - BULKY" and tp_22 == "ALLIANCE TRANSP" and tp_43 == "MG - DOMINALOG":
        # verdadeiro
        return blank_str

    # falso
    elif edi_consolidado == 1:
        # verdadeiro
        return blank_str
    # falso
    elif tp_43 == "MG - DOMINALOG" and (uf1 == "SP" or uf1 == "RJ"):
        # verdadeiro
        return blank_str

    # falso
    elif tp_43 == "PINHAIS - BULKY" and regiao1 != "Sul":
        # verdadeiro
        return blank_str

    # falso
    elif tp_43 == "ARUJA - BULKY" and uf1 == "SP":
        # verdadeiro
        return blank_str

    # falso
    elif tp_43 == "PINHAIS - BULKY" and tp_t2 == "PINHAIS - BULKY":
        # verdadeiro
        return blank_str

    # falso
    elif tp_43 == "SJRP - BULKYLOG" and tp_t2 == "SJRP - BULKYLOG":
        # verdadeiro
        return blank_str

    # falso
    elif tp_t2 == "SJRP - BULKYLOG" and tp_43 == "A N TRANSPORTES":
        # verdadeiro
        return blank_str

    # falso
    elif tp_43 == blank_str:
        # verdadeiro
        return blank_str

    # falso
    elif tipo_tp == "Mista":
        # verdadeiro
        if tp_t2 == blank_str:
            # verdadeiro
            if tp_edi == tp_t3:
                # verdadeiro
                return blank_str
            # falso
            else:
                return "Virada Errada"
        # falso
        elif tp_t2 == tp_t3:
            # verdadeiro
            if tp_edi == tp_t3:
                # verdadeiro
                return blank_str
            # falso
            else:
                return "Virada Errada"
        # falso
        elif tp_t2 == tp_edi:
            # verdadeiro
            return blank_str
        # falso
        elif tp_t3 == tp_edi:
            # verdadeiro
            return blank_str
        # falso
        else:
            return "Virada Errada"
    return blank_str


def posicaoFinal(notamad, virada, d41, d42, d48, d49, d52, d53, d54, d55, d56, d57, d58, edi44, edidata, tpedi, tp22,
                 tplm, statusmanifesto, iniciotransf, statusretorno, dataretorno, deparaedi, idfk, descricaossw, statusnota,
                 flagnext, arquivo):
    vErrada = virada
    edi_44 = edi44
    edi_data = edidata
    tp_edi = tpedi
    tp_22 = tp22
    tp_lm = tplm
    status_manifesto = statusmanifesto
    nota = notamad
    status_retorno = statusretorno
    data_retorno = dataretorno
    de_para_edi = deparaedi
    descricao_ssw = descricaossw

    d_41 = blank
    d_42 = blank
    d_48 = blank
    d_49 = blank
    d_52 = blank
    d_53 = blank
    d_54 = blank
    d_55 = blank
    d_56 = blank
    d_57 = blank
    d_58 = blank
    inicio_transf = blank

    if descricao_ssw == "ENTREGUE":
        p1 = blank
        return [p1, "ENTREGUE - SSW"]

    if (statusnota == "FATURADO" or statusnota == "ENVIADO PARA FATURAMENTO" or
            statusnota == "IMPORTADO" or statusnota == "AG. FORMACAO DE ROMANEIO/ONDA" or
            statusnota == "AG. SEPARACAO"):
        p1 = blank
        return [p1, "EM PCP"]

    if statusnota == "CANCELADO":
        p1 = blank
        return [p1, "CANCELADO - WMS"]

    if arquivo == "Tableau" and pd.isnull(d42) and edi_44 != blank_str:
        p1 = blank
        return [p1, de_para_edi]

    if flagnext == 1 and pd.isnull(d42):
        p1 = blank
        if descricao_ssw != blank_str:
            return [p1, descricao_ssw]
        else:
            return [p1, "EM PCP"]
    if pd.isnull(d41) == False:
        d_41 = d41
    if pd.isnull(d42) == False:
        d_42 = d42
    if pd.isnull(d48) == False:
        d_48 = d48
    if pd.isnull(d49) == False:
        d_49 = d49
    if pd.isnull(d52) == False:
        d_52 = d52
    if pd.isnull(d53) == False:
        d_53 = d53
    if pd.isnull(d54) == False:
        d_54 = d54
    if pd.isnull(d55) == False:
        d_55 = d55
    if pd.isnull(d56) == False:
        d_56 = d56
    if pd.isnull(d57) == False:
        d_57 = d57
    if pd.isnull(d58) == False:
        d_58 = d58
    if pd.isnull(iniciotransf) == False:
        inicio_transf = iniciotransf

    #   #    #   #   #   #   #
    # POSIÇÃO FINAL T3
    #   #    #   #   #   #   #

    # Se for virada errada = virada errada;
    if vErrada == "Virada Errada":
        p1 = "VIRADA ERRADA"

    # Se o Ultimo EDI for Mercadoria Redespachada, considera o "MERCADORIA REDESPACHADA"
    elif de_para_edi == "EM ROTA DE ENTREGA":
        p1 = "EM ROTA DE ENTREGA"

    # Considera os 5 EDI's T3, se algum diferente de vazio, é T3
    elif d_54 != blank or d_55 != blank or d_56 != blank or d_57 != blank or d_58 != blank:
        p1 = "T3"

    # Se o Ultimo EDI for Mercadoria Redespachada, considera o "MERCADORIA REDESPACHADA"
    elif edi_44 == "Mercadoria Redespachada (Entregue para Redespacho)":
        p1 = "MERCADORIA REDESPACHADA"

    # Se o último EDI for: entregue por outra, e proc da padrão transp do ult edi (padronizar igual last mile) for diferente da TP Padrão Entrega, traz mercadoria redespachada.
    elif edi_44 == "Entregue por outra transportadora" and tp_edi != tp_lm:
        p1 = "MERCADORIA REDESPACHADA"

    # Se a data de redespacho for diferente de vazio, e proc da padrão transp do ult edi (padronizar igual last mile) for diferente da TP Padrão Entrega, traz mercadoria redespachada.
    elif d_52 != blank and tp_edi != tp_lm:
        p1 = "MERCADORIA REDESPACHADA"

    # Se a transportadora de redespacho for igual a Direto, considera T3
    elif tp_22 == "Direto" and d_42 != blank:
        p1 = "T3"

    # Se o proc da padrão transp do ult edi (padronizar igual last mile) for igual da TP Padrão Entrega, considera T3
    elif tp_edi == tp_lm and tp_edi != blank_str and tp_lm != blank_str:
        p1 = "T3"

    # Se o status_final = Recebido (Manifesto Eagle), considera Mercadoria Redespachada
    elif status_manifesto == "RECEBIDO" or status_manifesto == "RECEBIDO COM ATRASO":
        p1 = "MERCADORIA REDESPACHADA"

    else:
        p1 = "T2"

    #   #    #   #   #   #   #
    # POSIÇÃO FINAL T2
    #   #    #   #   #   #   #

    # Se 53. Data Movimentação T3 for diferente de vazio, e 54. Data CTRC Emitido T3 for igual a vazio, considera T3;
    if d_53 != blank and d_54 == blank:
        p2 = "T3"

    # Se 54. Data CTRC Emitido T3 for diferente de vazio ou 52. Data Redespacho for diferente de vazio, considera T3;
    elif d_54 != blank or d_52 != blank:
        p2 = "T3"

    # Se Status Final for igual ""Recebido"", considera T3;
    elif status_manifesto == "RECEBIDO" or status_manifesto == "RECEBIDO COM ATRASO":
        p2 = "T3"

    # Se 49. Data Saída Origem T2, ou Inicio_Transferencia forem diferentes de vazio, considera ""Em transferência Redespacho"";
    elif d_49 != blank or inicio_transf != blank:
        p2 = "EM TRANSFERENCIA REDESPACHO"

    # Se Status_Final for igual ""Em trânsito"", considera ""Em transferência Redespacho"";
    elif status_manifesto == "EM TRANSITO":
        p2 = "EM TRANSFERENCIA REDESPACHO"

    # Se 48. Data CTRC Emitido T2 for diferente de vazio, e Status_Final for igual ""Em consolidador"", considera ""Chão consolidador"";
    elif d_48 != blank and status_manifesto == blank and (
            edi_44 == "CTRC Emitido" or edi_44 == "Sobra de Mercadoria sem Nota Fiscal"):
        p2 = "CHAO CONSOLIDADOR"

    # Se 42. Data Expedição for diferente de vazio, considera ""Coletado"";
    elif d_42 != blank:
        p2 = "COLETADO"

    # Se 41. Data BIP for diferente de vazio, considera ""Aguardando Coleta"";
    elif d_41 != blank:
        p2 = "AGUARDANDO COLETA"

    # Se 01. NF Madeira for diferente de vazio, considera ""Aguardando BIP""
    elif nota != blank:
        p2 = "AGUARDANDO BIP"

    # Se tudo acima der errado, considera "Em fornecedor"
    else:
        p2 = "EM FORNECEDOR"

    #   #    #   #   #   #   #
    # POSIÇÃO FINAL GERAL
    #   #    #   #   #   #   #

    if p2 == "COLETADO" and arquivo == "WMS":
        p1 = blank
        return [p1, "FINALIZADO"]

    # Se p1 for igual Mercadoria Redespachada, considera Merc. Redespachada;
    if p1 == "MERCADORIA REDESPACHADA":
        return [p1, "MERCADORIA REDESPACHADA"]

    # Se p1 for igual Mercadoria Redespachada, considera Merc. Redespachada;
    if p1 == "EM ROTA DE ENTREGA":
        return [p1, "EM ROTA DE ENTREGA"]

    # Se p1 for igual Virada Errada, Virada Errada
    elif p1 == "VIRADA ERRADA":
        return [p1, "VIRADA ERRADA"]

    # Se p1 for igual ""T2"" e a p2 for igual ""T3"", considera Erro Edi
    elif p1 == "T2" and p2 == "T3":
        return [p1, "ERRO EDI"]

    # Se p1 for igual T2, considera a p2
    elif p1 == "T2":
        return [p1, p2]

    # Se 74. Status Retorno Contato"" for igual ""Respondido""
    # e Data Último EDI menor ou igual a 70. Data Retorno Contato'
    # e 44. Último EDI for igual a ""Solicitação de Dados Adicionais"", considera ""Dados Respondidos""]
    # elif idfk == 1:
    #     return [p1, "SOLICITACAO DE DADOS APONTADA"]
    # elif idfk == 2 or idfk == 3:
    #     return [p1, "DADOS RESPONDIDOS"]

    elif status_retorno == "respondido" and edi_data <= data_retorno and edi_44 == "Solicitação de Dados Adicionais":
        return [p1, "DADOS RESPONDIDOS"]

    # Se 44. Último EDI for igual a ""Entregue por outra"".
    # e a 43. Transp. Último EDI for diferente da 24. Transp. Entrega,
    # considera o procv na tabela auxiliar ""Auxiliar    _PosiçãoF, trazendo o EDI mais recente.
    elif edi_44 == "Entregue por outra transportadora" and tp_edi != tp_lm:
        datas = [d_54, d_55, d_56, d_57]
        brancos = 12349999
        while brancos in datas:
            datas.remove(brancos)
        dia_max = max(dia for dia in datas)
        dia_pos = datas.index(dia_max)
        if dia_pos == 0:
            return [p1, "CTE T3 EMITIDO"]
        elif dia_pos == 1:
            return [p1, "SAIDA ORIGEM"]
        elif dia_pos == 2:
            return [p1, "CHEGADA DESTINO"]
        else:
            return [p1, "EM ROTA DE ENTREGA"]

    # Se 44. Último EDI for igual a ""Mercadoria Redespachada e p1 for igual ""T3"", considera Erro EDI
    elif edi_44 == "Mercadoria Redespachada" and p1 == "T3":
        return [p1, "ERRO EDI"]

    # Se 44. Último EDI for igual a ""Mercadoria Redespachada, considera o procv na tabela auxiliar ""Auxiliar_PosiçãoF, trazendo o EDI mais recente.
    elif edi_44 == "Mercadoria Redespachada":
        datas = [d_54, d_55, d_56, d_57]
        brancos = 12349999
        while brancos in datas:
            datas.remove(brancos)

        dia_max = max(int(dia) for dia in datas)
        dia_pos = datas.index(dia_max)
        if dia_pos == 0:
            return [p1, "CTE T3 EMITIDO"]
        elif dia_pos == 1:
            return [p1, "SAIDA ORIGEM"]
        elif dia_pos == 2:
            return [p1, "CHEGADA DESTINO"]
        else:
            return [p1, "EM ROTA DE ENTREGA"]

    # Se 22. Transp. Redespacho for igual a Direto, e a 42. Data Expedição for diferente de vazio e o 44. Último EDI  for igual a vazio, considera Coletado
    elif tp_22 == "Direto" and d_42 != blank and edi_44 == blank_str:
        return [p1, "COLETADO"]

    # Se p1 for igual ""T3"", considera o procv 44. Último EDI com a tabela auxiliar ""EDI_De_Para""
    elif p1 == "T3":
        if de_para_edi == "SOLICITACAO DE DADOS APONTADA" or de_para_edi == 'ENDERECO NAO LOCALIZADO':
            if idfk != 1:
                return [p1, "DADOS RESPONDIDOS"]
            else:
                return [p1, "SOLICITACAO DE DADOS APONTADA"]
        else:
            return [p1, de_para_edi]
    else:
        return [p1, blank_str]


def fasenota(pos_final, unidadeorigem, unidaderecep, consolidador, tplm, parceiro, descricao, origem, p1, arquivo, d42, edi_44):
    if pos_final == "ENTREGUE - SSW":
        return "ENTREGUE - SSW"

    elif pos_final == "EM PCP":
        return "AGUARDANDO CD"

    elif consolidador == "Cons. Pinhais" and tplm == "PINHAIS - BULKY":
        return "EM LAST MILE"

    elif pos_final == "CTE T3 EMITIDO":
        return "EM REDESPACHO"

    elif pos_final == "EM ROTA DE ENTREGA":
        return "EM LAST MILE"

    elif pos_final == "SAIDA UNIDADE" and parceiro == tplm:
        return "EM LAST MILE"

    elif pos_final == "SAIDA UNIDADE" and parceiro != tplm:
        return "EM REDESPACHO"

    elif pos_final == "EM TRANSFERENCIA REDESPACHO" and descricao == ' CHEGADA EM UNIDADE' or descricao == ' CHEGADA NA UNIDADE' and parceiro != tplm:
        return "EM LAST MILE"

    elif pos_final == "CHEGADA UNIDADE" and parceiro != tplm:
        return "EM LAST MILE"

    elif pos_final == "CLIENTE AUSENTE":
        return "EM LAST MILE"

    elif pos_final == "CTE CANCELADO":
        return "EM LAST MILE"

    elif pos_final == "CANCELADO - WMS":
        return "CANCELADO - WMS"

    elif pos_final == "FINALIZADO":
        return "FINALIZADO"

    if arquivo == "Tableau" and pd.isnull(d42) and edi_44 != blank_str:
        return "EM LAST MILE"

    elif pos_final == "VIRADA ERRADA":
        return "EM LAST MILE"

    elif pos_final == "MERCADORIA REDESPACHADA":
        return "EM LAST MILE"

    elif pos_final == "AGUARDANDO BIP":
        if origem == "CD JDI Fulfillment" or origem == "CD Paraná" or origem == "CD Espirito Santo" or origem == "CD ES Fulfillment" or origem == "CD PE Fulfillment":
            return "AGUARDANDO CD"
        else:
            return "EM FORNECEDOR"

    elif pos_final == "EM FORNECEDOR":
        return "EM FORNECEDOR"

    elif pos_final == "AGUARDANDO COLETA":
        return "AGUARDANDO COLETA"

    elif p1 == "T3":
        return "EM LAST MILE"

    elif p1 == "T2":
        return "EM REDESPACHO"

    else:
        return blank_str


def flagAtraso(aging):
    ag = aging
    flag5igual = blank
    status = blank_str
    flag0 = blank
    flag3 = blank
    flag5 = blank
    flag10 = blank
    flag15 = blank
    flag30 = blank
    if int(ag) == int(5):
        flag5igual = 1

    if aging == blank:
        status = '99. Sem Data'
        return [status, flag0, flag3, flag5, flag10, flag15, flag30, flag5igual]
    elif int(ag) > 30:
        status = "06. Atraso > 30 dias"
        flag0 = 1
        flag3 = 1
        flag5 = 1
        flag10 = 1
        flag15 = 1
        flag30 = 1
    elif int(ag) > 15:
        status = "05. Atraso > 15 dias"
        flag0 = 1
        flag3 = 1
        flag5 = 1
        flag10 = 1
        flag15 = 1
        flag30 = blank
    elif int(ag) > 10:
        status = "04. Atraso > 10 dias"
        flag0 = 1
        flag3 = 1
        flag5 = 1
        flag10 = 1
        flag15 = blank
        flag30 = blank
    elif int(ag) > 5:
        status = "03. Atraso > 05 dias"
        flag0 = 1
        flag3 = 1
        flag5 = 1
        flag10 = blank
        flag15 = blank
        flag30 = blank
    elif int(ag) > 3:
        status = "02. Atraso > 03 dias"
        flag0 = 1
        flag3 = 1
        flag5 = blank
        flag10 = blank
        flag15 = blank
        flag30 = blank
    elif int(ag) > 0:
        status = "01. Atraso < 03 dias"
        flag0 = 1
        flag3 = blank
        flag5 = blank
        flag10 = blank
        flag15 = blank
        flag30 = blank
    else:
        status = "00. No prazo"
        flag0 = blank
        flag3 = blank
        flag5 = blank
        flag10 = blank
        flag15 = blank
        flag30 = blank

    return [status, flag0, flag3, flag5, flag10, flag15, flag30, flag5igual]


def data_pos_final(posfinal, dfat40, dbip41, dexp42, dedi46, dsolcontato68, dretcontato70, edidepara, dultima_ssw,
                   dultima_wms):
    # SE A POSIÇÃO FINAL FOR IGUAL A ENTREGUE - SSW, TRAZ A DATA DO ULTIMA OCORRENCIA
    if posfinal == "ENTREGUE - SSW":
        data = dultima_ssw
    # SE A POSIÇÃO FINAL FOR IGUAL A EM PCP, TRAZ A DATA DO ULTIMA OCORRENCIA
    if posfinal == "AGUARDANDO CD":
        data = dultima_wms

    # SE A POSIÇÃO FINAL FOR IGUAL A VIRADA ERRADA, TRAZ A DATA DO ULTIMO EDI
    elif posfinal == "VIRADA ERRADA":
        data = dedi46

    # SE A POSIÇÃO FINAL FOR IGUAL A EM FORNECEDOR , TRAZ VAZIO
    elif posfinal == "EM FORNECEDOR":
        data = pd.NaT

    # SE A POSIÇÃO FINAL FOR AGUARDANDO BIP , TRAZ A DATA DE FATURAMENTO DA NOTA MADEIRA
    elif posfinal == "AGUARDANDO BIP":
        data = dfat40

    # SE A POSIÇÃO FINAL FOR AGUARDANDO COLETA , TRAZ A DAT DO BIP
    elif posfinal == "AGUARDANDO COLETA":
        data = dbip41

    # SE A POSIÇÃO FINAL FOR COLETADO , TRAZ A DATA DE EXPEDIÇÃO
    elif posfinal == "COLETADO":
        data = dexp42

    # SE A POSIÇÃO FINAL FOR ERRO EDI , TRAZ A DATA DO ULTIMO EDI
    elif posfinal == "ERRO EDI":
        data = dedi46

    # SE A POSIÇÃO FINAL FOR DADOS RESPONDIDOS E DATA RETORNO FOR MAIOR OU IGUAL AO ULTIMO EDI, TRAZ A DATA RETORNO CONTATO
    elif posfinal == "DADOS RESPONDIDOS" and (dretcontato70 >= dedi46):
        data = dretcontato70

    # SE A POSIÇÃO FINAL FOR SOLICITAÇÃO DE DADOS APONTADO E A DATA DA SOLICITAÇÃO DO CONTATO FOR MAIO OU IGUAL AO UTLIMO EDI, TRAZ A DATA DA SOLICITAÇÃO
    elif posfinal == "SOLICITAÇÃO DE DADOS APONTADA" and (dsolcontato68 >= dedi46):
        data = dsolcontato68

    # SE O PROCV DO ULTIMO EDI COM O EDI DE_PARA(LAST MILE) FOR DIFERENTE DE VAZIO, TRAZ A DATA DO ULTIMO EDI"
    elif edidepara != blank_str:
        data = dedi46

    else:
        data = pd.NaT

    return data


def data_limite(dia, prazo):
    if pd.isnull(dia):
        dl = pd.NaT

    else:
        if prazo == blank:
            dl = pd.to_datetime(dia, infer_datetime_format=True,
                                dayfirst=True)

        else:
            dl = cal.add_working_days(pd.to_datetime(dia), int(prazo))

    return dl


def data_limite_t2(dia, prazo, p30):
    if pd.isnull(dia):
        dl = pd.NaT

    else:
        if prazo == blank:
            if p30 == blank:
                dl = pd.to_datetime(dia, infer_datetime_format=True,
                                    dayfirst=True)
            else:
                dl = cal.add_working_days(pd.to_datetime(dia), int(p30))
        else:
            dl = cal.add_working_days(pd.to_datetime(dia), int(prazo))

    return dl


def erroprazoCliente(d25, d26, d27, d28, d41, d42):
    if pd.isnull(d28) and pd.isnull(d25):
        flag = "0"
    else:
        if (comparaData(d26, d41) == "primeiradata") and (comparaData(d27, d42) == "primeiradata") and (
                comparaData(d28, d25) == "primeiradata"):
            flag = "1"
        else:
            flag = "0"

    return flag


# calculo -- Prazo_Last_Mile
def prazo_last_mile(dltransp, dexp, ptransp, predespacho):
    prazolm = blank
    if ptransp == blank:
        ptransp = 0
    if predespacho == blank:
        predespacho = 0
    if pd.isnull(dltransp) or pd.isnull(dexp):
        prazolm = blank
        return prazolm
    else:
        prazolm = int(ptransp) - (int(predespacho))
    return prazolm


# calculo - atraso fornecedor
def atraso_fornecedor(d26, d41, hoje):
    flag = blank

    if pd.isnull(d26):
        flag = blank
        dias_f = blank

    elif pd.isnull(d41):
        dias_f = (diaUtil_entredatas(hoje, d26))

    else:
        dias_f = (diaUtil_entredatas(d41, d26))

    if dias_f != blank:
        if int(dias_f) > 0:
            flag = "0"
        else:
            flag = "1"

    return flag


def aging_resp_dados(d_68, d_70, hoje, s_74):
    agingRespostaDados = blank

    if s_74 != blank:
        if d_70 != blank:
            agingRespostaDados = diaUtil_entredatas(d_68, d_70)
        else:
            agingRespostaDados = diaUtil_entredatas(d_68, hoje)
    else:
        agingRespostaDados = blank

    return agingRespostaDados


def aging_emissao_cte(d_42, d_52, d_54, tp22):
    agingEmissaoCTE = blank

    if pd.isnull(d_54) == False:
        if tp22 == "Direto":
            agingEmissaoCTE = diaUtil_entredatas(d_42, d_54)
        else:
            agingEmissaoCTE = diaUtil_entredatas(d_52, d_54)
    else:
        agingEmissaoCTE = blank

    return agingEmissaoCTE


def contstatus(dia, dl_t2):
    cont_stats = blank
    a = pd.to_datetime(dl_t2, infer_datetime_format=True, dayfirst=True)
    b = pd.to_datetime(dia, infer_datetime_format=True, dayfirst=True)
    if a == b:
        cont_stats = 0
    else:
        cont_stats = diaUtil_entredatas(dl_t2, dia)

    return cont_stats


def step_macro(posfinal, p1, aux, arquivo, d42, edi_44):
    if posfinal == "ENTREGUE - SSW":
        return "ENTREGUE - SSW"

    elif posfinal == "EM PCP":
        return "AGUARDANDO CD"

    elif posfinal == "CANCELADO - WMS":
        return "CANCELADO - WMS"

    elif posfinal == "FINALIZADO":
        return "FINALIZADO"

    if arquivo == "Tableau" and pd.isnull(d42) and edi_44 != blank_str:
        return "SEM EXPEDICAO - PCP"

    elif posfinal == "MERCADORIA REDESPACHADA":
        step = "EMITIR CTE - T3"

    elif p1 == "T3" and posfinal == "COLETADO":
        step = "EMITIR CTE - T3"

    elif p1 == "T2" and posfinal == "COLETADO":
        step = "CHAO CONSOLIDADOR"
    else:
        step = aux

    return step


def destinoSLA(reg, uf, destino):
    if reg == "Centro-Oeste":
        destino_sla = "CO"
    elif reg == "Norte":
        destino_sla = "N"
    elif reg == "Nordeste":
        destino_sla = "NE"
    elif uf == "SP":
        destino_sla = destino
    else:
        destino_sla = uf

    return destino_sla


def tp_nota(fasenota, tp22, tpredespacho, tplmv2, posiparceiro, tplm, viradaerrada, tpedi):
    tpnota = pd.NA

    if fasenota == "EM FORNECEDOR":
        tpnota = pd.NA

    elif fasenota == "AGUARDANDO BIP" or fasenota == "AGUARDANDO COLETA" or fasenota == "AGUARDANDO CD":
        if tp22 == "Direto" or tp22 == blank_str:
            tpnota = tplmv2
        else:
            tpnota = tpredespacho
    elif fasenota == "EM REDESPACHO":
        tpnota = tpredespacho
    elif posiparceiro == "EM TRANSFERENCIA PARCEIRO" or posiparceiro == "EMITIR CTE - T3" or posiparceiro == "CTE EMITIDO":
        tpnota = tplmv2
    elif viradaerrada == "Virada Errada":
        tpnota = tpedi
    else:
        tpnota = tplm

    return tpnota


def aging_falta_emissao(stepmacro, tp22, hoje, d42, d52):
    if stepmacro == blank_str:
        ag = blank

    elif stepmacro == "EMITIR CTE - T2" or stepmacro == "EMITIR CTE - T3" or stepmacro == "EMITIR CTE":
        if tp22 == "Direto":
            if pd.isnull(d42) == True:
                ag = blank
            else:
                ag = diaUtil_entredatas(d42, hoje)

        else:
            if pd.isnull(d52) == True:
                ag = blank
            else:
                ag = diaUtil_entredatas(d52, hoje)
    else:
        ag = blank

    return ag


def stepparceiro(edi44, d53, d54, d55, d56):
    edi = edi44
    if pd.isnull(d56) == False:
        step_parceiro = "ENTREGUE PARCEIRO"

    elif edi == "Chegada Destino" or edi == "Mercadoria Entregue no Parceiro" or edi == "Devolução Autorizada Mi" or edi == "Entrega Prejudicada por Horário/Falta de Tempo Hábil" or edi == "Entregue por outra transportadora":
        step_parceiro = "ENTREGUE PARCEIRO"

    elif pd.isnull(d55) == False:
        step_parceiro = "EM TRANSITO"

    elif pd.isnull(d54) == False or pd.isnull(d53) == False:
        step_parceiro = "EM LAST MILE"

    else:
        step_parceiro = "STEP NAO RECONHECIDO"

    return step_parceiro


def weeknum(data):
    if pd.isnull(data):
        return blank
    else:
        d = pd.to_datetime(data, infer_datetime_format=True, dayfirst=True)
        weekday_hoje = int(d.strftime("%w"))
        return weekday_hoje


def DL_Parceiro(d54):
    if pd.isnull(d54):
        dia = pd.NaT
    else:
        dia = cal.add_working_days(pd.to_datetime(d54), int(3))

    return dia


def DL_transf(d49, dinicio, placa, predespacho):
    if pd.isnull(d49) and placa != blank_str:
        if pd.isnull(dinicio):
            dl_transf_T2 = pd.NaT
        else:
            if predespacho == blank:
                dl_transf_T2 = pd.to_datetime(dinicio, infer_datetime_format=True, dayfirst=True)
            else:
                dl_transf_T2 = cal.add_working_days(pd.to_datetime(dinicio, infer_datetime_format=True, dayfirst=True),
                                                    int(predespacho))

    elif pd.isnull(d49):
        dl_transf_T2 = pd.NaT

    else:
        if predespacho == blank:
            dl_transf_T2 = pd.to_datetime(d49, infer_datetime_format=True, dayfirst=True)
        else:
            dl_transf_T2 = cal.add_working_days(pd.to_datetime(d49, infer_datetime_format=True, dayfirst=True),
                                                int(predespacho))

    return dl_transf_T2


def coletas_t2(fasenota, diasemanaBip, qtd, col1, col2, col3):
    prox = blank

    if fasenota == "AGUARDANDO COLETA":
        # domingo 0
        # seg 1
        # ter 2
        # qua 3
        # qui 4
        # sex 5
        # sab 6

        # SE ENTREGA DE SEGUNDA A SEXTA, COLETA SEMPRE NO PROXIMO DIA, SE O DIA DO BIP FOR SEXTA, COLETA NA SEGUNDA
        if qtd == 5:
            if diasemanaBip > 4:
                prox = 2
            else:
                prox = diasemanaBip + 1

        elif qtd == 1:
            prox = col1

        # SE TIVER 2 DIAS DE COLETA, VERIFICA SE O DIA DO BIP É MAIOR QUE O 1o DIA
        elif qtd == 2:
            if diasemanaBip > 4:
                prox = col1
            elif diasemanaBip >= col1:
                prox = col2
            else:
                prox = col1

        elif qtd == 3:
            if diasemanaBip > 4:
                prox = col1
            elif diasemanaBip >= col1 and diasemanaBip >= col2:
                prox = col3
            elif diasemanaBip >= col1:
                prox = col2
            else:
                prox = col1
    else:
        prox = blank

    return prox


def diabip_ajust(d_bip):
    if pd.isnull(d_bip):
        dia = pd.NaT
        return dia

    d = pd.to_datetime(d_bip, infer_datetime_format=True, dayfirst=True)
    h = int(d.strftime("%H"))

    if cal.is_working_day(d) == False:
        dia = cal.add_working_days(pd.to_datetime(d), int(1))

    elif h > 17:
        dia = cal.add_working_days(pd.to_datetime(d), int(1))

    else:
        dia = d

    return dia


def dia_prox_coleta(fasenota, diasemanaBip, d27, databip, prox):
    if fasenota == "AGUARDANDO COLETA":
        if prox == blank:
            if pd.isnull(d27):
                d = pd.NaT
            else:
                d = d27

        elif prox == diasemanaBip:
            d = pd.to_datetime(databip) + timedelta(days=7)

        elif diasemanaBip < prox:
            prazo = prox - diasemanaBip
            d = pd.to_datetime(databip) + timedelta(days=prazo)
        else:
            prazo = 7 - (diasemanaBip - prox)
            d = pd.to_datetime(databip) + timedelta(days=prazo)
    else:
        d = pd.NaT

    return d


def status_coleta_fornecedor_t2(fasenota, proxcoleta, data_hoje):
    if fasenota == "EM REDESPACHO":
        return "Redespachado"
    elif fasenota != "AGUARDANDO COLETA":
        return "Fora de T2"

    d_prox = pd.to_datetime(proxcoleta, infer_datetime_format=True, dayfirst=True)
    d_hoje = pd.to_datetime(data_hoje, infer_datetime_format=True, dayfirst=True)
    semana_d_prox = int(d_prox.strftime("%U"))
    semana_d_hoje = int(d_hoje.strftime("%U"))
    # 0 é domingo e 6 é sabado
    weekday_d_prox = int(d_prox.strftime("%w"))
    weekday_hoje = int(d_hoje.strftime("%w"))

    if d_prox < (d_hoje - timedelta(days=1)):
        return "01. Atraso"
    elif d_prox == (d_hoje - timedelta(days=1)):
        return "02. Atraso Ontem"
    elif d_prox == d_hoje:
        return "03. Vence Hoje"
    elif d_prox == (d_hoje + timedelta(days=1)):
        return "04. Vence Amanha"
    elif semana_d_prox == (semana_d_hoje):
        return "05. Vence Semana"
    elif (weekday_hoje >= 4 or weekday_hoje == 0) and weekday_d_prox == 1 and semana_d_prox == (semana_d_hoje + 1):
        return "06. Vence Segunda"
    elif semana_d_prox == (semana_d_hoje + 1):
        return "07. Vence S+1"
    else:
        return "08. Vence S++"


def atraso_forn_aux(origem, d26, d41, d42, hoje):
    if origem == "CD JDI Fulfillment":
        flag = 0
    elif pd.isnull(d26):
        flag = 0

    elif pd.isnull(d41) and pd.isnull(d42) == False:
        d_42 = cal.sub_working_days(pd.to_datetime(d42), int(1))
        if diaUtil_entredatas(d_42, d26) < 0:
            flag = 1
        else:
            flag = 0

    elif pd.isnull(d41) == False:
        if diaUtil_entredatas(d41, d26) < 0:
            flag = 1
        else:
            flag = 0
    else:
        if diaUtil_entredatas(hoje, d26) < 0:
            flag = 1
        else:
            flag = 0
    return flag


def atraso_coleta_aux(origem, d27, d42, hoje):
    if origem == "CD JDI Fulfillment":
        flag = 0
    elif pd.isnull(d27):
        flag = 0

    elif pd.isnull(d42) == False:
        if diaUtil_entredatas(d42, d27) < 0:
            flag = 1
        else:
            flag = 0
    else:
        if diaUtil_entredatas(hoje, d27) < 0:
            flag = 1
        else:
            flag = 0
    return flag


def atraso_cd_ff_aux(origem, d42, d54, hoje, a_cliente):
    # if(Data Importacao]>=[Data Esperada para Embarque;"Erro Prazo Cliente")
    # 37. Data Compra Cliente
    # 90. Data Esperada para Embarque (WMS)

    if a_cliente == 1:
        if origem == "CD JDI Fulfillment":
            if pd.isnull(d42):
                flag = "FF"
            elif pd.isnull(d54):
                if diaUtil_entredatas(d42, hoje) > 1:
                    flag = "FF"
                else:
                    flag = blank_str
            else:
                if diaUtil_entredatas(d42, d54) > 1:
                    flag = "FF"
                else:
                    flag = "TP"
        else:
            flag = blank_str
    else:
        flag = blank_str
    return flag


def stepReorganizado(statusmanifesto, origem, d42, d53, s06):
    if statusmanifesto == "RECEBIDO":
        stepReorganiza = 4

    elif origem == "CD JDI Fulfillment" and (pd.isnull(d42) == False) and pd.isnull(d53):
        stepReorganiza = 3

    else:
        stepReorganiza = s06

    return stepReorganiza


def posic_parceiro_transf(posfinal, flag_parceiro, stepmacro):
    if posfinal == "EMITIR CTE - T3" or posfinal == "CTE T3 EMITIDO" and flag_parceiro == 1:
        pos_parceiro = "A TRANSFERIR"
    elif posfinal == 'SAIDA ORIGEM' and flag_parceiro == 1:
        pos_parceiro = "EM TRANSFERENCIA PARCEIRO"
    else:
        pos_parceiro = stepmacro

    return pos_parceiro


def deltaTransf(agingCTRC, agingTransf):
    a1 = int(agingCTRC)
    b1 = int(agingTransf)

    if a1 == blank or b1 == blank:
        deltaTransf = blank
    else:
        deltaTransf = int(a1) - int(b1)

    return deltaTransf


def reprogramado(d25, d26):
    d = diaUtil_entredatas(d25, d26)
    if d > 0:
        flag = 0
    else:
        flag = 1
    return flag


def faseatraso(a_cliente, a_fornecedor, a_coleta, a_cd_ff, a_next, a_transp, reprogramado):
    if a_cliente == 1:
        if a_fornecedor == 1:
            return "1. At. Fornecedor"
        elif reprogramado == 1:
            return "1. At. Fornecedor"
        elif a_coleta == 1:
            return "2. At. Coleta"
        elif a_cd_ff == 1:
            return "3. At. CD FF"
        elif a_next == 1:
            return "4. At. Next Day"
        elif a_transp == 1:
            return "5. At. Transporte"
        else:
            return "99. Erro prazo"
    else:
        return blank_str


def data_ultima_wms(d93, d94, d95, d96, d97, d98, d99, d100):
    d_93 = blank
    d_94 = blank
    d_95 = blank
    d_96 = blank
    d_97 = blank
    d_98 = blank
    d_99 = blank
    d_100 = blank

    if pd.isnull(d93) == False:
        d_93 = d93
    if pd.isnull(d94) == False:
        d_94 = d94
    if pd.isnull(d95) == False:
        d_95 = d95
    if pd.isnull(d96) == False:
        d_96 = d96
    if pd.isnull(d97) == False:
        d_97 = d97
    if pd.isnull(d98) == False:
        d_98 = d98
    if pd.isnull(d100) == False:
        d_100 = d100

    datas = [d_93, d_94, d_95, d_96, d_97, d_98, d_100]
    brancos = 12349999
    while brancos in datas:
        datas.remove(brancos)
    if datas == []:
        return [pd.NaT, ""]

    dia_max = max(dia for dia in datas)
    dia_pos = datas.index(dia_max)
    if dia_pos == 0:
        return [d_93, "CADASTRADO"]
    elif dia_pos == 1:
        return [d_94, "IMPORTADO"]
    elif dia_pos == 2:
        return [d_95, "ROTEIRIZADO"]
    elif dia_pos == 3:
        return [d_96, "SEPARADO"]
    elif dia_pos == 4:
        return [d_97, "CONFERIDO"]
    elif dia_pos == 5:
        return [d_98, "PESADO"]
    elif dia_pos == 6:
        return [d_100, "COLETADO"]
    else:
        return [pd.NaT, ""]


def diario_flag(arquivo, atraso_MM, atraso_cliente, atraso_tp, atraso_redespacho, agT3, agCliente, agTransporte, agedi,
                costats, st_consol, st_parce, st_trans,
                st_colet, vencmm, venccliente, venctransporte, agmovimentacao, vencredespacho, vencchaot, vencparceiro,
                venctransferenciat, venccoletat, marketplace, diarioedi, diarionotacritica, fasenota):
    a = blank
    b = blank

    if arquivo == "VS":

        if fasenota == "FINALIZADO" or fasenota == "CANCELADO - WMS" or fasenota == "EM PCP" or fasenota == "ENTREGUE - SSW":
            return [a, b]

        if diarionotacritica == "X":
            a = "nota critica"
            b = "X"
            return [a, b]

        if atraso_tp == 1:
            a = "Atraso Transporte"
            b = "X"
            return [a, b]
        if atraso_cliente == 1:
            a = "Atraso Cliente"
            b = "X"
            return [a, b]
        if atraso_MM == 1:
            a = "Atraso MM"
            b = "X"
            return [a, b]

        if agT3 != blank and vencmm != blank:
            if agT3 * (-1) <= vencmm:
                a = "Aging T3"
                b = "X"
                return [a, b]

        if agCliente != blank and venccliente != blank:
            if agCliente * (-1) <= venccliente:
                a = "Aging cliente"
                b = "X"
                return [a, b]

        if agTransporte != blank and venctransporte != blank:
            if agTransporte * (-1) <= venctransporte:
                a = "Aging Transporte"
                b = "X"
                return [a, b]

        if agedi != blank and marketplace != blank:
            if agedi >= marketplace:
                a = "Marketplace"
                b = "X"
                return [a, b]

        if agedi != blank and agmovimentacao != blank:
            if agedi >= agmovimentacao:
                a = "Aging edi"
                b = "X"
                return [a, b]

        if fasenota != "EM LAST MILE":
            if costats != blank and vencredespacho != blank:
                if costats <= vencredespacho:
                    a = "Aging redespacho"
                    b = "X"
                    return [a, b]

            # if st_consol != blank and vencchaot != blank:
            #     if st_consol <= vencchaot:
            #         a = "Aging chao"
            #         b = "X"
            #         return [a, b]
            if st_parce != blank and vencparceiro != blank:
                if st_parce <= vencparceiro:
                    a = "Aging parceiro"
                    b = "X"
                    return [a, b]
            # if st_trans != blank and venctransferenciat != blank:
            #     if st_trans <= venctransferenciat:
            #         a = "Aging transferencia"
            #         b = "X"
            #         return [a, b]
            # if st_colet != blank and venccoletat != blank:
            #     if st_colet <= venccoletat:
            #         a = "Aging coleta"
            #         b = "X"
            #         return [a, b]

        if diarioedi == "X":
            a = "dexpara"
            b = "X"
            return [a, b]
        else:
            return [a, b]
    else:
        return [a, b]


def transpLMFinal(tplm, tpparceiro, tp_lmv2):
    if tp_lmv2 == "FULFILLMENT - NEXT DAY":
        return "FULFILLMENT"
    elif tplm == 'BULKYLOG - UBA':
        return tpparceiro

    elif tplm == "FULFILLMENT" or tplm == "BULKY-JUNDIAI":
        if tpparceiro == "SJRP - BULKYLOG P":
            return "SJRP - BULKYLOG"
        elif tpparceiro != blank_str:
            return tpparceiro
        else:
            return tplm

    elif tplm == "PINHAIS - BULKY":
        if tpparceiro == "ARAPON-BULKY - P":
            return "ARAPON-BULKY"
        elif tpparceiro == "GRAVATAI-BULK - P":
            return "GRAVATAI-BULK"
        elif tpparceiro == "ITAJAI-BULKY - P":
            return "ITAJAI-BULKY"
        elif tpparceiro != blank_str:
            return tpparceiro
        else:
            return "PINHAIS - BULKY"

    elif tplm == "RIO - BULKYLOG":
        if tpparceiro != blank_str:
            return tpparceiro
        else:
            return tplm

    else:
        return tplm


def novaDataLimiteTransp(tp22, flagatrasoredespacho, d28, d_redespachoCTE, prazoLM):
    if tp22 == "Direto":
        return d28
    elif flagatrasoredespacho == 0:
        return d28
    elif pd.isnull(d_redespachoCTE):
        return d28
    elif flagatrasoredespacho == 1:
        try:
            dl = cal.add_working_days(pd.to_datetime(d_redespachoCTE), int(prazoLM))
        except:
            dl = pd.NaT
        return dl
    else:
        return pd.NaT


def statustransfT2(contstatus, statusvenci):
    if contstatus > 2:
        return "No prazo>D+2"
    elif contstatus < -5:
        return "Atraso>D-5"
    elif contstatus == -5 or contstatus == -4 or contstatus == -3:
        return "Atraso>D-2"
    else:
        return statusvenci


def remove_combine_regex(string: str) -> str:
    normalized = unicodedata.normalize('NFD', string)
    return re.sub(r'[\u0300-\u036f]', '', normalized).casefold()


def flagaberto(codigossw):
    if codigossw in [45,50,53,54,55,59,91,92,1,2,3,	5,25,26,27,36,20,23,41]:
        return 0
    else:
        return 1


def flagfechado(codigossw):
    if codigossw in [45,50,53,54,55,59,91,92,1,2,3,	5,25,26,27,36,20,23,41,18]:
        return 1
    else:
        return 0

def slafechado(dataedi, datalimite):
    if diaUtil_entredatas(dataedi, datalimite) >= 0:
        return 1
    else:
        return 0

def flagfechadoconsolidador(codigossw):
    if codigossw in [45,50,53,54,55,59,91,92,1,2,3,	5,25,26,27,36,20,23,41,18, 83, 84, 99]:
        return 1
    else:
        return 0

def slafechadoconsolidador(primeiradata, segundadata):
    a = pd.to_datetime(primeiradata, infer_datetime_format=True, dayfirst=True)
    b = pd.to_datetime(segundadata, infer_datetime_format=True, dayfirst=True)
    if a == b:
        return 1
    elif a > b:
        return 0
    else:
        return 1

def data_limitecol(dia, prazo):
    try:
        if pd.isnull(dia):
            dl = pd.NaT

        else:
            dl = pd.to_datetime(dia) + timedelta(days=prazo)

        return dl
    except:
        return pd.NaT