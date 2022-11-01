import time
import pyautogui
from datetime import datetime, timedelta
import shutil
import os
from selenium import webdriver
import sys
import pandas as pd
import ColsCalculadasVS
import numpy as np


blank = 12349999
blank_str = str(blank)
blankdate = time.strptime('01/01/1990', '%d/%m/%Y')
#######################################################################################################################
#                                                   DOWNLOAD 455
#######################################################################################################################

# login = "erodrigu"
#
# #Fazer download do chromedriver compativel com o seu Chrome e depois colocar o endereço do executável
# browser = webdriver.Chrome(executable_path="C:\Python\chromedriver.exe")
# browser.get("https://sistema.ssw.inf.br/bin/ssw0422")
# browser.maximize_window()
# pyautogui.doubleClick(1798, 118)
# time.sleep(3)
#
# #encontrando na pagina HTMl local de usuario e senha.
# dominio = browser.find_element_by_id("1")
# cpf = browser.find_element_by_id("2")
# usuario = browser.find_element_by_id("3")
# senha = browser.find_element_by_id("4")
# botao_login = browser.find_element_by_id('5')
# time.sleep(2)
#
# #colocando as credenciais.
# dominio.send_keys("BLK")
# cpf.send_keys("10741554917")
# usuario.send_keys(login)
# senha.send_keys("eli4321")
# time.sleep(2)
# botao_login.click()
# time.sleep(10)
#
# #Entrando na tela 455
# browser.get("https://sistema.ssw.inf.br/bin/ssw0230")
# time.sleep(2)
# d = datetime.today() - timedelta(days=30)
# d1 = datetime.today().strftime('%d%m%y')
# d = d.strftime('%d%m%y') + d1
# time.sleep(2)
#
# #colocando o periodo do download
# data = browser.find_element_by_name("f11")
# data.clear()
# data.send_keys(d)
# time.sleep(1)
#
# excel = browser.find_element_by_id("35")
# excel.clear()
# excel.send_keys("E")
# time.sleep(1)
# funcao_a = browser.find_element_by_id("37")
# funcao_b = browser.find_element_by_id("38")
# funcao_f = browser.find_element_by_id("39")
# funcao_a.send_keys("A")
# time.sleep(1)
# funcao_b.send_keys("B")
# time.sleep(1)
# funcao_f.send_keys("F")
# time.sleep(10)
#
# #Fazendo download
# botao_down = browser.find_element_by_id("40")
# botao_down.click()
# time.sleep(10)
#
# #Entrando na tela do download
# browser.get("https://sistema.ssw.inf.br/bin/ssw1440")
# time.sleep(20)
# user = ""
# i = 2
# j = 0
# #Buscando na tabela o login do usuario, se o download esta completo e o botao de baixar.
# while i < 101:
#     print(str(j) +"a tentativa")
#     x = "//tr[@class='srtr2'][" + str(i) + "]/td[@class='srtd2'][4]/div[@class='srdvl']"
#     user = browser.find_element_by_xpath(x)
#     if user.text == login:
#         c = "//tr[@class='srtr2'][" + str(i) + "]/td[@class='srtd2'][7]/div[@class='srdvl']"
#         concluido = browser.find_element_by_xpath(c)
#         if concluido.text == "Concluído":
#             b = "//tr[@class='srtr2'][" + str(i) + "]/td[@class='srtd2'][9]/div[@class='srdvl']/a[@class='sra']/u"
#             baixar = browser.find_element_by_xpath(b)
#             time.sleep(1)
#             baixar.click()
#             break
#         else:
#             i = 1
#             j = j + 1
#             browser.refresh()
#             time.sleep(8)
#     i = i + 1
# time.sleep(5)
# #
# #verifica se o arquivo foi baixado
# fileDir = r"C:\Users\eliana.rodrigues\Downloads"
# fileExt = r".sswweb"
# newlist = []
# cont = 0
# while len(newlist) == 0:
#     time.sleep(5)
#     items = os.listdir(fileDir)
#     for names in items:
#         if names.endswith(fileExt):
#             newlist.append(names)
#     cont = cont + 1
#     print(cont, newlist)
# time.sleep(15)
# ssw455 = str(newlist[0])
# #
# #renomeia o arquivo para um auxiliar
# old_file = os.path.join(fileDir, ssw455)
# new_file = os.path.join(fileDir, "SSW_455_testeCopia.csv")
# os.rename(old_file, new_file)

# new_file = r'C:\Users\eliana.rodrigues\Downloads\SSW_455_testeCopia.csv'
#
# #ajusta o arquivo
# #Tira a primeira linha
# line_to_delete = 1
# initial_line = 1
# file_lines = {}
#
# print('ajusta arquivo')
#
# #abre o arquivo e le as linhas
# with open(new_file) as f:
#     content = f.readlines()
# for line in content:
#     file_lines[initial_line] = line.strip()
#     initial_line += 1
# print('abre arquivo e le linhas')
#
# #Deleta a primeira linha
# f = open(new_file, "w")
# for line_number, line_content in file_lines.items():
#     if line_number != line_to_delete:
#         f.write('{}\n'.format(line_content))
# print('Deleta a primeira linha')
# f.close()

#######################################################################################################################
#                                                   LÊ CSV - SSW
#######################################################################################################################
#########
# JUNTA DOIS ARQUIVOS SSW, DE  30 E 60 DIAS #
########

#######################################################################################################################
#                                                   LÊ CSV - SSW
#######################################################################################################################
#########
# JUNTA DOIS ARQUIVOS SSW, DE  30 E 60 DIAS #
########

base_ssw1 = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\SSW_455_30D.csv', sep=';', encoding='latin1')
base_ssw2 = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\SSW_455_testeCopia.csv', sep=';', encoding='latin1')

base_sswtodas = pd.DataFrame()

base_sswtodas = base_sswtodas.append(base_ssw1)
base_sswtodas = base_sswtodas.append(base_ssw2)

base_sswtodas.to_csv('C:/Users/eliana.rodrigues/Downloads/SSW_VS.csv', sep=';', index=False)


s455_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\SSW_VS.csv', sep=';')

#Gera coluna com data de hoje e contador
d = datetime.today().strftime('%d/%m/%y')
s455_df['Hoje'] = d
s455_df['Contador'] = 1
print('Gera Contador e Coluna com a data de hoje')



s455_df['CNPJ Pagador'] = s455_df['CNPJ Pagador'].replace('Â  ', blank)

try:
        s455_df['CNPJ Pagador'] = s455_df['CNPJ Pagador'].astype(float)
except ValueError:
    pass


print(s455_df['CNPJ Pagador'])


#CNPJ
list = ['CNPJ Pagador', 'Ativo']
cnpj_df = pd.read_excel(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\CNPJ venda de serviço.xlsx', usecols=list)
s455_df = pd.merge(s455_df, cnpj_df, on='CNPJ Pagador', how='left')

print(cnpj_df)


#Filtra Placa de Coleta = "VS"
#s455_df['Placa de Coleta (Left)'] = s455_df['Placa de Coleta'].str[:2]
vendas_df = s455_df.loc[(s455_df['Ativo'] == 1)]

vendas_df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\testevs.csv', sep=';', index=False)
print(vendas_df)

vendas_df['Qtd_Nfs'] = pd.Index(vendas_df['Notas Fiscais'].str.count('/'))
print(vendas_df['Qtd_Nfs'])

# vendas_df['86. Tipo De Transporte'] =  np.where(vendas_df["Qtd_Nfs"] > 1, "B2B", "B2C")

#Quebra de linhas da coluna Notas Fiscais
print(vendas_df['Notas Fiscais'])

vendas_df = (vendas_df.assign(Notas_Fiscais=vendas_df['Notas Fiscais'].str.split(',')).explode('Notas_Fiscais'))
vendas = vendas_df['Notas_Fiscais'].str.split("/")

print(vendas_df['Notas_Fiscais'])
print('primeira nota')

notafiscal_1 = vendas.str.get(0)
print(notafiscal_1)
notafiscal_2 = vendas.str.get(1)
print(notafiscal_2)

vendas_df['Notas_Fiscais'] = notafiscal_2

#Dropa colunas que não precisam aparecer na base
vendas_df = vendas_df.drop(columns=['Volumes'])

num_colunas = vendas_df.shape[1]
num_linhas = vendas_df.shape[0]

vendas_df['Numero da Nota Fiscal'] = vendas_df['Numero da Nota Fiscal'].astype(int)
vendas_df['Numero da Nota Fiscal'] = vendas_df['Numero da Nota Fiscal'].astype(str)
vendas_df['Numero da Nota Fiscal'] = vendas_df['Numero da Nota Fiscal'].apply(str).str.replace('.0', '')



#rename columns
vendas_df.rename(columns = {'Data de Emissao' : '48. Data CTRC Emitido T2',
'Data de Autorizacao' : '42. Data Expedicao',
'Cidade do Remetente' : '15. Origem',
'Placa de Coleta' : 'Placa Manifesto',
'CNPJ Remetente' : '14. CNPJ Fornecedor',
'Cliente Remetente' : '05. Fornecedor',
'Cliente Destinatario' : '12. Nome Cliente',
'UF do Destinatario' : '87. UF Destino',
'CEP do Destinatario' : '17. CEP',
'Fone do Destinatario' : '13. Telefone Cliente',
'Cidade de Entrega' : '88. Cidade',
'CEP de Entrega ': '17. CEP',
'Numero da Nota Fiscal' : '04. NF Fornecedor',
'Peso Real em Kg' : '10. Peso NF (kg)',
'Quantidade de Volumes' : '11. Qntd. Volumes NF',
'Valor do Frete' : '08. Valor Frete',
'Data do Ultimo Manifesto' : 'Inicio da Transferencia Manifesto',
'Placa de Entrega' : 'Placa Veiculo - SSW 036',
'Codigo da Ultima Ocorrencia' : 'Codigo da Ultima Ocorrencia - SWW 455',
'Data da Ultima Ocorrencia' : '46. Data Ultimo EDI',
'Usuario da Ultima Ocorrencia' : 'Usuario da Ultima Ocorrencia - SSW 455',
'Unidade da Ultima Ocorrencia' : 'Unidade da Ultima Ocorrencia',
'Previsao de Entrega' : 'Previsao de Entrega - SSW 455',
'Hoje' : 'Dia',
'Contador' : 'Contador',
'Notas_Fiscais' : '01. NF Madeira',
'Data da Entrega Realizada' : '58. Data Entrega',
'Serie/Numero CT-e' : 'Serie/Numero CTRC',
'Classificação' : 'Status Last Mile',
'Tipo do Documento' : '06. Step da NF/OC'
}, inplace=True)

vendas_df.insert(loc=num_colunas, column='Chave Nota e Cliente Destinatario', value=vendas_df['01. NF Madeira'].astype(str)+vendas_df['12. Nome Cliente'].str[:4])

print(vendas_df['01. NF Madeira'].dtype)

vendas_df['01. NF Madeira'] = vendas_df['01. NF Madeira'].replace('Â  ', blank)
try:
    vendas_df['01. NF Madeira'] = vendas_df['01. NF Madeira'].astype(int)
except ValueError:
    pass

#######################################################################################################################
#                             C O M E Ç A R   O S  MERGES   A Q U I
#######################################################################################################################

#Nome Fantasia Cliente
list = ['Cliente Pagador', 'FANTASIA', 'TIPO']
deparacliente_df = pd.read_csv(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\DeParaCliente.csv', sep=';', usecols=list)
deparacliente_df.rename(columns={'FANTASIA': 'Cliente',
                                 'TIPO': '86. Tipo De Transporte'
                                               } , inplace = True)
vendas_df = pd.merge(vendas_df, deparacliente_df, on='Cliente Pagador', how='left')
deparacliente_df = pd.DataFrame()

#CHAVE ORIGEM-DESTINO-CLIENTE
num_colunas = vendas_df.shape[1]
vendas_df.insert(loc=num_colunas, column='ChaveOrigem24TEntrega', value=(vendas_df['15. Origem'].astype(str) + vendas_df['88. Cidade'].astype(str) + vendas_df['Cliente'].astype(str)))


#Chave Pedido e Nota
num_colunas = vendas_df.shape[1]
vendas_df.insert(loc=num_colunas, column='Chave Pedido e Nota', value=("VS" + vendas_df['01. NF Madeira'].astype(str)))

#44 Ultimo EDI
list = ['Codigo da Ultima Ocorrencia - SWW 455', '44. Ultimo EDI']
edi_df = pd.read_excel(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\DEPARA_Edi.xlsx', usecols=list)
vendas_df = pd.merge(vendas_df, edi_df, on='Codigo da Ultima Ocorrencia - SWW 455', how='left')

print(edi_df)


#44 Ultimo EDI - DEPARA
list = ['44. Ultimo EDI', 'DExPARA EDI']
edi_df = pd.read_csv(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\EDI_De_Para.csv', usecols=list, sep=';')
vendas_df = pd.merge(vendas_df, edi_df, on='44. Ultimo EDI', how='left')

print(edi_df)

#DESCRIÇÃO - SSW
deparaSSW_df = pd.read_csv(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Codigos_ID_SSW.csv', sep=';')
vendas_df = pd.merge(vendas_df, deparaSSW_df, on='Codigo da Ultima Ocorrencia - SWW 455', how='left')
deparaSSW_df = pd.DataFrame()

#DESCRIÇÃO - Classificação Ocorrência
list = ['Codigo da Ultima Ocorrencia - SWW 455', 'Classificação']
ocorrenSSW_df = pd.read_csv(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\Codigos_ID_SSW.csv', sep=';', usecols=list)
vendas_df = pd.merge(vendas_df, ocorrenSSW_df, on='Codigo da Ultima Ocorrencia - SWW 455', how='left')
ocorrenSSW_df = pd.DataFrame()

#DEPARA - Transportadora (Unidade Receptora)
list = ['Unidade Receptora', 'Transportadora']
tpdepara_df = pd.read_csv(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\Transportadora_Entrega.csv', sep=';', usecols=list)
tpdepara_df.rename(columns={'Transportadora': 'Transp. LM',
                                               } , inplace = True)
vendas_df = pd.merge(vendas_df, tpdepara_df, on='Unidade Receptora', how='left')
tpdepara_df = pd.DataFrame()


#Proc com Prazos
list = ['CHAVE ORIGEM-DESTINO-CLIENTE', 'PRAZO DE TRANSFERÊNCIA', 'PRAZO DE ENTREGA', 'PRAZO TOTAL']
prazos_df = pd.read_csv(r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\PrazosVS.csv', sep=';', usecols=list)
prazos_df.rename(columns={'PRAZO DE TRANSFERÊNCIA': 'Prazo_Viagem_T2',
                            'PRAZO DE ENTREGA': 'Prazo Last Mile',
                            'PRAZO TOTAL' : 'Prazo Transporte',
                            'CHAVE ORIGEM-DESTINO-CLIENTE' :'ChaveOrigem24TEntrega'
                                               } , inplace = True)
vendas_df = pd.merge(vendas_df, prazos_df, on='ChaveOrigem24TEntrega', how='left')

prazos_df = pd.DataFrame()
print(prazos_df)

#Proc com Consolidador
list = ['Unidade Emissora', 'Consolidador']
types = {'Unidade Emissora': 'string' , 'Consolidador': 'string'}
consolidador_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\consolidador.csv", sep=';', usecols=list, dtype= types)
consolidador_df = ColsCalculadasVS.ajustaLetras(consolidador_df)
#consolidador_df = consolidador_df.convert_dtypes() #ajustando os tipos da colunas
vendas_df = pd.merge(vendas_df, consolidador_df, on='Unidade Emissora', how='left')
consolidador_df= pd.DataFrame()



#Proc com Transportadora Entrega
list = ['Unidade Receptora', 'Transportadora']
tpentrega_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\Transportadora_Entrega.csv", sep=';', usecols=list)
tpentrega_df = ColsCalculadasVS.ajustaLetras(tpentrega_df)
tpentrega_df.rename(columns={'Transportadora': '24. Transp. Entrega'
                                               } , inplace = True)
vendas_df = pd.merge(vendas_df, tpentrega_df, on='Unidade Receptora', how='left')
tpentrega_df = pd.DataFrame()

#Proc com Transportadora Redespacho
list = ['Consolidador', '22. Transp. Redespacho']
tpt2_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\Transportadora_Redespacho.csv", sep=';', usecols=list)
tpt2_df = ColsCalculadasVS.ajustaLetras(tpt2_df)
vendas_df = pd.merge(vendas_df, tpt2_df, on='Consolidador', how='left')
tpt2_df = pd.DataFrame()



#Proc com Unidade Ult Ocorr
list = ['Unidade da Ultima Ocorrencia', 'Parceiro Ultima Ocorrencia']
types = {'Unidade da Ultima Ocorrencia': 'category', 'Parceiro Ultima Ocorrencia': 'category'}
unidadessw_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\DePara - Pça - SSW.csv", sep=';', usecols=list, dtype= types)
unidadessw_df = ColsCalculadasVS.ajustaLetras(unidadessw_df)
#unidadessw_df = unidadessw_df.convert_dtypes() #ajustando os tipos da colunas
unidadessw_df['Unidade da Ultima Ocorrencia'] = unidadessw_df['Unidade da Ultima Ocorrencia'].str.upper()

vendas_df = pd.merge(vendas_df, unidadessw_df, on='Unidade da Ultima Ocorrencia', how='left')
unidadessw_df= pd.DataFrame()

#Proc com o UF - DESTINO
types = {'87. UF Destino': 'string', 'Regiao': 'category', 'Macro Regiao': 'category'}
uf_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\UF.csv", sep=';', dtype= types)
#uf_df = ColunasCalculadas.ajustaLetras(uf_df)
#uf_df = uf_df.convert_dtypes() #ajustando os tipos da colunas
vendas_df = pd.merge(vendas_df, uf_df, on='87. UF Destino', how='left')
uf_df= pd.DataFrame()

#Proc com Prazo Coleta

prazo_coleta = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\07. AuxiliaresVS\PRAZO REVERSA - VS.csv", sep=';', dtype= types)
vendas_df = pd.merge(vendas_df, prazo_coleta, on='24. Transp. Entrega', how='left')
prazo_coleta = pd.DataFrame()

#######################################################################################################################
#                             C O M E Ç A R   O S   C A L C U L O S   A Q U I
#######################################################################################################################
lista = ['42. Data Expedicao', '48. Data CTRC Emitido T2']

for d in lista:
    #pd.to_datetime(df['Dates'], format='%y%m%d')
    vendas_df[d] = pd.to_datetime(vendas_df[d], infer_datetime_format=True, dayfirst=True)

#insert colums blank
vendas_df["Virada Errada"]= blank
vendas_df["41. Data BIP"]= blank
vendas_df["49. Data Saida Origem T2"]= blank
vendas_df["52. Data Redespacho"]= blank
vendas_df["53. Data Movimentacao T3"]= blank
vendas_df["54. Data CTRC Emitido T3"]= blank
vendas_df["55. Data Saida Origem T3"]= blank
vendas_df["56. Data Chegada Destino T3"]= blank
vendas_df["57. Data Em Rota de Entrega T3"]= blank
vendas_df["TP Ultimo EDI"] = vendas_df['Parceiro Ultima Ocorrencia']
vendas_df["Transportadora Last Mile"] = vendas_df['24. Transp. Entrega']
vendas_df["Status_Manifesto Padrao"]= blank
vendas_df["74. Status Retorno Contato"]= blank
vendas_df["70. Data Retorno Contato"]= blank
vendas_df['idfk_status']= blank
vendas_df["FLAG NEXT DAY"]= blank
vendas_df["Arquivo"]= blank
vendas_df['Posicao_Parceiros_Transferencia'] = blank
vendas_df['Transportadora Redespacho'] = blank
vendas_df['76. Danfe_Nf_Mi'] = vendas_df['Chave CT-e']


vendas_df['Flag Aberto'] = np.vectorize(ColsCalculadasVS.flagaberto, otypes=[np.ndarray])(vendas_df['Codigo da Ultima Ocorrencia - SWW 455'])

vendas_df["p1 - aux"], vendas_df["Posicao Final"] = zip(*vendas_df.apply(lambda row:
                                                                                ColsCalculadasVS.posicaoFinal(
                                                                                    row["01. NF Madeira"],
                                                                                    row["Virada Errada"],
                                                                                    row["41. Data BIP"],
                                                                                    row["42. Data Expedicao"],
                                                                                    row["48. Data CTRC Emitido T2"],
                                                                                    row["49. Data Saida Origem T2"],
                                                                                    row["52. Data Redespacho"],
                                                                                    row["53. Data Movimentacao T3"],
                                                                                    row["54. Data CTRC Emitido T3"],
                                                                                    row["55. Data Saida Origem T3"],
                                                                                    row["56. Data Chegada Destino T3"],
                                                                                    row["57. Data Em Rota de Entrega T3"],
                                                                                    row["58. Data Entrega"],
                                                                                    row["44. Ultimo EDI"],
                                                                                    row["46. Data Ultimo EDI"],
                                                                                    row["TP Ultimo EDI"],
                                                                                    row["22. Transp. Redespacho"],
                                                                                    row["Transportadora Last Mile"],
                                                                                    row["Status_Manifesto Padrao"],
                                                                                    row["Inicio da Transferencia Manifesto"],
                                                                                    row["74. Status Retorno Contato"],
                                                                                    row["70. Data Retorno Contato"],
                                                                                    row["DExPARA EDI"],
                                                                                    row['idfk_status'],
                                                                                    row["Descricao Padronizada  - SSW 455"],
                                                                                    row['06. Step da NF/OC'],
                                                                                    row["FLAG NEXT DAY"],
                                                                                row["Arquivo"]), axis=1))


vendas_df['Prazo_Viagem_T2'] = vendas_df['Prazo_Viagem_T2'].fillna(0).astype(int)
vendas_df['Prazo Last Mile'] = vendas_df['Prazo Last Mile'].fillna(0).astype(int)
vendas_df['Prazo Transporte'] = vendas_df['Prazo Transporte'].fillna(0).astype(int)

#Aging Ultima Movimentação
vendas_df["Aging EDI"] = np.vectorize(ColsCalculadasVS.diaUtil_entredatas, otypes=[np.ndarray])(vendas_df['46. Data Ultimo EDI'],vendas_df['Dia'])

#Cluster EDI
vendas_df["Cluster EDI"] = np.vectorize(ColsCalculadasVS.clusterDUTIL, otypes=[np.ndarray])(vendas_df['Aging EDI'])

#Data Limite Transferência
vendas_df["Data_Limite_T2"] = vendas_df.apply(lambda row: ColsCalculadasVS.data_limite(row["42. Data Expedicao"],
                                                                                                row["Prazo_Viagem_T2"]), axis=1)

#Data Limite Transporte
vendas_df["28. Data Limite Transporte"] = vendas_df.apply(lambda row: ColsCalculadasVS.data_limite(row["42. Data Expedicao"],
                                                                                                row["Prazo Transporte"]), axis=1)
#Data_Limite_Coleta
vendas_df['27. Data Limite Coleta'] = vendas_df.apply(lambda row: ColsCalculadasVS.data_limitecol(row['42. Data Expedicao'], row['82. Prazo_Fornecedor']), axis=1)

#Aging Transporte
vendas_df["Aging Transporte"] = np.vectorize(ColsCalculadasVS.diaUtil_entredatas, otypes=[np.ndarray])(vendas_df['28. Data Limite Transporte'], vendas_df['Dia'] )


#Aging Transporte
vendas_df["Aging Transporte"] = np.vectorize(ColsCalculadasVS.diaUtil_entredatas, otypes=[np.ndarray])(vendas_df['28. Data Limite Transporte'], vendas_df['Dia'] )

#Cluster Transporte
vendas_df["Cluster Transporte"] = np.vectorize(ColsCalculadasVS.clusterDUTIL, otypes=[np.ndarray])(vendas_df['Aging Transporte'] )

#Status Vencimento Transporte
vendas_df["Status Vencimento Transporte"] = np.vectorize(ColsCalculadasVS.statusVencimento, otypes=[np.ndarray])(vendas_df['Dia'], vendas_df['28. Data Limite Transporte'] )

#Flags Atraso Transporte
vendas_df["Status Atraso Transporte"], vendas_df["Flag Atraso Transporte"], vendas_df["Flag Atraso Transporte > 3D"], \
vendas_df["Flag Atraso Transporte > 5D"], vendas_df["Flag Atraso Transporte > 10D"], vendas_df[
    "Flag Atraso Transporte > 15D"], vendas_df["Flag Atraso Transporte > 30D"], vendas_df[
    "Flag Atraso Transporte = 5D"] = zip(
    *vendas_df.apply(lambda row: ColsCalculadasVS.flagAtraso(row["Aging Transporte"]), axis=1))


#Status Final T2
vendas_df["Status_Final_T2"] = np.vectorize(ColsCalculadasVS.statusVencimento, otypes=[np.ndarray])(vendas_df['Dia'], vendas_df['Data_Limite_T2'])

#Cont_stats
vendas_df["cont_stats"] = np.vectorize(ColsCalculadasVS.diaUtil_entredatas, otypes=[np.ndarray])(vendas_df['Dia'], vendas_df['Data_Limite_T2'])

print(vendas_df['Consolidador'])
vendas_df['Consolidador'] = vendas_df['Consolidador'].replace(pd.NA, np.nan)

# Fase Nota
vendas_df["Fase Nota"] = np.vectorize(ColsCalculadasVS.fasenota, otypes=[np.ndarray])(vendas_df['Posicao Final'],
                                                                                     vendas_df['Unidade Emissora'],
                                                                                     vendas_df['Unidade Receptora'],
                                                                                     vendas_df['Consolidador'],
                                                                                     vendas_df['24. Transp. Entrega'],
                                                                                     vendas_df['Parceiro Ultima Ocorrencia'],
                                                                                     vendas_df['Descricao da Ultima Ocorrencia - SSW 455'],
                                                                                     vendas_df['15. Origem'],
                                                                                     vendas_df['p1 - aux'],
                                                                                     vendas_df['Arquivo'],
                                                                                     vendas_df['42. Data Expedicao'],
                                                                                     vendas_df['44. Ultimo EDI'])



vendas_df["TP que esta com a nota"] = np.vectorize(ColsCalculadasVS.tp_nota, otypes=[np.ndarray])(vendas_df['Fase Nota'],
                                                                                                  vendas_df['22. Transp. Redespacho'],
                                                                                                  vendas_df['Transportadora Redespacho'],
                                                                                                  vendas_df['Transportadora Last Mile'],
                                                                                                  vendas_df['Posicao_Parceiros_Transferencia'],
                                                                                                  vendas_df['Transp. LM'],
                                                                                                  vendas_df['Virada Errada'],
                                                                                                  vendas_df['TP Ultimo EDI'])


#Flag_Atraso_Redespacho
vendas_df["Flag_Atraso_Redespacho"] = np.where((vendas_df["Fase Nota"] == "EM REDESPACHO") &
                                                                    (vendas_df["cont_stats"] < 0), 1, blank)

#Proc com o Step Macro
types = {'Posicao Final': 'string', 'Aux - Step Macro': 'string'}
stepmacro_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Step_Macro.csv", sep=';', dtype= types)
stepmacro_df = ColsCalculadasVS.ajustaLetras(stepmacro_df)
#stepmacro_df = stepmacro_df.convert_dtypes() #ajustando os tipos da colunas
vendas_df = pd.merge(vendas_df, stepmacro_df, on='Posicao Final', how='left')
stepmacro_df= pd.DataFrame()

#step_macro(posfinal, p1, aux, arquivo, d42, edi_44):
vendas_df["Step Macro"] = np.vectorize(ColsCalculadasVS.step_macro, otypes=[np.ndarray])(vendas_df['Posicao Final'],
                                                                                         vendas_df['p1 - aux'],
                                                                                         vendas_df['Aux - Step Macro'],
                                                                                         vendas_df['Arquivo'],
                                                                                         vendas_df['42. Data Expedicao'],
                                                                                         vendas_df['44. Ultimo EDI'])


print(vendas_df)
print(vendas_df.shape[0], "linhas")

sys.setrecursionlimit(10000) # 10000 is an example, try with different values





## ______Remove Duplicadas pela chave(pedido & nome cliente), exclui a primeira linha (cancelado)
#vendas_df = vendas_df.drop_duplicates(subset=['Chave Nota e Cliente Destinatario'], keep='first', ignore_index=True)
vendas_df = vendas_df.drop_duplicates(subset=['Chave Nota e Cliente Destinatario'], keep='last', ignore_index=True)



## -- filtra somente pedidos em aberto na base

vendas_df = vendas_df.drop(vendas_df.loc[(vendas_df['Flag Aberto'] != 1)] .index)


vendas_df = vendas_df.convert_dtypes()
vendas_df = vendas_df.replace(blank, np.nan, regex=True)
vendas_df = vendas_df.replace(str(blank), '', regex=True)

vendas_df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Venda_Servico_teste.csv', sep=';', columns=list,  index=False)

#
list = ['48. Data CTRC Emitido T2',	'42. Data Expedicao',	'Placa Manifesto',	'14. CNPJ Fornecedor',	'05. Fornecedor',	'15. Origem',	'12. Nome Cliente',	'87. UF Destino',	'17. CEP',	'13. Telefone Cliente',	'88. Cidade',	'04. NF Fornecedor',	'10. Peso NF (kg)',	'11. Qntd. Volumes NF',	'08. Valor Frete',	'Inicio da Transferencia Manifesto',	'Placa Veiculo - SSW 036',	'Codigo da Ultima Ocorrencia - SWW 455',	'46. Data Ultimo EDI',	'Usuario da Ultima Ocorrencia - SSW 455',	'Unidade da Ultima Ocorrencia',	'Previsao de Entrega - SSW 455',	'58. Data Entrega',	'Dia',	'Contador',	'01. NF Madeira',	'Chave Nota e Cliente Destinatario',	'ChaveOrigem24TEntrega',	'Chave Pedido e Nota',	'44. Ultimo EDI',	'DExPARA EDI',	'Descricao da Ultima Ocorrencia - SSW 455',	'Descricao Padronizada  - SSW 455',		'Transp. LM',	'Prazo_Viagem_T2',	'Prazo Last Mile',	'Prazo Transporte',	'Consolidador',	'24. Transp. Entrega',	'22. Transp. Redespacho',	'Parceiro Ultima Ocorrencia',	'Regiao',	'Macro Regiao',	'Virada Errada',	'41. Data BIP',	'49. Data Saida Origem T2',	'52. Data Redespacho',	'53. Data Movimentacao T3',	'54. Data CTRC Emitido T3',	'55. Data Saida Origem T3',	'56. Data Chegada Destino T3',	'57. Data Em Rota de Entrega T3',	'TP Ultimo EDI',	'Status_Manifesto Padrao',	'74. Status Retorno Contato',	'70. Data Retorno Contato',		'06. Step da NF/OC',	'FLAG NEXT DAY',	'Posicao_Parceiros_Transferencia',	'Transportadora Redespacho',	'Posicao Final',	'Aging EDI',	'Cluster EDI',	'Data_Limite_T2',	'28. Data Limite Transporte',	'Aging Transporte',	'Cluster Transporte',	'Status Vencimento Transporte',	'Status Atraso Transporte',	'Flag Atraso Transporte',	'Flag Atraso Transporte > 3D',	'Flag Atraso Transporte > 5D',	'Flag Atraso Transporte > 10D',	'Flag Atraso Transporte > 15D',	'Flag Atraso Transporte > 30D',	'Flag Atraso Transporte = 5D',	'Status_Final_T2',	'cont_stats',	'Fase Nota',	'TP que esta com a nota',	'Flag_Atraso_Redespacho',	'Step Macro', '86. Tipo De Transporte', '82. Prazo_Fornecedor', '27. Data Limite Coleta', '76. Danfe_Nf_Mi']
#

#######################################################################################################################
#                                                   VENDA DE SERVIÇO - ler base ISEND
#######################################################################################################################


# vendaservico_df = pd.read_csv(r"C:\Users\eliana.rodrigues\Downloads\vs_isend.csv", sep=';', drop=True)
#
# h = datetime.today().strftime('%H')
# num_colunas = vendaservico_df.shape[1]
# #vendaservico_df.insert(loc=num_colunas, column='Dia (D)', value='D_0 - '+h+'h')
#
# vendaservico_df.insert(loc=0, column='Arquivo', value="VS")
# vendaservico_df = vendaservico_df.convert_dtypes()
#
# #Concatenando as notas do tableau com as informações do WMS
# vs_df = pd.concat([vendas_df, vendaservico_df], ignore_index=True)
# print(vendas_df.shape[0], "linhas")

# #######################################################################################################################
# #                                                   VENDA DE SERVIÇO - ler base ISEND
# #######################################################################################################################

# vs_df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Venda_Servico_Isend.csv', sep=';', columns=list,  index=False)
vendas_df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Venda_Servico.csv', sep=';', columns=list,  index=False)

# #Copia arquivo para a rede
fileDir = r"C:\Users\eliana.rodrigues\Downloads"
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads'

old_file_path = os.path.join(fileDir, "Venda_Servico.csv")
new_file_path = os.path.join(pasta_nova, "Venda_Servico.csv")
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")


# vendas_df.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Venda_Servico.csv', sep=';', encoding='UTF-32BE', index=False)