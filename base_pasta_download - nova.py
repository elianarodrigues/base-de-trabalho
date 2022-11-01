#importando bibliotecas
import time
import pandas as pd
import os
import shutil
from datetime import datetime
import numpy as np
from workalendar.america import BrazilBankCalendar
import ColunasCalculadas
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype


arquivo = open('tempo_codigo/times_{}.txt'.format(pd.datetime.today().strftime('%Y%m%d-%H%M')), 'w')


print(datetime.today().strftime('%H:%M'))
start_time = time.time()
cal = BrazilBankCalendar()
cal.include_ash_wednesday = False                       #tirar a quarta de cinzas, pra gente é dia normal
cal.include_christmas = True                            # considera natal como feriado
cal.include_christmas_eve = True                        # considera natal como feriado
cal.include_new_years_day = True                        # considera ano novo como feriado
cal.include_new_years_eve = False                        # considera ano novo como feriado
blank = 12349999

#buscando a data e hora do sistema
d = datetime.today().strftime('%d/%m/%y')
h = datetime.today().strftime('%H')

################################################
#DATABASE - TABLEAU
print("tableau")
pd.set_option('display.max_rows', None)
tableau_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\Giro_Total.csv', sep=';')
tableau_df["58. Data Entrega"] = tableau_df["58. Data Entrega"].astype(str) #ajustando o tipo da coluna
tableau_df["67. Data Analise Devolucao"] = tableau_df["67. Data Analise Devolucao"].astype(str) #ajustando o tipo da coluna
tableau_df["17. CEP"] = tableau_df["17. CEP"].astype(str) #ajustando o tipo da coluna
tableau_df = tableau_df.convert_dtypes() #ajustando os tipos da colunas
tableau_df = tableau_df.convert_dtypes() #ajustando os tipos da colunas
tableau_df["17. CEP"] = tableau_df["17. CEP"].str.zfill(8) #Preenche a coluna com 8 digitos (zero a esquerda)

tableau_df["14. CNPJ Fornecedor"] = tableau_df["14. CNPJ Fornecedor"].astype('Int64') #ajustando o tipo da coluna
print(tableau_df.shape[0], " linhas tableau") #printa o numero de linhas do tableau


tableau_df['04. NF Fornecedor'] = tableau_df['04. NF Fornecedor'].fillna(blank) #preenchendo os espaços vazios com 12349999
tableau_df['01. NF Madeira'] = tableau_df['01. NF Madeira'].fillna(blank) #preenchendo os espaços vazios com 12349999
print("Concat Tableau", " - %s" % (time.time() - start_time), " - segundos", "- %s" % (d), file=arquivo)


#criando coluna Chave pedido e nota
num_colunas = tableau_df.shape[1] #numero de colunas do tableau
#tableau_df.insert(loc=0, column='Arquivo', value="Tableau") #Coloca na 1a coluna o valor "Tableau"
#tableau_df.insert(loc=num_colunas, column='Chave Pedido e Nota', value=tableau_df['02. Pedido'].astype(str) + tableau_df['01. NF Madeira'].astype(str)) #Insere na ultima coluna a chave pedido e nota
print("Chave Pedido e Nf", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


# a função groupby junta as mesmas "chave pedido e nota" e soma quantas vezes aparece com o .cumcount
#coloca numero se nao tiver nota e tiver mais de 1 pedido "Z1234567-1"
tableau_df['Chave Pedido e Nota'] += '-' + tableau_df.groupby(['Chave Pedido e Nota']).cumcount().astype(str)
tableau_df['Chave Pedido e Nota'] = tableau_df['Chave Pedido e Nota'].replace('-0', '', regex=True)
print("Coloca Numero", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

################################################
#      DATABASE - WMS
    # wms_df cria a base de dados que vem embaixo das notas do tableau
# wms_colunas_df cria uma base de dados que adiciona informações do WMS nas notas do tableau

print("WMS")
#Criando dataframe do WMS que ficará embaixo do Tableau
wms_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\WMS.csv', sep=';')
wms_df = wms_df.convert_dtypes() #ajustando os tipos da colunas
print("df wms", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#criando coluna Chave pedido e nota
num_colunas = wms_df.shape[1]
wms_df.insert(loc=num_colunas, column='Chave Pedido e Nota', value=wms_df['02. Pedido'].astype(str) + wms_df['01. NF Madeira'].astype(str))#Insere na ultima coluna a chave pedido e nota
wms_df.insert(loc=0, column='Arquivo', value="WMS")
print("df wms chave pv e nf", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

print("CEP E CIDADE - WMS")

#Colocando as informações do banco no wms -> CEP, DATA LIMITE CLIENTE E CIDADE
banco_wms_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\banco.csv', sep=';')
banco_wms_df.rename(columns={"02_Pedido": "02. Pedido", "88_Cidade": "88. Cidade",
                             "25_Data_Limite_Cliente": "25. Data Limite Cliente",
                             "17_CEP": "17. CEP", "87_UF Destino": "87. UF Destino"}, inplace=True)
banco_wms_df['17. CEP'] = banco_wms_df['17. CEP'].replace('X', "0", regex=True)
banco_wms_df["17. CEP"] = banco_wms_df["17. CEP"].astype(str) #ajustando o tipo da coluna
banco_wms_df["17. CEP"] = banco_wms_df["17. CEP"].str.zfill(8) #Preenche a coluna com 8 digitos (zero a esquerda)
banco_wms_df = banco_wms_df.convert_dtypes() #ajustando os tipos da colunas
print(wms_df.shape[0], "num linhas wms")
print("info banco datas", " - %s" % (time.time() - start_time), " - segundos" "- %s" % (d), file=arquivo)

#Faz o proc do SSW 455 e 036 pelo ultimo romaneio
wms_df = pd.merge(wms_df, banco_wms_df, on='02. Pedido', how='left')
print(wms_df.shape[0], "num linhas wms")
banco_wms_df = pd.DataFrame()
print("proc 455-036", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Criando lista das colunas do WMS que será adicionado ao Tableau
list = ['02. Pedido', '01. NF Madeira', "Classificacao Tipo Pedido", "Data Esperada para Embarque",
        "Cancelado Pelo ERP", "Onda", "Cadastrada em", "Importado em", "Roteirizado em", "Separado em",
        "Conferido em", "Pesado em", "Cancelado em", "Coletado em", "Rota"
        ]
print("criou lista de cols wms x tableau", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Criando dataframe que adiciona as informações do wms nas notas do tableau
wms_colunas_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\WMS.csv', sep=';', usecols=list) #usecols utiliza apenas as colunas listadas no "list" do excel
wms_colunas_df = wms_colunas_df.convert_dtypes() #ajustando os tipos da colunas
print("criou df que adc info wms no tb", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#criando coluna Chave pedido e nota
num_colunas = wms_colunas_df.shape[1]
wms_colunas_df.insert(loc=num_colunas, column='Chave Pedido e Nota', value=wms_colunas_df['02. Pedido'].astype(str) + wms_colunas_df['01. NF Madeira'].astype(str))
wms_colunas_df = wms_colunas_df.drop(columns=['02. Pedido', '01. NF Madeira'] )
print("chave pedido e nota", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


print("SSW 036")
#dataframe do SSW 036
ssw_036_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\SSW_036.csv', sep=';')

#criando a Data hora emissao
ssw_036_df.insert(loc=1, column='DataHora Emissao - SSW 036', value=pd.to_datetime(ssw_036_df['DATA EMISSAO'], dayfirst=True) + pd.to_timedelta(ssw_036_df['HORA EMISSAO']))
ssw_036_df['ROMANEIO'] = ssw_036_df['ROMANEIO'].replace(' ', '', regex=True)
ssw_036_df.rename(columns = {'ROMANEIO': 'Ultimo Romaneio - SSW 455',
                         'PLACA': 'Placa Veiculo - SSW 036',
                         'MOTORISTA': 'Motorista - SSW 036',
                         'CPF DO MOTORISTA': 'CPF do Motorista - SSW 036'
                         } , inplace = True)
ssw_036_df["CPF do Motorista - SSW 036"] = ssw_036_df["CPF do Motorista - SSW 036"].astype('Float64').astype('Int32')
ssw_036_df = ssw_036_df.drop(columns=['DATA EMISSAO', 'HORA EMISSAO'])
print("codigo SSW 036", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

print("codigo SSW 455")
#pega os codigos dexpara do ssw455
cod_ssw_455_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Codigos_ID_SSW.csv", sep=';')
cod_ssw_455_df = cod_ssw_455_df.convert_dtypes() #ajustando os tipos da colunas
print("codigo SSW 455", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

print("SSW 455")
#dataframe do ssw455
ssw_455_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\SSW_455.csv', sep=';')
ssw_455_df = ssw_455_df.convert_dtypes() #ajustando os tipos da colunas
ssw_455_df = ssw_455_df.drop(columns=['Descricao da Ultima Ocorrencia'])

#Renomeia as colunas
ssw_455_df.rename(columns={'Usuario da Ultima Ocorrencia': 'Usuario da Ultima Ocorrencia - SSW 455',
                             'Data da Ultima Ocorrencia': 'Data da Ultima Ocorrencia - SSW 455',
                             'Codigo da Ultima Ocorrencia': 'Codigo da Ultima Ocorrencia - SWW 455',
                             'Ultimo Manifesto': 'Ultimo Manifesto - SSW 455',
                             'Data do Ultimo Manifesto': 'Data do Ultimo Manifesto - SSW 455',
                             'Ultimo Romaneio': 'Ultimo Romaneio - SSW 455',
                             'Data do Ultimo Romaneio': 'Data do Ultimo Romaneio - SSW 455',
                             'Data de Emissao': 'Data de Emissao - SSW 455',
                             'Previsao de Entrega': 'Previsao de Entrega - SSW 455',
                             'Setor de Destino': 'Setor de Destino - SSW 455',
                             'Cliente Destinatario': 'Cliente Destinatario - SSW 455',
                             'CEP do Destinatario': 'CEP do Destinatario - SSW 455',
                             'Numero da Nota Fiscal': 'Numero da Nota Fiscal - SSW 455'
                         } , inplace = True)
ssw_455_df['Ultimo Romaneio - SSW 455']=ssw_455_df['Ultimo Romaneio - SSW 455'].replace(' ', '', regex=True)
num_colunas = ssw_455_df.shape[1]
num_linhas = ssw_455_df.shape[0]
ssw_455_df.insert(loc=num_colunas, column='Chave Nota e Cliente Destinatario', value=ssw_455_df['Numero da Nota Fiscal - SSW 455'].astype(str)+ssw_455_df['Cliente Destinatario - SSW 455'].str[:4])
print("criou df SSW", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#faz o proc da descrição do Codigo
ssw_455_df = pd.merge(ssw_455_df, cod_ssw_455_df, on='Codigo da Ultima Ocorrencia - SWW 455', how='left')
#Faz o proc do SSW 455 e 036 pelo ultimo romaneio
ssw_455_df = pd.merge(ssw_455_df, ssw_036_df, on='Ultimo Romaneio - SSW 455', how='left')
ssw_455_df['Chave Nota e Cliente Destinatario'] = ssw_455_df['Chave Nota e Cliente Destinatario'].replace(' ', 'X', regex=True)

cod_ssw_455_df = pd.DataFrame()
ssw_036_df = pd.DataFrame()
print("criou proc com desc produto", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


#Proc das notas do tableau com as informações do WMS
base_aux_df = pd.merge(tableau_df, wms_colunas_df, on='Chave Pedido e Nota', how='left')
print("Concatenando TABLEAU E WMS")
print("proc tb e wms", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Concatenando as notas do tableau com as informações do WMS
base_df = pd.concat([base_aux_df, wms_df], ignore_index=True)
print(base_df.shape[0], "linhas")

tableau_df = pd.DataFrame()
wms_colunas_df = pd.DataFrame()
wms_df = pd.DataFrame()
base_aux_df = pd.DataFrame()
print("concatenou tb e wms", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#remove duplicatas
base_df = base_df.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)

#incluindo dia, dia(d) e Chave Nota Cep
num_colunas = base_df.shape[1]
base_df.insert(loc=num_colunas, column='Dia', value=d)
base_df.insert(loc=num_colunas+1, column='Contador', value=1)
base_df['04. NF Fornecedor'] = base_df['04. NF Fornecedor'].fillna(blank)
base_df['Chave Nota e Cliente Destinatario'] = base_df['04. NF Fornecedor'].astype(str) + base_df['12. Nome Cliente'].str[:4]
print("incluindo dia, dia(d) e Chave Nota Cep", " - %s" % (time.time() - start_time), " - segundos" , file=arquivo)

#Se ['04. NF Fornecedor'] == blank, utiliza a nota madeira, se não usa a nota fornecedor.
base_df.loc[(base_df['04. NF Fornecedor'] == blank), 'Chave Nota e Cliente Destinatario'] =(base_df['01. NF Madeira'].astype(str) + base_df['12. Nome Cliente'].str[:4])
print("chave master", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


#incluindo chave Nota e Cep
base_df['04. NF Fornecedor'] = base_df['04. NF Fornecedor'].fillna(blank)
base_df['Chave Nota e CEP'] = base_df['04. NF Fornecedor'].astype(str) + base_df['17. CEP']
print("incluindo dia, dia(d) e Chave Nota Cep", " - %s" % (time.time() - start_time), " - segundos" , file=arquivo)

#Se ['04. NF Fornecedor'] == blank, utiliza a nota madeira, se não usa a nota fornecedor.
base_df.loc[(base_df['04. NF Fornecedor'] == blank), 'Chave Nota e CEP'] =(base_df['01. NF Madeira'].astype(str) + base_df['17. CEP'])
print("chave master", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#renomeia as colunas
base_df.rename(columns = {"Classificacao Tipo Pedido": "89. Classificacao Tipo Pedido (WMS)",
                             "Data Esperada para Embarque": "90. Data Esperada para Embarque (WMS)",
                             "Cancelado Pelo ERP": "91. Cancelado Pelo ERP (WMS)",
                             "Onda": "92. Onda (WMS)",
                             "Cadastrada em": "93. Cadastrada em (WMS)",
                             "Importado em": "94. Importado em (WMS)",
                             "Roteirizado em": "95. Roteirizado em (WMS)",
                             "Separado em": "96. Separado em (WMS)",
                             "Conferido em": "97. Conferido em (WMS)",
                             "Pesado em": "98. Pesado em (WMS)",
                             "Cancelado em": "99. Cancelado em (WMS)",
                             "Coletado em": "100. Coletado em (WMS)",
                             "Rota": "101. Rota (WMS)"
                             }, inplace=True)
base_df['Chave Nota e Cliente Destinatario'] = base_df['Chave Nota e Cliente Destinatario'].replace(' ', 'X', regex=True)
print("renomeia cols", " - %s" % (time.time() - start_time), " - segundos", "- %s" % (d), file=arquivo)

#Proc com o SSW 455 pela Chave Nota e Cliente Destinatario
base_df = pd.merge(base_df, ssw_455_df, on='Chave Nota e Cliente Destinatario', how='left')
ssw_455_df = pd.DataFrame()
print("proc ssw pela chave", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#remove duplicatas
base_df = base_df.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first')
print('base pos remover duplicadas')
print(base_df.shape[0], " linhas")
print(base_df.shape[1], " colunas")
print("remove duplis", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#move chave pedido e nota de lugar
cols = base_df.columns.tolist()
column_to_move = "Chave Pedido e Nota"
new_position = 91
cols.insert(new_position, cols.pop(cols.index(column_to_move)))
base_df = base_df[cols]
print("move chave pedido e nota de lugar", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#######################################################################################################################
#      DATABASE - MANIFESTO
print("MANIFESTO")
colunas_manifesto_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Colunas_Manifesto.txt", sep='\t')
num_linhas = colunas_manifesto_df.shape[0]
list = []
for i in range(0, num_linhas):
    list.append(colunas_manifesto_df.values[i, 0])


#Criando dataframe do MANIFESTO
manifesto_df = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\Eagle_manifesto.csv', sep=';', usecols=list)
print("Criando dataframe do MANIFESTO", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#coloca na ordem da lista
manifesto_df = manifesto_df[list]
manifesto_df = manifesto_df.convert_dtypes() #ajustando os tipos da colunas
print("coloca na ordem da lista", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#criando coluna Chave pedido e nota
num_colunas = manifesto_df.shape[1]
manifesto_df.insert(loc=num_colunas, column='Chave Pedido e Nota', value=manifesto_df['Pedido Venda'].astype(str) + manifesto_df['Nota Madeira'].astype(str))

manifesto_df = manifesto_df.replace(['EM TRÂNSITO'],'EM TRANSITO')
manifesto_df = manifesto_df.replace(['Não Iniciada','Não Informada', 'Aguardando'], '' )
print("replace", " - %s" % (time.time() - start_time), " - segundos")

print("criando chave notapedido", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#AJUSTA OS NOMES DAS COLUNAS DO MANIFESTO
manifesto_df.rename(columns = {"Nº Manifesto": "Numero Manifesto",
                               "Origem (T2)": "Origem (T2) Manifesto",
                               "Destino": "Destino Manifesto",
                               "Placa": "Placa Manifesto",
                               "Peso": "Peso Manifesto",
                               "Início da Transferência": "Inicio da Transferencia Manifesto",
                               "Previsão de Chegada": "Previsao de Chegada Manifesto",
                               "Chegada": "Chegada Manifesto",
                               "Status": "Status Manifesto"
                               }, inplace=True)
manifesto_df = manifesto_df.drop(columns=['Pedido Venda', 'Nota Madeira'])

print("ajusta cols manifesto", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

##Proc Status Manifesto
statusManifesto_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\manifesto_DePara.csv", sep=';')
manifesto_df = pd.merge(manifesto_df, statusManifesto_df, on='Status Manifesto', how='left')
statusManifesto_df = pd.DataFrame()

print("proc status manifesto", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#remove duplicatas
#manifesto_df = manifesto_df.drop_duplicates(subset=['Numero Manifesto'], keep='first')

#Proc com o SSW 455 pela chave pedido e nota
base_df = pd.merge(base_df, manifesto_df, on='Chave Pedido e Nota', how='left')
manifesto_df = pd.DataFrame()


print("proc ssw 455", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#remove duplicatas
base_df = base_df.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first')

lista = ["25. Data Limite Cliente", "26. Data Limite Fornecedor", "27. Data Limite Coleta",
         "28. Data Limite Transporte", "29. Data Limite Redespacho",
         "37. Data Compra Cliente", "38. Data Criacao OC", "39. Data Aceite OC", "40. Data Faturamento NF MI",
         "41. Data BIP", "42. Data Expedicao", "46. Data Ultimo EDI", "48. Data CTRC Emitido T2",
         "49. Data Saida Origem T2", "50. Data Chegada Destino T2", "51. Data Em Rota de Entrega T2",
         "52. Data Redespacho", "53. Data Movimentacao T3", "54. Data CTRC Emitido T3", "55. Data Saida Origem T3",
         "56. Data Chegada Destino T3", "57. Data Em Rota de Entrega T3", "58. Data Entrega",
         "64. Data Apontamento Avaria", "66. Data Solicitacao Devolucao", "67. Data Analise Devolucao",
         "68. Data Solicitacao Contato", "70. Data Retorno Contato", "77. Data_Criacao_Pv", "78. Data_Revisada_Cliente",
         "90. Data Esperada para Embarque (WMS)", "93. Cadastrada em (WMS)", "94. Importado em (WMS)",
         "95. Roteirizado em (WMS)", "96. Separado em (WMS)", "97. Conferido em (WMS)", "98. Pesado em (WMS)",
         "99. Cancelado em (WMS)", "100. Coletado em (WMS)", "Dia", "Data da Ultima Ocorrencia - SSW 455",
         "Data do Ultimo Manifesto - SSW 455", "Data do Ultimo Romaneio - SSW 455", "Data de Emissao - SSW 455", 'Inicio da Transferencia Manifesto']


print("lista", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

for d in lista:
    #pd.to_datetime(df['Dates'], format='%y%m%d')
    base_df[d] = pd.to_datetime(base_df[d], infer_datetime_format=True, dayfirst=True)
print("converteu lista em data", " - %s" % (time.time() - start_time), " - segundos" , file=arquivo)

#colocando brancos
for y in base_df.columns:
    if (is_string_dtype(base_df[y])):
        base_df[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(base_df[y])):
        base_df[y].fillna(blank, inplace=True)
print("colocando brancos", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#######################################################################################################################
#                                                   COMEÇA MERGES
#######################################################################################################################
print('COMEÇA MERGERS - DEUS NO COMANDO')

base_df.insert(loc=num_colunas, column='Chave Pedido e SKU', value=base_df['02. Pedido'].astype(str) + base_df['07. Codigo SKU'].astype(str))

#Proc com o Pedidos Criticos CX
lista = ['Pedido_n', 'SKU','prio_necessidade_solucao']
criticos_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\56 - Qualidade Atendimento\1 - Lucas\prioridadeCX.csv", sep=',', usecols=lista)
num_colunas = criticos_df.shape[1]
criticos_df.insert(loc=num_colunas, column='Chave Pedido e SKU', value=criticos_df['Pedido_n'].astype(str) + criticos_df['SKU'].astype(str))
base_df = pd.merge(base_df, criticos_df, on='Chave Pedido e SKU', how='left')
print(criticos_df)

base_df['Aux_Prazo_Chao - DestinoOrigem'] = blank
base_df['Aux_Prazo_Chao - transp_origem'] = blank

#remove duplicatas
base_df = base_df.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)
base_df['Prazo Redespacho'] = base_df['Prazo Redespacho'].astype('Float32').astype('Int32')

#Proc com o EDI_De_Para.txt
edi_de_para_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\EDI_De_Para.csv", sep=';')
edi_de_para_df = ColunasCalculadas.ajustaLetras(edi_de_para_df)
edi_de_para_df = edi_de_para_df.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, edi_de_para_df, on='44. Ultimo EDI', how='left')
edi_de_para_df= pd.DataFrame()

print("edi de para", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Proc com o UF - DESTINO
types = {'87. UF Destino': 'string', 'Regiao': 'category', 'Macro Regiao': 'category'}
uf_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\UF.csv", sep=';', dtype= types)
#uf_df = ColunasCalculadas.ajustaLetras(uf_df)
#uf_df = uf_df.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, uf_df, on='87. UF Destino', how='left')
uf_df= pd.DataFrame()

base_df['Macro Regiao'] = np.where((base_df['87. UF Destino'] != "SP"), base_df['Macro Regiao'],
                                   np.where(
                                       (base_df['87. UF Destino'] == "SP") & (base_df['16. Destino'] == "SP-Interior"),
                                       "SP-Interior", 'SP-Capital') )

print("proc uf destino", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Proc com o Transportadora Redespacho.txt
types = {'22. Transp. Redespacho': 'category', 'Transportadora Redespacho': 'category'}
tp_redespacho_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Transportadora_Redespacho.csv", sep=';', dtype= types)
#tp_redespacho_df = ColunasCalculadas.ajustaLetras(tp_redespacho_df)
#tp_redespacho_df = tp_redespacho_df.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, tp_redespacho_df, on='22. Transp. Redespacho', how='left')
tp_redespacho_df= pd.DataFrame()

print("proc transp redespacho", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Proc com o Transportadora Entrega.txt
types = {'24. Transp. Entrega': 'category', 'Transportadora Last Mile': 'string', 'Tipo TP': 'string', 'FLAG_BLK': 'Int32', 'Analista Last Mile': 'string', 'Supervisao': 'string' }
tp_entrega_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Transportadora_Entrega.csv", sep=';', dtype=types)
#tp_entrega_df = tp_entrega_df.convert_dtypes() #ajustando os tipos da colunas
tipo_tp_df = tp_entrega_df.drop(['Transportadora Last Mile', 'FLAG_BLK', 'Analista Last Mile'], axis = 1)

tp_entrega_df = tp_entrega_df.drop(['Tipo TP'], axis = 1)
#tp_entrega_df = ColunasCalculadas.ajustaLetras(tp_entrega_df)
base_df = pd.merge(base_df, tp_entrega_df, on='24. Transp. Entrega', how='left')

print("proc transp entrega", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

tp_ultimo_EDI_df = tp_entrega_df.drop(['FLAG_BLK', 'Analista Last Mile'], axis = 1)
tp_ultimo_EDI_df.rename(columns ={'Transportadora Last Mile': 'TP Ultimo EDI',
                                  '24. Transp. Entrega': '43. Transp. Ultimo EDI'
                                  }, inplace=True)
base_df = pd.merge(base_df, tp_ultimo_EDI_df, on='43. Transp. Ultimo EDI', how='left')

tipo_tp_df.rename(columns ={'24. Transp. Entrega': '43. Transp. Ultimo EDI',
                            'Tipo TP': 'Tipo TP UltimoEDI'}, inplace=True)
base_df = pd.merge(base_df, tipo_tp_df, on='43. Transp. Ultimo EDI', how='left')
tp_entrega_df= pd.DataFrame()
tp_ultimo_EDI_df= pd.DataFrame()
tipo_tp_df= pd.DataFrame()

print("proc tipo tp", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#criando chave auxiliar - 22. tp redespacho e 43 tp ultimo edi
base_df.insert(loc=num_colunas, column='Chave22TPRedespahoe43TpEDI', value=base_df['22. Transp. Redespacho'].astype(str) + base_df['43. Transp. Ultimo EDI'].astype(str))

list = ['Chave22TPRedespahoe43TpEDI', 'FLAG EDI_consolidador']
#Proc com o Transportadora Entrega.txt
edi_cons = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Edi_Consolidador.csv", sep=';', usecols=list)
edi_cons = edi_cons.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, edi_cons, on='Chave22TPRedespahoe43TpEDI', how='left')
edi_cons= pd.DataFrame()

print("proc edi consolidador", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Proc com o Cluster UF
cluster_UF_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Cluster_UF.csv", sep=';')
#cluster_UF_df = ColunasCalculadas.ajustaLetras(cluster_UF_df)
cluster_UF_df = cluster_UF_df.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, cluster_UF_df, on='16. Destino', how='left')

base_df['88. Cidade'] = base_df['88. Cidade'].str.upper()
cluster_UF_df= pd.DataFrame()


print("proc cluster uf", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Proc com o Cidade_nextday
cidade_next_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Cidade_nextday.csv", sep=';')
cidade_next_df = ColunasCalculadas.ajustaLetras(cidade_next_df)
cidade_next_df = cidade_next_df.convert_dtypes() #ajustando os tipos da colunas
cidade_next_df['88. Cidade'] = cidade_next_df['88. Cidade'].str.upper()
base_df = pd.merge(base_df, cidade_next_df, on='88. Cidade', how='left')
cidade_next_df= pd.DataFrame()

print("proc cidade next day", " - %s" % (time.time() - start_time), " - segundos", "- %s" % (d), file=arquivo)

#colocando brancos
for y in base_df.columns:
    if (is_string_dtype(base_df[y])):
        base_df[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(base_df[y])):
        base_df[y].fillna(blank, inplace=True)

num_colunas = base_df.shape[1]
base_df.insert(loc=num_colunas, column='ChaveOrigem24TEntrega', value=base_df['15. Origem'].astype(str) + base_df['24. Transp. Entrega'].astype(str))

#Proc com o TranspRetira
list = ['ChaveOrigem24TEntrega', 'Retira']
types = {'ChaveOrigem24TEntrega': "string" , 'Retira': "string"}
transp_retira_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Transp_Retira.csv", sep=';', usecols=list, dtype= types)
#transp_retira_df = transp_retira_df.convert_dtypes() #ajustando os tipos da colunas
transp_retira_df = ColunasCalculadas.ajustaLetras(transp_retira_df)
base_df = pd.merge(base_df, transp_retira_df, on='ChaveOrigem24TEntrega', how='left')
transp_retira_df= pd.DataFrame()

print("proc transp retira", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#chave para proc unidades
#base_df['chave_unidade_transp'] = base_df['Transportadora Last Mile'].astype(str) + base_df['88. Cidade'].astype(str) + base_df ['87. UF Destino'].astype(str)
base_df['chave_unidade_transp'] = base_df.apply(lambda row: row['Transportadora Last Mile'] + row['88. Cidade'] + row[
                                                                                        '87. UF Destino'], axis=1)
base_df['chave_unidade_transp'] = base_df['chave_unidade_transp'].str.upper()

#Proc com o unidades
list = ['chave_unidade_transp', 'Unidade']
#types = {'chave_unidade_transp': "string" , 'Unidade': "string" }
unidades_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Unidades.csv", sep=';', usecols=list)
print("abriu csv", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)
#unidades_df = ColunasCalculadas.ajustaLetras(unidades_df)
unidades_df = unidades_df.convert_dtypes() #ajustando os tipos da colunas
unidades_df['chave_unidade_transp'] = unidades_df['chave_unidade_transp'].str.upper()
base_df = pd.merge(base_df, unidades_df, on='chave_unidade_transp', how='left')
unidades_df= pd.DataFrame()

print("proc unidades", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Proc com Consolidador
list = ['15. Origem', 'Consolidador']
types = {'15. Origem': 'string' , 'Consolidador': 'string'}
consolidador_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\consolidador.csv", sep=';', usecols=list, dtype= types)
consolidador_df = ColunasCalculadas.ajustaLetras(consolidador_df)
#consolidador_df = consolidador_df.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, consolidador_df, on='15. Origem', how='left')
consolidador_df= pd.DataFrame()

print("proc consolidador", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


#chave para proc Parceiros_Bulky
num_colunas = base_df.shape[1]
base_df.insert(loc=num_colunas, column='chave_parceiros', value=(base_df['88. Cidade'].astype(str) + base_df['87. UF Destino'].astype(str) + base_df['24. Transp. Entrega'].astype(str)))
base_df['chave_parceiros'] = base_df['chave_parceiros'].str.upper()

#Proc com Parceiros_Bulky)
list = ['chave_parceiros', 'Transportadora Parceiro', 'Flag(Parceiro-Bulky)', 'Prazo Chao Parceiro', 'Prazo Viagem Parceiro', 'Prazo Parceiro']
#types = {'chave_parceiros': 'string', 'Transportadora Parceiro':'category', 'Flag(Parceiro-Bulky)': 'Int32', 'Prazo Parceiro':'Int32', 'Risco Sobre. Parceiro': 'string'}
parceiros_bulky_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Parceiros_Bulky.csv", sep=';', usecols=list)
parceiros_bulky_df = ColunasCalculadas.ajustaLetras(parceiros_bulky_df)
parceiros_bulky_df = parceiros_bulky_df.convert_dtypes() #ajustando os tipos da colunas
parceiros_bulky_df['chave_parceiros'] = parceiros_bulky_df['chave_parceiros'].str.upper()

base_df = pd.merge(base_df, parceiros_bulky_df, on='chave_parceiros', how='left')
parceiros_bulky_df= pd.DataFrame()

print("proc parceiros bulky", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Proc com UnidadeSSW
list = ['Unidade da Ultima Ocorrencia', 'Parceiro Ultima Ocorrencia']
types = {'Unidade da Ultima Ocorrencia': 'category', 'Parceiro Ultima Ocorrencia': 'category'}
unidadessw_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\DePara - Pça - SSW.csv", sep=';', usecols=list, dtype= types)
unidadessw_df = ColunasCalculadas.ajustaLetras(unidadessw_df)
#unidadessw_df = unidadessw_df.convert_dtypes() #ajustando os tipos da colunas
unidadessw_df['Unidade da Ultima Ocorrencia'] = unidadessw_df['Unidade da Ultima Ocorrencia'].str.upper()

base_df = pd.merge(base_df, unidadessw_df, on='Unidade da Ultima Ocorrencia', how='left')
unidadessw_df= pd.DataFrame()

print("Proc com UnidadeSSW", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#Proc - diário de bordo (giro)
list = ['Chave', 'Data de Entrada', 'Tratativa', 'Motivo', 'Observação', 'Vencimento', 'Transp Apontada', 'Transp Diário']
types = {'Chave': 'string', 'Data de Entrada': 'string' , 'Tratativa': 'string' , 'Motivo': 'string' , 'Observação': 'string' , 'Vencimento': 'string' , 'Transp Apontada': 'string' , 'Transp Diário': 'string' }
base_diario = pd.read_excel(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\00- Diários_Novos\Rotina_Atualização_Geral.xlsm", sheet_name="Giro" , usecols=list, dtype= types)
base_diario.rename(columns = {'Chave': 'Chave Pedido e Nota' }, inplace=True)
base_diario.rename(columns = {'Data de Entrada': 'Data de Entrada - Diario' }, inplace=True)
base_diario.rename(columns = {'Tratativa': 'Tratativa - Diario' }, inplace=True)
base_diario.rename(columns = {'Motivo': 'Motivo - Diario' }, inplace=True)
base_diario.rename(columns = {'Observação': 'Observacao - Diario' }, inplace=True)
base_diario.rename(columns = {'Vencimento': 'Vencimento - Diario' }, inplace=True)
base_diario.rename(columns = {'Transp Apontada': 'Transp Apontada - Diario' }, inplace=True)
base_diario.rename(columns = {'Transp Diário': 'Transp Diario' }, inplace=True)
base_diario = ColunasCalculadas.ajustaLetras(base_diario)
#base_diario = base_diario.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, base_diario, on='Chave Pedido e Nota', how='left')
base_diario = pd.DataFrame()

print("proc diario", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


#Proc com o Dados CX
list = ['Chave', 'idfk_status']
dados_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Dados_CX.csv", sep=';', usecols= list)
dados_df.rename(columns = {'Chave': 'Chave Pedido e Nota' }, inplace=True)
dados_df = ColunasCalculadas.ajustaLetras(dados_df)
dados_df = dados_df.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, dados_df, on='Chave Pedido e Nota', how='left')
dados_df = pd.DataFrame()

print("Dados CX", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#remove duplicatas
base_df = base_df.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)
base_df = base_df.convert_dtypes() #ajustando os tipos da colunas

#colocando brancos
for y in base_df.columns:
    if (is_string_dtype(base_df[y])):
        base_df[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(base_df[y])):
        base_df[y].fillna(blank, inplace=True)



#remove duplicatas
base_df = base_df.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)
print('base df')

#######################################################################################################################
#                             C O M E Ç A R   O S   C A L C U L O S   A Q U I
#######################################################################################################################

linhas = base_df.shape[0]
print(linhas, " linhas")
linhas = linhas - 1

j = 10000
for i in range(0, 50000):
    if i == j:
        j = j + 10000
        print(i, " - ", datetime.today().strftime('%H:%M:%S'))




# Criando lista de colunas retiradas
colunas_retiradas = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Colunas_retiradas_para_calculo - Copia.csv", sep=';')
num_linhas = colunas_retiradas.shape[0]
list = []
for i in range(0, num_linhas):
    list.append(colunas_retiradas.values[i, 0])

base_reduzida = base_df.drop(list, axis = 1)
print('base reduzida')
print(base_reduzida.shape[0], " linhas")
print(base_reduzida.shape[1], " colunas")
col_anterior = base_reduzida.shape[1]

#colocando brancos
for y in base_reduzida.columns:
    if (is_string_dtype(base_df[y])):
        base_reduzida[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(base_df[y])):
        base_reduzida[y].fillna(blank, inplace=True)

print("começando os calculos", " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)
blank_str = str(blank)
print("--- %s seconds ---" % (time.time() - start_time))


#coluna Flag Guide
base_reduzida['Flag Guide'] = np.where(base_reduzida['81. Pedido Em Guideshop'] == "Pedido feito em Guideshop", 1, blank)
print('Guide', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


#coluna Status Redespacho
base_reduzida['Status Redespacho'] = np.where(base_reduzida['86. Tipo De Transporte'] == "Direto", "Direto",
                                              np.where((pd.isnull(base_reduzida['52. Data Redespacho'])) &
                                                       (pd.isnull(base_reduzida['55. Data Saida Origem T3'])) &
                                                       (pd.isnull(base_reduzida['56. Data Chegada Destino T3'])) &
                                                       (pd.isnull(base_reduzida['57. Data Em Rota de Entrega T3'])) &
                                                       (pd.isnull(base_reduzida['54. Data CTRC Emitido T3'])) &
                                                       (pd.isnull(base_reduzida['58. Data Entrega']))
                                                       , "Nao Redespachado", "Redespachado"))
print('Statusredespacho', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


#coluna Status Coleta
base_reduzida['Status coleta'] = np.where((pd.isnull(base_reduzida['42. Data Expedicao']) == False) &
                                          (base_reduzida['86. Tipo De Transporte'] == "Direto"),
                                          "Coletado", blank_str)
print('Status coleta', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


#coluna Status Last Mile
base_reduzida['Status Last Mile'] = np.where((base_reduzida['Status coleta'] == "Coletado") |
                                             (base_reduzida['Status Redespacho'] == "Redespachado"),
                                             "Em Last Mile", blank_str)
print('Status LM', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


#coluna Status Dados
base_reduzida['Status Dados'] = np.where((pd.isnull(base_reduzida['70. Data Retorno Contato'])) &
                                         (pd.isnull(base_reduzida['68. Data Solicitacao Contato'])),"Sem Solicitacao",
                                         np.where((pd.isnull(base_reduzida['70. Data Retorno Contato']) == False) &
                                                  (base_reduzida['74. Status Retorno Contato'] == "respondido") |
                                                  (base_reduzida['74. Status Retorno Contato'] == "finalizado")
                                                  , "Dados Respondidos", "Dados Solicitados"))
print('Dados', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


base_reduzida["FLAG NEXT DAY"] = np.where(  (base_reduzida['Cidade (Next Day)'] == 1) &
                                                   (base_reduzida["15. Origem"]== "CD JDI Fulfillment") &
                                                   (base_reduzida['Transportadora Last Mile'] == "FULFILLMENT"), 1, 0)
print('FLAG NEXT DAY', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


base_reduzida["Transportadora_LM V2"] = np.where(  (base_reduzida['Cidade (Next Day)'] == 1) &
                                                   (base_reduzida["15. Origem"]== "CD JDI Fulfillment") &
                                                   (base_reduzida['Transportadora Last Mile'] == "FULFILLMENT"),
                                                        "FULFILLMENT -  NEXT DAY",
                                                        base_reduzida['Transportadora Last Mile'])
print('Transportadora_LM V2', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)



base_reduzida["Transp. LM"] = np.vectorize(ColunasCalculadas.transpLMFinal, otypes=[np.ndarray])(base_reduzida['Transportadora Last Mile'],base_reduzida['Transportadora Parceiro'],base_reduzida['Transportadora_LM V2'])
print('Transp. LM', " - %s" % (time.time() - start_time), " - segundos", "- %s" % (d), file=arquivo)

#Aging EDI
base_reduzida["Aging EDI"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['46. Data Ultimo EDI'],base_reduzida['Dia'])
print('aging edi', " - %s" % (time.time() - start_time), " - segundos", "- %s" % (d), file=arquivo)

#Cluster EDI
base_reduzida["Cluster EDI"] = np.vectorize(ColunasCalculadas.clusterDUTIL_EDI, otypes=[np.ndarray])(base_reduzida['46. Data Ultimo EDI'],base_reduzida['Dia'])
print('Cluster edi', " - %s" % (time.time() - start_time), " - segundos", "- %s" % (d), file=arquivo)



#Virada Errada
base_reduzida["Virada Errada"] = np.vectorize(ColunasCalculadas.viradaErrada, otypes=[np.ndarray])(base_reduzida['TP Ultimo EDI'],base_reduzida['Transportadora Last Mile'], base_reduzida['Transportadora Redespacho'], base_reduzida['FLAG EDI_consolidador'],base_reduzida['Tipo TP UltimoEDI'],
                                                                                        base_reduzida['24. Transp. Entrega'], base_reduzida['22. Transp. Redespacho'], base_reduzida['23. Transp. Entrega NF'], base_reduzida['43. Transp. Ultimo EDI'], base_reduzida['87. UF Destino'], base_reduzida['Regiao'])
print('virada', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

base_reduzida['Flag Virada Errada'] = np.where(base_reduzida['Virada Errada'] == "Virada Errada", 1, blank)
print('Flag Virada Errada', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)


base_reduzida['Chave_viradas'] = np.where(base_reduzida['Flag Virada Errada'] == 1,
                                          base_reduzida['23. Transp. Entrega NF'].astype(str) + " > " + base_reduzida[
                                              'Transportadora Last Mile'].astype(str), blank_str)
print('Chave_viradas', " - %s" % (time.time() - start_time), " - segundos", "- %s" % (d), file=arquivo)

#def data_ultima_wms(d93, d94, d95, d96, d97, d98, d99, d100):
base_reduzida["Data Step WMS"], base_reduzida["Step WMS"] = zip(*base_reduzida.apply(lambda row:
                                                                                ColunasCalculadas.data_ultima_wms(
                                                                                    row["93. Cadastrada em (WMS)"],
                                                                                    row["94. Importado em (WMS)"],
                                                                                    row["95. Roteirizado em (WMS)"],
                                                                                    row["96. Separado em (WMS)"],
                                                                                    row["97. Conferido em (WMS)"],
                                                                                    row["98. Pesado em (WMS)"],
                                                                                    row["99. Cancelado em (WMS)"],
                                                                                    row["100. Coletado em (WMS)"])
                                                                                     ,axis=1))
print('Data WMS', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

#posição final
base_reduzida["p1 - aux"], base_reduzida["Posicao Final"] = zip(*base_reduzida.apply(lambda row:
                                                                                ColunasCalculadas.posicaoFinal(
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
print('pos final', " - %s" % (time.time() - start_time), " - segundos" , "- %s" % (d), file=arquivo)

# Fase Nota
base_reduzida["Fase Nota"]= np.vectorize(ColunasCalculadas.fasenota, otypes=[np.ndarray])(base_reduzida['Posicao Final'],base_reduzida['15. Origem'],
                                                                                                               base_reduzida['p1 - aux'], base_reduzida['Arquivo'],
                                                                                                               base_reduzida['42. Data Expedicao'], base_reduzida['44. Ultimo EDI'])
print('fase nota', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#def data_pos_final(posfinal, dfat40, dbip41, dexp42, dedi46, dsolcontato68, dretcontato70, edidepara, dultima_ssw, dultima_wms):
#Data Posiçao Final

base_reduzida["Data Posicao Final"] = base_reduzida.apply(
    lambda row: ColunasCalculadas.data_pos_final(row["Posicao Final"],
                                                 row["40. Data Faturamento NF MI"], row["41. Data BIP"],
                                                 row["42. Data Expedicao"], row["46. Data Ultimo EDI"],
                                                 row["68. Data Solicitacao Contato"], row["70. Data Retorno Contato"],
                                                 row["DExPARA EDI"], row["Data da Ultima Ocorrencia - SSW 455"],
                                                 row["Data Step WMS"]), axis=1)
base_reduzida["Data Posicao Final"] = pd.to_datetime(base_reduzida["Data Posicao Final"], infer_datetime_format=True, dayfirst=True)
print('Data Posicao Final', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)
print(base_reduzida.dtypes)


# #Aging Posicao Final
base_reduzida["Aging Posicao Final"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['Data Posicao Final'],base_reduzida['Dia'])

print('aging Posicao Final', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

# #Cluster Posicao Final
base_reduzida["Cluster Posicao Final"] = np.vectorize(ColunasCalculadas.clusterDUTIL, otypes=[np.ndarray])(base_reduzida['Aging Posicao Final'])

print('Cluster Posicao Final', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


#Proc com o Step Macro
types = {'Posicao Final': 'string', 'Aux - Step Macro': 'string'}
stepmacro_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Step_Macro.csv", sep=';', dtype= types)
stepmacro_df = ColunasCalculadas.ajustaLetras(stepmacro_df)
#stepmacro_df = stepmacro_df.convert_dtypes() #ajustando os tipos da colunas
base_reduzida = pd.merge(base_reduzida, stepmacro_df, on='Posicao Final', how='left')
#remove duplicatas
base_reduzida = base_reduzida.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)
stepmacro_df= pd.DataFrame()

#step_macro(posfinal, p1, aux, arquivo, d42, edi_44):
base_reduzida["Step Macro"] = np.vectorize(ColunasCalculadas.step_macro, otypes=[np.ndarray])(base_reduzida['Posicao Final'],base_reduzida['p1 - aux'],
                                                                                                               base_reduzida['Aux - Step Macro'], base_reduzida['Arquivo'],
                                                                                                               base_reduzida['42. Data Expedicao'], base_reduzida['44. Ultimo EDI'])

print('Step Macro', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)
list = ["p1 - aux", "Aux - Step Macro"]
base_reduzida = base_reduzida.drop(columns=list, axis=1)
base_reduzida["Step Macro"] = base_reduzida["Step Macro"].fillna(blank_str)

#Vencimento T3
base_reduzida["Vencimento T3"] = np.vectorize(ColunasCalculadas.menorData, otypes=[np.ndarray])(base_reduzida['25. Data Limite Cliente'],base_reduzida['28. Data Limite Transporte'])

print('Vencimento T3', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Aging Vencimento T3
base_reduzida["Aging Vencimento T3"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['Vencimento T3'],base_reduzida['Dia'])

print('agingVencimento T3', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Cluster Vencimento T3
base_reduzida["Cluster Vencimento T3"] = np.vectorize(ColunasCalculadas.clusterDUTIL, otypes=[np.ndarray])(base_reduzida['Aging Vencimento T3'])

print('Cluster Vencimento T3', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Status Vencimento T3
base_reduzida["Status Vencimento T3"] = np.vectorize(ColunasCalculadas.statusVencimento, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Vencimento T3'])

print('Status Vencimento T3', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Flags Atraso Vencimento T3
base_reduzida["Status Atraso Vencimento T3"], base_reduzida["Flag Atraso Vencimento T3"], base_reduzida["Flag Atraso Vencimento T3 > 3D"], \
base_reduzida["Flag Atraso Vencimento T3 > 5D"], base_reduzida["Flag Atraso Vencimento T3 > 10D"], base_reduzida[
    "Flag Atraso Vencimento T3 > 15D"], base_reduzida["Flag Atraso Vencimento T3 > 30D"], base_reduzida[
    "Flag Atraso Vencimento T3 = 5D"] = zip(
    *base_reduzida.apply(lambda row: ColunasCalculadas.flagAtraso(row["Aging Vencimento T3"]), axis=1))

print('Flags Atraso Vencimento T3', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

# Aging Cliente
base_reduzida["Aging Cliente"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['25. Data Limite Cliente'], base_reduzida['Dia'])

print('agingCliente', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


#Cluster Cliente
base_reduzida["Cluster Cliente"] = np.vectorize(ColunasCalculadas.clusterDUTIL, otypes=[np.ndarray])(base_reduzida['Aging Cliente'])

print('Cluster Cliente', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Status Vencimento Cliente
base_reduzida["Status Vencimento Cliente"] = np.vectorize(ColunasCalculadas.statusVencimento, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['25. Data Limite Cliente'] )

print('Status Vencimento Cliente', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Flags Atraso Cliente
base_reduzida["Status Atraso Cliente"], base_reduzida["Flag Atraso Cliente"], base_reduzida["Flag Atraso Cliente > 3D"], \
base_reduzida["Flag Atraso Cliente > 5D"], base_reduzida["Flag Atraso Cliente > 10D"], base_reduzida[
    "Flag Atraso Cliente > 15D"], base_reduzida["Flag Atraso Cliente > 30D"], base_reduzida[
    "Flag Atraso Cliente = 5D"] = zip(
    *base_reduzida.apply(lambda row: ColunasCalculadas.flagAtraso(row["Aging Cliente"]), axis=1))

print('Flags Atraso Cliente', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Aging Transporte
base_reduzida["Aging Transporte"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['28. Data Limite Transporte'], base_reduzida['Dia'] )

print('agingTransporte', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Cluster Transporte
base_reduzida["Cluster Transporte"] = np.vectorize(ColunasCalculadas.clusterDUTIL, otypes=[np.ndarray])(base_reduzida['Aging Transporte'] )

print('Cluster Transporte', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Status Vencimento Transporte
base_reduzida["Status Vencimento Transporte"] = np.vectorize(ColunasCalculadas.statusVencimento, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['28. Data Limite Transporte'] )

print('Status Vencimento Transporte', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Flags Atraso Transporte
base_reduzida["Status Atraso Transporte"], base_reduzida["Flag Atraso Transporte"], base_reduzida["Flag Atraso Transporte > 3D"], \
base_reduzida["Flag Atraso Transporte > 5D"], base_reduzida["Flag Atraso Transporte > 10D"], base_reduzida[
    "Flag Atraso Transporte > 15D"], base_reduzida["Flag Atraso Transporte > 30D"], base_reduzida[
    "Flag Atraso Transporte = 5D"] = zip(
    *base_reduzida.apply(lambda row: ColunasCalculadas.flagAtraso(row["Aging Transporte"]), axis=1))

print('Flags Atraso Transporte', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Data Limite T2

base_reduzida["Data_Limite_T2"] = base_reduzida.apply(
                                        lambda row: ColunasCalculadas.data_limite_t2(row['42. Data Expedicao'],
                                                                                  row['Prazo Redespacho'],row["30. Prazo Redespacho"]), axis=1)
print('Data limite T2', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Data Limite Chão Parceiro

base_reduzida["Data_Limite_Chao_Parceiro"] = base_reduzida.apply(
                                        lambda row: ColunasCalculadas.data_limite(row['54. Data CTRC Emitido T3'],
                                                                                  row['Prazo Chao Parceiro']), axis=1)

print('Data_Limite_Chao_Parceiro', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Data Limite Transferência Parceiro

base_reduzida["Data_Limite_Transf_Parceiro"] = base_reduzida.apply(
                                        lambda row: ColunasCalculadas.data_limite(row['55. Data Saida Origem T3'],
                                                                                  row['Prazo Viagem Parceiro']), axis=1)

print('Data_Limite_Transf_Parceiro', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Data_Limite_Parceiro

base_reduzida["Data_Limite_Parceiro"] = base_reduzida.apply(
                                        lambda row: ColunasCalculadas.data_limite(row['54. Data CTRC Emitido T3'],
                                                                                  row['Prazo Parceiro']), axis=1)

print('Data_Limite_Parceiro', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)



#Status Transf Parceiros
base_reduzida["Status Transferencia Parceiro"] = np.vectorize(ColunasCalculadas.statusVencimentoparc, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data_Limite_Transf_Parceiro'])
print('Status Transferencia Parceiro', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Status Chao Parceiros
base_reduzida["Status Chao Parceiro"] = np.vectorize(ColunasCalculadas.statusVencimentoparc, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data_Limite_Chao_Parceiro'])
print('Status Chao Parceiro', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#Status Parceiros
base_reduzida["Status Parceiro"] = np.vectorize(ColunasCalculadas.statusVencimentoparc, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data_Limite_Parceiro'])
print('Status Chao Parceiro', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


# #erro prazo cliente
base_reduzida["Flag Erro Prazo Cliente"] = np.vectorize(ColunasCalculadas.erroprazoCliente, otypes=[np.ndarray])(base_reduzida['25. Data Limite Cliente'], base_reduzida['26. Data Limite Fornecedor'], base_reduzida['27. Data Limite Coleta'], base_reduzida['28. Data Limite Transporte'], base_reduzida['41. Data BIP'], base_reduzida['42. Data Expedicao'])

print('Flag Erro Prazo Cliente', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)



# Atraso Fornecedor
base_reduzida["Flag Atraso Fornecedor"] = np.vectorize(ColunasCalculadas.atraso_fornecedor, otypes=[np.ndarray])(base_reduzida['26. Data Limite Fornecedor'], base_reduzida['41. Data BIP'], base_reduzida['Dia'])

# Aging resposta dados
base_reduzida["Aging Resposta Dados"] = np.vectorize(ColunasCalculadas.aging_resp_dados, otypes=[np.ndarray])(base_reduzida['26. Data Limite Fornecedor'], base_reduzida['68. Data Solicitacao Contato'], base_reduzida['Dia'], base_reduzida['74. Status Retorno Contato'])

print('Aging Resposta Dados', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#aging_emissao_cte(d_42, d_52, d_54, tp22):
# Aging emissao cte
base_reduzida["Aging emissao cte"] = np.vectorize(ColunasCalculadas.aging_emissao_cte, otypes=[np.ndarray])(base_reduzida['42. Data Expedicao'], base_reduzida['52. Data Redespacho'], base_reduzida['54. Data CTRC Emitido T3'], base_reduzida['22. Transp. Redespacho'])


print('Aging emissao cte', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Prazo Transporte
base_reduzida["Prazo Transporte"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['42. Data Expedicao'], base_reduzida['28. Data Limite Transporte'])


print('Prazo Transporte', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Prazo Transporte restante
base_reduzida["Prazo transporte restante"] = np.where(base_reduzida['Aging Transporte'] == blank, blank, base_reduzida['Aging Transporte']*(-1))
print('Prazo transporte restante', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Cont_stats
base_reduzida["cont_stats"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data_Limite_T2'])

print('Cont Status', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Flag_Atraso_Redespacho
base_reduzida["Flag_Atraso_Redespacho"] = np.where((base_reduzida["Fase Nota"] == "EM REDESPACHO") &
                                                                    (base_reduzida["cont_stats"] < 0), 1, blank)
print('Flag_Atraso_Redespacho', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Cluster Aging T2

base_reduzida["Cluster T2"] = np.vectorize(ColunasCalculadas.clusterDUTIL, otypes=[np.ndarray])(base_reduzida['cont_stats'])


#Status_Final_T2
base_reduzida["Status_Final_T2"] = np.vectorize(ColunasCalculadas.statusVencimentoT2, otypes=[np.ndarray])(base_reduzida['86. Tipo De Transporte'], base_reduzida['Dia'], base_reduzida['Data_Limite_T2'])

print('Status_Final_T2', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)



# prazo_last_mile
#prazo_last_mile(dltransp,dexp,ptransp,predespacho):
base_reduzida["Prazo Last Mile"] = np.vectorize(ColunasCalculadas.prazo_last_mile, otypes=[np.ndarray])(base_reduzida['28. Data Limite Transporte'], base_reduzida['42. Data Expedicao'], base_reduzida['Prazo Transporte'], base_reduzida['Prazo Redespacho'])

print('Prazo Last Mile', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida['Data_Redespacho OU Cte_Emitido'] = np.where(pd.isnull(base_reduzida['52. Data Redespacho']) == False,
                                                           base_reduzida['52. Data Redespacho'],np.where (pd.isnull(base_reduzida['54. Data CTRC Emitido T3']) == False,
                                                                    base_reduzida['54. Data CTRC Emitido T3'], pd.NaT))
print('Data_Redespacho OU Cte_Emitido', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["Destinos_SLA"] = np.vectorize(ColunasCalculadas.destinoSLA, otypes=[np.ndarray])(base_reduzida['Regiao'], base_reduzida['87. UF Destino'], base_reduzida['16. Destino'])

print('Destinos_SLA', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["Flag Atraso T3"] = np.where((base_reduzida['Status Vencimento T3'] == "01. Atraso") |
                                           (base_reduzida["Status Vencimento T3"] == "02. Atraso Ontem"), 1, 0)
print('Flag Atraso T3', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["Status_MacroT2"] = np.where((base_reduzida['Status_Final_T2'] == "01. Atraso") |
                                           (base_reduzida['Status_Final_T2'] == "02. Atraso Ontem"), "Atraso",
                                     np.where(base_reduzida['Status_Final_T2'] == '03. Vence Hoje', "Vence Hoje", "No Prazo"))
print('Status_MacroT2', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)



#Data_Limite_Consolidador

base_reduzida["Data Limite Consolidador"] = base_reduzida.apply(lambda row: ColunasCalculadas.data_limite(row["42. Data Expedicao"],
                                                                                                row["Prazo Chao T2"]), axis=1)
print('Data Limite Consolidador', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Flag Recompra
base_reduzida["Flag Recompra"] = np.where(base_reduzida["Chave Pedido e Nota"].str.startswith("Z"),1,0)
print('Flag Recompra', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Cont_stats
base_reduzida["Cont_Stats_Consolidador"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data Limite Consolidador'])

print('Cont_Stats_Consolidador', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Status_chao_Consolidador
base_reduzida["Status_chao_Consolidador"] = np.vectorize(ColunasCalculadas.statusVencimento, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data Limite Consolidador'])

print('Status_chao_Consolidador', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

base_reduzida["Status_chao_Consolidador_final"] = np.where((base_reduzida['Status_chao_Consolidador'] == "01. Atraso") |
                                           (base_reduzida['Status_chao_Consolidador'] == "02. Atraso Ontem"), "Atraso",
                                     np.where(base_reduzida['Status_chao_Consolidador'] == '03. Vence Hoje', "Vence Hoje", "No Prazo"))
print('Status_chao_Consolidador_final', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["Aging Falta Emissao Cte"] = np.vectorize(ColunasCalculadas.aging_falta_emissao, otypes=[np.ndarray])(base_reduzida['Step Macro'], base_reduzida['22. Transp. Redespacho'], base_reduzida['Dia'], base_reduzida['42. Data Expedicao'], base_reduzida['52. Data Redespacho'])


print('Aging Falta Emissao Cte', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["Flag_Movimentacao_T3"] = np.where(pd.isnull(base_reduzida["53. Data Movimentacao T3"]), 0, 1)
print('Flag_Movimentacao_T3', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

base_reduzida["Flag Aging Edi > 1 Cte"] = np.where((base_reduzida["Step Macro"] == "EMITIR CTE - T3") &
                                                   (base_reduzida["Aging Falta Emissao Cte"] > 1), 1, blank)
print('Flag Aging Edi > 1 Cte', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

base_reduzida["Flag Aging Edi > 2 Transfer"] = np.where((base_reduzida["Step Macro"] == "TRANSFERIR") &
                                                   (base_reduzida["Aging EDI"] > 2), 1, blank)
print('Flag Aging Edi > 2 Transfer', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

base_reduzida["riscoErroPrazo"] = np.where( (pd.isnull(base_reduzida['28. Data Limite Transporte'])) |
                                             (base_reduzida["Prazo Redespacho"] == 0), blank,
											np.where(base_reduzida['28. Data Limite Transporte'] < base_reduzida['Data_Limite_T2'],
											"Erro", "OK"))
print('riscoErroPrazo', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#stepparceiro(edi44, d53, d54, d55, d56):
base_reduzida["Step_parceiro"] = np.vectorize(ColunasCalculadas.stepparceiro, otypes=[np.ndarray])(base_reduzida['44. Ultimo EDI'], base_reduzida['53. Data Movimentacao T3'],
                                                                                                                             base_reduzida['54. Data CTRC Emitido T3'], base_reduzida['55. Data Saida Origem T3'], base_reduzida['56. Data Chegada Destino T3'], base_reduzida['57. Data Em Rota de Entrega T3'])

print('Step_parceiro', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Prazo_Aux_Parceiro
base_reduzida["Prazo_Aux_Parceiro"] = np.where((base_reduzida['Transp. LM']) == "ARAPON-BULKY", 4, 3)

print('Prazo_Aux_Parceiro', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


#coluna Data_Lim_LM_Parceiro
base_reduzida['Data_Lim_LM_Parceiro'] = base_reduzida.apply(lambda row: ColunasCalculadas.data_limite(
                                                                    row['54. Data CTRC Emitido T3'], row['Prazo_Aux_Parceiro']), axis=1)

print('Data_Lim_LM_Parceiro', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Cont_Status_Parceiros
base_reduzida["Cont_Status_Parceiros"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data_Lim_LM_Parceiro'])

print('Cont_Status_Parceiros', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Flag_Atraso_Parceiros
base_reduzida["Flag_Atraso_Parceiros"] = np.where((base_reduzida["Fase Nota"] == "EM LAST MILE") &
                                                  (base_reduzida['Flag(Parceiro-Bulky)'] == 1) &
                                                                    (base_reduzida["Cont_Status_Parceiros"] < 0), 1, blank)
print('Flag_Atraso_Parceiros', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Status_Parceiros_att
base_reduzida["Status_Parceiros_att"] = np.vectorize(ColunasCalculadas.statusVencimento, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data_Lim_LM_Parceiro'])

print('Status_Parceiros_att', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

# Data_Limite_Transferencia

base_reduzida["Data_Limite_Transferencia"] = base_reduzida.apply(lambda row: ColunasCalculadas.DL_transf(
                                                                               row['49. Data Saida Origem T2'],
                                                                               row['Inicio da Transferencia Manifesto'],
                                                                               row["Placa Manifesto"],
                                                                               row["Prazo_Viagem_T2"]), axis=1)
print('Data_Limite_Transferencia', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Cont_Status_Transferencia
base_reduzida["Cont_Status_Transferencia"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data_Limite_Transferencia'])

print('Cont_Status_Transferencia', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

base_reduzida["Flag_Atraso_Transferencia_T2"] = np.where((base_reduzida["Fase Nota"] == "EM REDESPACHO") &
                                                                    (base_reduzida["Cont_Status_Transferencia"] < 0), 1, blank)

#Status_Transf_T2
base_reduzida["Status_Transf_T2"] = np.vectorize(ColunasCalculadas.statusVencimento, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Data_Limite_Transferencia'])

print('Status_Transf_T2', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Status_Transferencias_T2
base_reduzida["Status_Transferencias_T2"] = np.vectorize(ColunasCalculadas.statustransfT2, otypes=[np.ndarray])(base_reduzida['Cont_Status_Transferencia'], base_reduzida['Status_Transf_T2'])

print('Status_Transferencias_T2', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Status_Macro_Transferencia
base_reduzida["Status_Macro_Transf"] = np.where((base_reduzida['Status_Transf_T2'] == "01. Atraso") |
                                           (base_reduzida['Status_Transf_T2'] == "02. Atraso Ontem"), "Atraso",
                                     np.where(base_reduzida['Status_Transf_T2'] == '03. Vence Hoje', "Vence Hoje", "No Prazo"))
print('Status_Macro_Transf', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Data BIP ajustada

base_reduzida['Data BIP ajustada'] = base_reduzida.apply(lambda row: ColunasCalculadas.diabip_ajust(
                                                                                    row['41. Data BIP']), axis=1)
print('Data BIP ajustada', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#coluna Dia Semana - DATA BIP

base_reduzida["Dia Semana DATA BIP"] = base_reduzida.apply(lambda row: ColunasCalculadas.weeknum(row['Data BIP ajustada']), axis=1)
print('Dia Semana - DATA BIP', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["Transp. T2 / Origem"] = np.where(base_reduzida['22. Transp. Redespacho'] == blank_str, "Sem origem",
                                                np.where((base_reduzida['15. Origem'] == blank_str) | (
                                                            base_reduzida['15. Origem'] == "Origem Temporária"),
                                                         "Sem origem",
                                                         np.where((base_reduzida['22. Transp. Redespacho'] == "Direto"),
                                                                  "Direto",
                                                                  base_reduzida['22. Transp. Redespacho'].astype(
                                                                      str) + " / " + base_reduzida['15. Origem'].astype(
                                                                      str))))
print('Transp. T2 / Origem', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Proc com o coletas
coletas_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Coletas_T2.csv", sep=';')
coletas_df = ColunasCalculadas.ajustaLetras(coletas_df)
coletas_df = coletas_df.convert_dtypes() #ajustando os tipos da colunas
list = ["TRANSPORTADORA", "ORIGEM", "CONSOLIDADOR", "ANALISTA", "DIAS DA SEMANA"]
coletas_df = coletas_df.drop(columns=list, axis=1)
base_reduzida = pd.merge(base_reduzida, coletas_df, on='Transp. T2 / Origem', how='left')
coletas_df= pd.DataFrame()
print('Coletas T2', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#remove duplicatas
base_reduzida = base_reduzida.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)

base_reduzida["QTD COLETAS NA SEMANA"] = base_reduzida["QTD COLETAS NA SEMANA"].fillna(blank)
base_reduzida["COLETA 1"] = base_reduzida["COLETA 1"].fillna(blank)
base_reduzida["COLETA 2"] = base_reduzida["COLETA 2"].fillna(blank)
base_reduzida["COLETA 3"] = base_reduzida["COLETA 3"].fillna(blank)

# PROX. DIA COLETA

base_reduzida["PROX. DIA COLETA"] = base_reduzida.apply(lambda row: ColunasCalculadas.coletas_t2(
                                                                                        row["Fase Nota"],
                                                                                        row["Dia Semana DATA BIP"],
                                                                                        row["QTD COLETAS NA SEMANA"],
                                                                                        row["COLETA 1"],
                                                                                        row["COLETA 2"],
                                                                                        row["COLETA 3"]), axis=1)
print('PROX. DIA COLETA', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

# def dia_prox_coleta(fasenota, diasemanaBip, d27, databip, prox):
base_reduzida["PROXIMA COLETA"] = np.vectorize(ColunasCalculadas.dia_prox_coleta, otypes=[np.ndarray])(base_reduzida['Fase Nota'], base_reduzida['Dia Semana DATA BIP'],
                                                                                                             base_reduzida['27. Data Limite Coleta'], base_reduzida['Data BIP ajustada'],
                                                                                                             base_reduzida['PROX. DIA COLETA'])

print('PROXIMA COLETA', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)



base_reduzida["Cont_Status_Coleta"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['PROXIMA COLETA'])

print('Cont_Status_Coleta', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


# def status_coleta_fornecedor_t2(fasenota, proxcoleta, data_hoje):
base_reduzida["Status Coleta Fornecedor T2"] = np.vectorize(ColunasCalculadas.status_coleta_fornecedor_t2, otypes=[np.ndarray])(base_reduzida['Fase Nota'], base_reduzida['PROXIMA COLETA'], base_reduzida['Dia'])

print('Status Coleta Fornecedor T2', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

base_reduzida["Atraso Cliente Aux"] = np.where((base_reduzida["Aging Cliente"] == blank) |
                                               (base_reduzida["Arquivo"] == "WMS"), 0,
                                               np.where(base_reduzida["Aging Cliente"] > 0, 1, 0))
print('Atraso Cliente Aux', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["Atraso Transporte Aux"] = np.where(base_reduzida["Aging Transporte"] == blank, 0,
                                                    np.where(base_reduzida["Aging Transporte"] > 0, 1, 0))
print('Atraso Transporte Aux', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


base_reduzida["Atraso Fornecedor Aux"] = np.vectorize(ColunasCalculadas.atraso_forn_aux, otypes=[np.ndarray])(base_reduzida['15. Origem'], base_reduzida['26. Data Limite Fornecedor'], base_reduzida['41. Data BIP'], base_reduzida['42. Data Expedicao'], base_reduzida['Dia'])

print('Atraso Fornecedor Aux', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


#def atraso_coleta_aux(origem, d27, d42, hoje):
base_reduzida["Atraso Coleta Aux"] = np.vectorize(ColunasCalculadas.atraso_coleta_aux, otypes=[np.ndarray])(base_reduzida['15. Origem'], base_reduzida['27. Data Limite Coleta'], base_reduzida['42. Data Expedicao'], base_reduzida['Dia'])

print('Atraso Coleta Aux', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


base_reduzida["Atraso Transp Sem FF Aux"] = np.where((base_reduzida["Atraso Cliente Aux"]== 1) &
                                                         (base_reduzida["Atraso Fornecedor Aux"] == 0) &
                                                         (base_reduzida["Atraso Coleta Aux"] == 0) &
                                                         (base_reduzida["15. Origem"] != "CD JDI Fulfillment") &
                                                         (base_reduzida["Atraso Transporte Aux"] == 1), 1, 0)
print('Atraso Transp Sem FF Aux', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


base_reduzida["Atraso NextDay Aux"] = np.where( (base_reduzida["Atraso Cliente Aux"]== 1) &
												(base_reduzida['Cidade (Next Day)'] == 1) &
                                                (base_reduzida["15. Origem"]== "CD JDI Fulfillment"),1 , 0)
print('Atraso NextDay Aux', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


base_reduzida["Atraso CD/TP FF Aux"] = np.vectorize(ColunasCalculadas.atraso_cd_ff_aux, otypes=[np.ndarray])(base_reduzida['15. Origem'], base_reduzida['42. Data Expedicao'], base_reduzida['54. Data CTRC Emitido T3'], base_reduzida['Dia'], base_reduzida['Atraso Cliente Aux'])

print('Atraso CD/TP FF Aux', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


base_reduzida["Atraso TP + TP FF Aux"] = np.where((base_reduzida["Atraso Cliente Aux"] == 1) &
                                                    ((base_reduzida["Atraso Transp Sem FF Aux"] == 1) |
                                                     (base_reduzida["Atraso CD/TP FF Aux"] == "TP")),1 ,0)
print('Atraso TP + TP FF Aux', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


base_reduzida["Atraso CD FF Aux"] = np.where((base_reduzida["Atraso Cliente Aux"] == 1) &
                                               (base_reduzida["Atraso CD/TP FF Aux"] == "FF"),1 , 0)
print('Atraso CD FF Aux', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


# base_reduzida["Reprogramado"] = np.vectorize(ColunasCalculadas.reprogramado, otypes=[np.ndarray])(base_reduzida['25. Data Limite Cliente'], base_reduzida['26. Data Limite Fornecedor'])
base_reduzida["Reprogramado"] = base_reduzida.apply(lambda row: ColunasCalculadas.reprogramado(
                                                                                row['25. Data Limite Cliente'],
                                                                                row['26. Data Limite Fornecedor']),
                                                                                            axis=1)
print('Reprogramado', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

base_reduzida["FlagAtraso Fornecedor"] = np.where((base_reduzida["Atraso Cliente Aux"] == 1) &
                                                  (base_reduzida["Atraso Fornecedor Aux"] == 1), 1, 0)
print('FlagAtraso Fornecedor', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


base_reduzida["FlagAtraso Coleta"] = np.where((base_reduzida["Atraso Cliente Aux"] == 1) &
                                              (base_reduzida["Atraso Fornecedor Aux"] == 0) &
                                              (base_reduzida["Atraso Coleta Aux"] == 1), 1, 0)
print('FlagAtraso Coleta', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)


base_reduzida["FlagAtraso CD FF"] = np.where((base_reduzida["Atraso Cliente Aux"] == 1) &
                                             (base_reduzida["Atraso Fornecedor Aux"] == 0) &
                                             (base_reduzida["Atraso Coleta Aux"] == 0) &
                                             (base_reduzida["Atraso CD FF Aux"] == 1), 1, 0)
print('FlagAtraso CD FF', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


base_reduzida["FlagAtraso NextDay"] = np.where((base_reduzida["Atraso Cliente Aux"] == 1) &
                                               (base_reduzida["Atraso Fornecedor Aux"] == 0) &
                                               (base_reduzida["Atraso Coleta Aux"] == 0) &
                                               (base_reduzida["Atraso CD FF Aux"] == 0) &
                                               (base_reduzida["Atraso NextDay Aux"] == 1), 1, 0)
print('FlagAtraso NextDay', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


base_reduzida["FlagAtraso Transporte"] = np.where((base_reduzida["Atraso Cliente Aux"] == 1) &
                                                  (base_reduzida["Atraso Fornecedor Aux"] == 0) &
                                                  (base_reduzida["Atraso Coleta Aux"] == 0) &
                                                  (base_reduzida["Atraso CD FF Aux"] == 0) &
                                                  (base_reduzida["Atraso NextDay Aux"] == 0) &
                                                  ((base_reduzida["Atraso TP + TP FF Aux"] == 1) |
                                                  (base_reduzida["Atraso Transporte Aux"] == 1)), 1, 0)
print('FlagAtraso Transporte', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


base_reduzida["FlagAtraso ErroPrazo"] = np.where((base_reduzida["Atraso Cliente Aux"] == 1) &
                                                 (base_reduzida["Atraso Fornecedor Aux"] == 0) &
                                                 (base_reduzida["Atraso Coleta Aux"] == 0) &
                                                 (base_reduzida["Atraso CD FF Aux"] == 0) &
                                                 (base_reduzida["Atraso NextDay Aux"] == 0) &
                                                 (base_reduzida["Atraso Transporte Aux"] == 0), 1, 0)
print('FlagAtraso ErroPrazo', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


#def faseatraso(a_cliente, a_fornecedor, a_coleta, a_cd_ff, a_next, a_transp):
base_reduzida["Fase Atraso"] = np.vectorize(ColunasCalculadas.faseatraso, otypes=[np.ndarray])(base_reduzida['FlagAtraso Fornecedor'],
                                                                                                        base_reduzida['FlagAtraso Coleta'], base_reduzida['FlagAtraso CD FF'],
                                                                                                        base_reduzida['FlagAtraso NextDay'], base_reduzida['FlagAtraso Transporte'],
                                                                                                        base_reduzida['Reprogramado'])

print('Fase Atraso', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

base_reduzida["Step Reorganizado"] = np.vectorize(ColunasCalculadas.stepReorganizado, otypes=[np.ndarray])(base_reduzida['Status_Manifesto Padrao'], base_reduzida['15. Origem'],
                                                                                                        base_reduzida['42. Data Expedicao'], base_reduzida['53. Data Movimentacao T3'],
                                                                                                        base_reduzida['06. Step da NF/OC'])


print('Step Reorganizado', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["Posicao_Parceiros_Transferencia"] = np.vectorize(ColunasCalculadas.posic_parceiro_transf, otypes=[np.ndarray])(base_reduzida['Posicao Final'], base_reduzida['Flag(Parceiro-Bulky)'],
                                                                                                        base_reduzida['Step Macro'])

print('Posicao_Parceiros_Transferencia', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

base_reduzida["TP que esta com a nota"] = np.vectorize(ColunasCalculadas.tp_nota, otypes=[np.ndarray])(base_reduzida['Fase Nota'], base_reduzida['22. Transp. Redespacho'],
                                                                                                                base_reduzida['Transportadora Redespacho'], base_reduzida['Transportadora Last Mile'],
                                                                                                                base_reduzida['Posicao_Parceiros_Transferencia'], base_reduzida['Transp. LM'],
                                                                                                                base_reduzida['Virada Errada'], base_reduzida['TP Ultimo EDI'])

print('TP que esta com a nota', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Aging SSW
base_reduzida["Aging SSW"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['Data da Ultima Ocorrencia - SSW 455'], base_reduzida['Dia'])

print('Aging SSW', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Aging Transferencia LM
base_reduzida["Aging Transferencia LM"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['54. Data CTRC Emitido T3'], base_reduzida['56. Data Chegada Destino T3'])

print('Aging Transferencia LM', " - %s" % (time.time() - start_time), " - segundos" ,"- %s" % (d), file=arquivo)

#Aging CTRCxSaida origem
base_reduzida["Aging CTRCxSaida origem"] = np.vectorize(ColunasCalculadas.diaUtil_entredatas, otypes=[np.ndarray])(base_reduzida['54. Data CTRC Emitido T3'], base_reduzida['55. Data Saida Origem T3'])

print('Aging CTRCxSaida origem', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#def deltaTransf(agingCTRC, agingTransf):
base_reduzida["Delta transferencia"] = np.vectorize(ColunasCalculadas.deltaTransf, otypes=[np.ndarray])(base_reduzida['Aging CTRCxSaida origem'], base_reduzida['Aging Transferencia LM'])
print('Delta transferencia', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#def novaDataLimiteTransp(tp22,flagatrasoredespacho, d28, d_redespachoCTE, prazoLM):

base_reduzida["Nova Data Limite Transporte"] = base_reduzida.apply(lambda row: ColunasCalculadas.novaDataLimiteTransp(row["22. Transp. Redespacho"],
                                                                                                                      row["Flag_Atraso_Redespacho"],
                                                                                                                      row["28. Data Limite Transporte"],
                                                                                                                      row["Data_Redespacho OU Cte_Emitido"],
                                                                                                                      row["Prazo Last Mile"]),
                                                                                                                      axis=1)
print('Nova Data Limite Transporte', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


#Novo Status Transporte
base_reduzida["Novo Status Transporte"] = np.vectorize(ColunasCalculadas.statusVencimento, otypes=[np.ndarray])(base_reduzida['Dia'], base_reduzida['Nova Data Limite Transporte'])

print("Novo Status Transporte", " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)


#Proc com Cluster_TP
#types = {'Transp. LM': "string", 'Tipo_TP': "string"}
cluster_tp_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Cluster_TP.csv", sep=';')
cluster_tp_df = ColunasCalculadas.ajustaLetras(cluster_tp_df)
cluster_tp_df = cluster_tp_df.convert_dtypes() #ajustando os tipos da colunas
#base_df = pd.merge(base_df, cluster_tp_df, on='Transp. LM', how='left')
base_reduzida = pd.merge(base_reduzida, cluster_tp_df, on='Transp. LM', how='left')
print(cluster_tp_df)

print("Cluster TP", " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Proc com o SAC
SAC_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\SAC.csv", sep=';')
SAC_df = ColunasCalculadas.ajustaLetras(SAC_df)
SAC_df = SAC_df.convert_dtypes() #ajustando os tipos da colunas
base_df = pd.merge(base_df, SAC_df, on='Chave Pedido e Nota', how='left')
SAC_df = pd.DataFrame()

print("SAC", " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

print("--- %s seconds DEPOIS DOS PROCS---" % (time.time() - start_time))
print(datetime.today().strftime('%H:%M'))
print('base depois dos procs')
cluster_tp_df= pd.DataFrame()

print("--- %s seconds ---" % (time.time() - start_time),"- %s" % (d), file=arquivo)
# #######################################################################################################################
# #                             O S   C A L C U L O S   A C A B A R A M   A Q U I
# #######################################################################################################################
col_depois = base_reduzida.shape[1]
print(col_anterior, "col antes", col_depois, "col depois")

print(datetime.today().strftime('%H:%M'))
print("--- %s seconds ---" % (time.time() - start_time),"- %s" % (d), file=arquivo)

inicio = int(col_anterior) - 1

#move chave pedido e nota pra ultima posição
cols = base_reduzida.columns.tolist()
column_to_move = "Chave Pedido e Nota"
new_position = col_depois
cols.insert(new_position, cols.pop(cols.index(column_to_move)))
base_reduzida = base_reduzida[cols]

print("move chave pedido e nota pra ultima posição", " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

list = []
for i in range(0, inicio):
    list.append(i)

base_reduzida = base_reduzida.drop(base_reduzida.columns[list], axis=1)
print(base_reduzida.head())

base_df2 = pd.merge(base_df, base_reduzida, on='Chave Pedido e Nota', how='left')
print("--- %s seconds colocando os calculos no base_df2---" % (time.time() - start_time),"- %s" % (d), file=arquivo)
base_df= pd.DataFrame()
base_reduzida= pd.DataFrame()

#remove duplicatas
base_df2 = base_df2.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)
print('base df 2')
base_df2 = base_df2.convert_dtypes() #ajustando os tipos da colunas

print("drop duplicadas", " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)
#######################################################################################################################
#                                                   VENDA DE SERVIÇO
#######################################################################################################################


vendaservico_df = pd.read_csv(r"C:\Users\eliana.rodrigues\Downloads\Venda_Servico.csv", sep=';')

h = datetime.today().strftime('%H')
num_colunas = vendaservico_df.shape[1]
#vendaservico_df.insert(loc=num_colunas, column='Dia (D)', value='D_0 - '+h+'h')

vendaservico_df.insert(loc=0, column='Arquivo', value="VS")
vendaservico_df = vendaservico_df.convert_dtypes()
print('vendaservico_df', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

# #######################################################################################################################
# #                                                   VENDA DE SERVIÇO
# #######################################################################################################################


#Concatenando as notas do tableau com as informações do WMS
base_df3 = pd.concat([base_df2, vendaservico_df], ignore_index=True)
print(base_df.shape[0], "linhas")

print('concatena base reduzida e venda de serviço', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

xls = pd.ExcelFile(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\0.Criterios_Diario.xlsx", engine='openpyxl')

transp_df = pd.read_excel(xls, sheet_name="Transportadora")
transp_df = transp_df.convert_dtypes() #ajustando os tipos da colunas
marketplace_df = pd.read_excel(xls, sheet_name="MarketPlace")
marketplace_df = marketplace_df.convert_dtypes() #ajustando os tipos da colunas
edi_criticas_df = pd.read_excel(xls, sheet_name="EDI_e_Criticas")
edi_criticas_df = edi_criticas_df.convert_dtypes() #ajustando os tipos da colunas
edi_df = edi_criticas_df
edi_df = edi_df.drop(columns=['Chave Pedido e Nota', 'Observacao Nota Prioritaria'])
edi_df.dropna(subset=['DExPARA EDI'], inplace=True)

criticas_df = edi_criticas_df
criticas_df = criticas_df.drop(columns=['DExPARA EDI'])
criticas_df.dropna(subset=['Chave Pedido e Nota'], inplace=True)

edi_df.insert(loc=1, column='Diario EDI', value="X")
criticas_df.insert(loc=1, column='Diario NotaCritica', value="X")

base_df3 = pd.merge(base_df3, transp_df, on='TP que esta com a nota', how='left')
base_df3 = base_df3.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)

base_df3 = pd.merge(base_df3, marketplace_df, on='18. Market Place', how='left')
base_df3 = base_df3.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)

base_df3 = pd.merge(base_df3, edi_df, on='DExPARA EDI', how='left')
base_df3 = base_df3.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)

base_df3 = pd.merge(base_df3, criticas_df, on='Chave Pedido e Nota', how='left')
base_df3 = base_df3.drop_duplicates(subset=['Chave Pedido e Nota'], keep='first', ignore_index=True)

print('criterios diario', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#colocando brancos
for y in base_df3.columns:
    if (is_string_dtype(base_df3[y])):
        base_df3[y].fillna(str(blank), inplace=True)
    elif (is_numeric_dtype(base_df3[y])):
        base_df3[y].fillna(blank, inplace=True)

base_df3["motivo diario"], base_df3["Diario de Bordo"] = zip(*base_df3.apply(lambda row:
                                                                             ColunasCalculadas.diario_flag(
                                                                                 row["Arquivo"],
                                                                                 row["Flag Atraso Vencimento T3"],
                                                                                 row["Flag Atraso Cliente"],
                                                                                 row["Flag Atraso Transporte"],
                                                                                 row["Flag_Atraso_Redespacho"],
                                                                                 row["Aging Vencimento T3"],
                                                                                 row["Aging Cliente"],
                                                                                 row["Aging Transporte"],
                                                                                 row["Aging Posicao Final"], row["cont_stats"],
                                                                                 row["Cont_Stats_Consolidador"],
                                                                                 row["Cont_Status_Parceiros"],
                                                                                 row["Cont_Status_Transferencia"],
                                                                                 row["Cont_Status_Coleta"],
                                                                                 row["Diario Venc. MM"],
                                                                                 row["Diario Venc. Cliente"],
                                                                                 row["Diario Venc. Transporte"],
                                                                                 row["Diario Ag. Movimentacao"],
                                                                                 row["Diario Venc. Redespacho"],
                                                                                 row["Diario Venc. Chao T2"],
                                                                                 row["Diario Venc. Parceiro"], row[
                                                                                     "Diario Venc. Transferencia T2"],
                                                                                 row["Diario Venc. Coleta T2"],
                                                                                 row["Venc. Market Place"],
                                                                                 row["Diario EDI"],
                                                                                 row["Diario NotaCritica"],
                                                                                 row["Fase Nota"])
                                                                             , axis=1))
print('motivo diario', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

################################################################################################
base_df3 = base_df3.replace(blank, np.nan, regex=True)
base_df3 = base_df3.replace(str(blank), '', regex=True)
print(base_df3.head())


list = ["SKU", "Pedido_n"]
base_df3 = base_df3.drop(columns=list, axis=1)

ordenacao_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\zzz_Ordem - Copia.csv", sep=';')
num_linhas = ordenacao_df.shape[0]
list = []
for i in range(0, num_linhas):
    list.append(ordenacao_df.values[i, 0])

print('zzz ordem', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

h = datetime.today().strftime('%H')
num_colunas = base_df3.shape[1]
base_df3.insert(loc=num_colunas, column='Dia (D)', value='D_0 - '+h+'h')


base_df3 = base_df3[list]
base_df3 = base_df3.drop(columns=['Transportadora Last Mile', 'Transportadora_LM V2', 'idfk_status'])

## -- filtra a coluna arquivo = 'wms' e limpa dados = 'entregue - ssw' e 'cancelado wms'

base_df3 = base_df3.drop(base_df3.loc[(base_df3['Arquivo'] == "WMS") & (base_df3['Fase Nota'] == "ENTREGUE - SSW")].index)
base_df3 = base_df3.drop(base_df3.loc[(base_df3['Arquivo'] == "WMS") & (base_df3['Fase Nota'] == "CANCELADO - WMS")].index)

print('dropa entregue/cancelado wms', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#writer = pd.ExcelWriter('novos_atrasos/Atrasos.xlsx'.format(pd.datetime.today().strftime('%Y%m%d-%H%M')))

############################## NOVOS ATRASOS ##########################

# novos_atrasos = open('novos_atrasos/novos atrasos_{}.txt'.format(pd.datetime.today().strftime('%Y%m%d-%H%M')))
# novos_atrasos = base_df3.loc[(base_df3['Aging Transporte'] == 1)]
# print(novos_atrasos, file=novos_atrasos)
# novos_atrasos.close()

##cria arquivo D0
partialFileName = "D_0"
endereco = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\02. BasesDia'
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

#Coloca hora no nome do arquivo.
#base_df2.to_csv('C:/Users/eliana.rodrigues/Downloads/D_0.csv', sep=';', index=False)

base_df3.to_csv('C:/Users/eliana.rodrigues/Downloads/'+nome, sep=';', index=False)
print("Arquivo Criado")

print('cria arquivo D0', " - %s" % (time.time() - start_time), " - segundos","- %s" % (d), file=arquivo)

#Copia arquivo para a rede

fileDir = r"C:\Users\eliana.rodrigues\Downloads"
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\02. BasesDia'

old_file_path = os.path.join(fileDir, nome)
new_file_path = os.path.join(pasta_nova, nome)
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")

print("--- %s seconds TOTAL ---" % (time.time() - start_time),"- %s" % (d), file=arquivo)
print(datetime.today().strftime('%H:%M'))

arquivo.close()
