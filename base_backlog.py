import pandas as pd
import time
from datetime import datetime
from workalendar.america import BrazilBankCalendar
import numpy as np
import modulos_consultas
import ColunasCalculadas
import subprocess
import shutil
import os

print(datetime.today().strftime('%H:%M'))
start_time = time.time()
cal = BrazilBankCalendar()
cal.include_ash_wednesday = False                       #tirar a quarta de cinzas, pra gente é dia normal
cal.include_christmas = True                            # considera natal como feriado
cal.include_christmas_eve = True                        # considera natal como feriado
cal.include_new_years_day = True                        # considera ano novo como feriado
cal.include_new_years_eve = False                        # considera ano novo como feriado
blank = 12349999


df_backlog = modulos_consultas.backlog()
df_prazo = modulos_consultas.prazo()
giro = df_backlog.merge(df_prazo, how = 'left', left_on = ['chaveorigem']
                                  , right_on =  ['chaveorigem'])

print(giro)
giro.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Giro_Pedidos.csv', sep=';')

def stepdanf(dataexpedicao, tipotransporte, datamovimentacao, dataredespacho, idorigem, oc, databip, datafaturamento, dataceitenf, qtdconfirmada, cross):

    if pd.notnull(dataexpedicao) and tipotransporte == "Direto":
        return 4
    elif pd.notnull(dataexpedicao) and (tipotransporte == "Redespacho" and pd.notnull(datamovimentacao)):
        return 4
    elif pd.notnull(dataexpedicao) and (tipotransporte == "Redespacho" and pd.notnull(dataredespacho)):
        return 4
    elif (idorigem == 1 or idorigem == 16 or idorigem == 20) and pd.notnull(dataexpedicao):
        return 4
    elif pd.notnull(dataexpedicao) and tipotransporte == "Redespacho" and (pd.isnull(dataredespacho) and pd.isnull(datamovimentacao)):
        return 3
    elif pd.notnull(oc) and pd.isnull(databip):
        return 1
    elif (idorigem == 1 or idorigem == 16 or idorigem == 20) and (pd.notnull(datafaturamento) and pd.isnull(dataceitenf) and pd.isnull(dataexpedicao)):
        return 5
    elif pd.isnull(databip) and (idorigem == 1 or idorigem == 16 or idorigem == 20) and pd.isnull(dataexpedicao):
        return 5
    elif pd.notnull(databip) and pd.isnull(dataexpedicao) and qtdconfirmada == 0:
        return 1
    elif pd.notnull(databip) and pd.isnull(dataexpedicao):
        return 2
    elif pd.notnull(databip) and (idorigem == 1 or idorigem == 16 or idorigem == 20) and pd.isnull(dataexpedicao):
        return 2
    elif cross == "ESTOQUE MADEIRA":
        return 5
    elif (idorigem == 1 or idorigem == 16 or idorigem == 20) and( pd.isnull(datafaturamento) and pd.isnull(databip)):
        return 1
    elif (pd.isnull(databip) and pd.isnull(dataexpedicao)):
        return 1
    else:
        return 5

def prazo_viagem(prazo1, prazo2):
    if prazo1 == blank:
        prazoviagem = prazo1
    elif prazo2 == blank:
        prazoviagem = prazo2
    else:
        prazoviagem = prazo1
        return prazoviagem

def prazo_chao(proazo0, prazo1, prazo2):
    if pd.notna(proazo0):
        return proazo0
    else:
        return prazo1 - prazo2

def prazo_redespacho(prazo1, prazo2):
    if pd.isna(prazo1) or prazo1 == blank:
        return prazo2
    else:
        return prazo1

# #######################################################################################################################
# #                                                   LÊ CSV BASE backlog
# #######################################################################################################################

base_backlog = pd.read_csv(r"C:\Users\eliana.rodrigues\Downloads\Giro_Pedidos.csv", sep=';')


base_backlog = base_backlog.convert_dtypes()
base_backlog = base_backlog.convert_dtypes()

base_backlog.rename(columns = {'01. nf madeira' : '01. NF Madeira',
'pedido' : '02. Pedido',
'03. oc' : '03. OC',
'04. nf fornecedor' : '04. NF Fornecedor',
'05. fornecedor' : '05. Fornecedor',
'07. codigo sku' : '07. Codigo SKU',
'08. valor frete' : '08. Valor Frete',
'09. valor total nf' : '09. Valor Total NF',
'10. peso nf (kg)' : '10. Peso NF (kg)',
'11. qntd. volumes nf' : '11. Qntd. Volumes NF',
'12. nome cliente' : '12. Nome Cliente',
'13. telefone cliente' : '13. Telefone Cliente',
'14. cnpj fornecedor' : '14. CNPJ Fornecedor',
'15. origem' : '15. Origem',
'16. destino' : '16. Destino',
'17. cep' : '17. CEP',
'18. market place' : '18. Market Place',
'19. analista t2' : '19. Analista T2',
'20. analista t3' : '20. Analista T3',
'21. transp. redespacho nf' : '21. Transp. Redespacho NF',
'22. transp. redespacho' : '22. Transp. Redespacho',
'23. transp. entrega nf' : '23. Transp. Entrega NF',
'24. transp. entrega' : '24. Transp. Entrega',
'25. data limite cliente' : '25. Data Limite Cliente',
'26. data limite fornecedor' : '26. Data Limite Fornecedor',
'27. data limite coleta' : '27. Data Limite Coleta',
'28. data limite transporte' : '28. Data Limite Transporte',
'29. data limite redespacho' : '29. Data Limite Redespacho',
'30. prazo redespacho' : '30. Prazo Redespacho',
'32. prazo last mile' : '32. Prazo Last Mile',
'37. data compra cliente' : '37. Data Compra Cliente',
'38. data criacao oc' : '38. Data Criacao OC',
'39. data aceite oc' : '39. Data Aceite OC',
'40. data faturamento nf mi' : '40. Data Faturamento NF MI',
'41. data bip' : '41. Data BIP',
'42. data expedicao' : '42. Data Expedicao',
'43. transp. ultimo edi' : '43. Transp. Ultimo EDI',
'44. ultimo edi' : '44. Ultimo EDI',
'45.observacao ultimo edi' : '45.Observacao Ultimo EDI',
'46. data ultimo edi' : '46. Data Ultimo EDI',
'48. data ctrc emitido t2' : '48. Data CTRC Emitido T2',
'49. data saida origem t2' : '49. Data Saida Origem T2',
'50. data chegada destino t2' : '50. Data Chegada Destino T2',
'51. data em rota de entrega t2' : '51. Data Em Rota de Entrega T2',
'52. data redespacho' : '52. Data Redespacho',
'53. data movimentacao t3' : '53. Data Movimentacao T3',
'54. data ctrc emitido t3' : '54. Data CTRC Emitido T3',
'55. data saida origem t3' : '55. Data Saida Origem T3',
'56. data chegada destino t3' : '56. Data Chegada Destino T3',
'57. data em rota de entrega t3' : '57. Data Em Rota de Entrega T3',
'58. data entrega' : '58. Data Entrega',
'63. tipo avaria' : '63. Tipo Avaria',
'64. data apontamento avaria' : '64. Data Apontamento Avaria',
'66. data solicitacao devolucao' : '66. Data Solicitacao Devolucao',
'67. data analise devolucao' : '67. Data Analise Devolucao',
'68. data solicitacao contato' : '68. Data Solicitacao Contato',
'70. data retorno contato' : '70. Data Retorno Contato',
'72. qual o ultimo sac aberto?' : '72. Qual o Ultimo SAC aberto?',
'74. status retorno contato' : '74. Status Retorno Contato',
'75. danfe_nf_fornecedor' : '75. Danfe_Nf_Fornecedor',
'76. danfe_nf_mi' : '76. Danfe_Nf_Mi',
'77. data_criacao_pv' : '77. Data_Criacao_Pv',
'78. data_revisada_cliente' : '78. Data_Revisada_Cliente',
'79. market_place_grupo' : '79. Market_Place_Grupo',
'81. pedido em guideshop' : '81. Pedido Em Guideshop',
'82. prazo_fornecedor' : '82. Prazo_Fornecedor',
'83. status_eagle' : '83. Status_Eagle',
'84. status_portal' : '84. Status_Portal',
'87. uf destino' : '87. UF Destino',
'88. cidade' : '88. Cidade',
'tipo de transporte' : '86. Tipo De Transporte',
'tipo_venda' : "89. Tipo Venda",
'nome_reduzido' : "Nome Parceiro",
'unidade_codigo': 'Codigo Rota',
'cabecasa' : 'Cabecasa',
'guide' : 'Guide',
'assistencia' : 'Assistencia',
'forma_pagamento' : 'Forma de Pagamento',
'prazo_redespacho' : 'prazo_redespacho_query',
'categoria' : 'Categoria'
}, inplace = True)

base_backlog['75. Danfe_Nf_Fornecedor'] = base_backlog['75. Danfe_Nf_Fornecedor'].astype(str)
base_backlog['76. Danfe_Nf_Mi'] = base_backlog['76. Danfe_Nf_Mi'].astype(str)


lista = ["25. Data Limite Cliente", "26. Data Limite Fornecedor", "27. Data Limite Coleta",
         "28. Data Limite Transporte", "29. Data Limite Redespacho",
         "37. Data Compra Cliente", "38. Data Criacao OC", "39. Data Aceite OC", "40. Data Faturamento NF MI",
         "41. Data BIP", "42. Data Expedicao", "46. Data Ultimo EDI", "48. Data CTRC Emitido T2",
         "49. Data Saida Origem T2", "50. Data Chegada Destino T2", "51. Data Em Rota de Entrega T2",
         "52. Data Redespacho", "53. Data Movimentacao T3", "54. Data CTRC Emitido T3", "55. Data Saida Origem T3",
         "56. Data Chegada Destino T3", "57. Data Em Rota de Entrega T3", "58. Data Entrega",
         "64. Data Apontamento Avaria", "66. Data Solicitacao Devolucao", "67. Data Analise Devolucao",
         "68. Data Solicitacao Contato", "70. Data Retorno Contato", "77. Data_Criacao_Pv", "78. Data_Revisada_Cliente"]


print("lista", " - %s" % (time.time() - start_time), " - segundos" )


for d in lista:
    #pd.to_datetime(df['Dates'], format='%y%m%d')
    base_backlog[d] = pd.to_datetime(base_backlog[d], infer_datetime_format=True, dayfirst=True)
print("converteu lista em data", " - %s" % (time.time() - start_time), " - segundos" )


#CRIANDO COLUNA STEP DA NF OC
base_backlog["06. Step da NF/OC"] = base_backlog.apply(lambda row: stepdanf(row["42. Data Expedicao"],
                                                                              row["86. Tipo De Transporte"],
                                                                              row["53. Data Movimentacao T3"],
                                                                              row["52. Data Redespacho"],
                                                                              row["id_origem"],
                                                                              row["03. OC"],
                                                                              row["41. Data BIP"],
                                                                              row["40. Data Faturamento NF MI"],
                                                                              row["data_aceite_nf_cd"],
                                                                              row["quantidade_confirmada_oc"],
                                                                              row['cross/entrega/vao']), axis=1)

base_backlog["58. Data Entrega"] = base_backlog["58. Data Entrega"].astype(str) #ajustando o tipo da coluna
base_backlog["67. Data Analise Devolucao"] = base_backlog["67. Data Analise Devolucao"].astype(str) #ajustando o tipo da coluna
base_backlog["17. CEP"] = base_backlog["17. CEP"].astype(str) #ajustando o tipo da coluna
base_backlog = base_backlog.convert_dtypes() #ajustando os tipos da colunas
base_backlog = base_backlog.convert_dtypes() #ajustando os tipos da colunas
base_backlog["17. CEP"] = base_backlog["17. CEP"].str.zfill(8) #Preenche a coluna com 8 digitos (zero a esquerda)

#base_backlog["14. CNPJ Fornecedor"] = base_backlog["14. CNPJ Fornecedor"].astype('Int64') #ajustando o tipo da coluna
print(base_backlog.shape[0], " linhas tableau") #printa o numero de linhas do tableau


base_backlog['04. NF Fornecedor'] = base_backlog['04. NF Fornecedor'].fillna(blank) #preenchendo os espaços vazios com 12349999
base_backlog['01. NF Madeira'] = base_backlog['01. NF Madeira'].fillna(blank) #preenchendo os espaços vazios com 12349999
print("Concat Tableau", " - %s" % (time.time() - start_time), " - segundos" )


#criando coluna Chave pedido e nota
num_colunas = base_backlog.shape[1] #numero de colunas do tableau
base_backlog.insert(loc=0, column='Arquivo', value="Tableau") #Coloca na 1a coluna o valor "Tableau"
base_backlog.insert(loc=num_colunas, column='Chave Pedido e Nota', value=base_backlog['02. Pedido'].astype(str) + base_backlog['01. NF Madeira'].astype(str)) #Insere na ultima coluna a chave pedido e nota
print("Chave Pedido e Nf", " - %s" % (time.time() - start_time), " - segundos" )


# a função groupby junta as mesmas "chave pedido e nota" e soma quantas vezes aparece com o .cumcount
#coloca numero se nao tiver nota e tiver mais de 1 pedido "Z1234567-1"
base_backlog['Chave Pedido e Nota'] += '-' + base_backlog.groupby(['Chave Pedido e Nota']).cumcount().astype(str)
base_backlog['Chave Pedido e Nota'] = base_backlog['Chave Pedido e Nota'].replace('-0', '', regex=True)
print("Coloca Numero", " - %s" % (time.time() - start_time), " - segundos" )

base_backlog = ColunasCalculadas.ajustaLetras(base_backlog)

################## COMEÇA CALCULOS DE PRAZO

#chave para proc Prazo Atualizado (Aux_Prazo_Chão)
num_colunas = base_backlog.shape[1]
base_backlog.insert(loc=num_colunas, column='destino_origem', value=(base_backlog['16. Destino'].astype(str) + base_backlog['15. Origem'].astype(str)))
print('cria primeira chave destino-origem - prazos t2')

#chave para proc Prazo Atualizado (Prazo_Viagem_T2) -- Retorna o prazo de acordo com a chave de transportadora nota  e origem
num_colunas = base_backlog.shape[1]
base_backlog.insert(loc=num_colunas, column='transp_origem', value=(base_backlog['23. Transp. Entrega NF'].astype(str) + base_backlog['15. Origem'].astype(str)))
print('cria segunda chave transp-origem - prazos t2')

#merge com prazo vigem - t2
list = ['transp_origem', 'Aux_Prazo_Viagem']
prazo_atualizado_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Prazo_Viagem.csv", sep=';',  usecols=list)
prazo_atualizado_df.rename(columns={'Aux_Prazo_Viagem': 'Aux_Prazo_Viagem - transp_origem'}, inplace = True)
prazo_atualizado_df = ColunasCalculadas.ajustaLetras(prazo_atualizado_df)
base_backlog = pd.merge(base_backlog, prazo_atualizado_df, on='transp_origem', how='left')
prazo_atualizado_df = pd.DataFrame()
print('merge prazos destino origem - ok')

#merge com prazo vigem - t2 - apepnas para buscar o prazo viagem do FF
list = ['transp_origem', 'Aux_Prazo_Chao']
prazo_chao_ff = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Prazo_Atualizado.csv", sep=';',  usecols=list)
prazo_chao_ff.rename(columns={'Aux_Prazo_Chao': 'Aux_Prazo_Chao - FF'}, inplace = True)
prazo_chao_ff = ColunasCalculadas.ajustaLetras(prazo_chao_ff)
base_backlog = pd.merge(base_backlog, prazo_chao_ff, on='transp_origem', how='left')
prazo_chao_ff = pd.DataFrame()

#Prazo_Redespacho
base_backlog['Prazo Redespacho'] = np.vectorize(prazo_redespacho, otypes=[np.ndarray])(base_backlog['prazo_redespacho_query'],base_backlog['30. Prazo Redespacho'])
base_backlog['Prazo Redespacho'] = base_backlog['Prazo Redespacho'].astype('Float32').astype('Int32')

base_backlog['Aux_Prazo_Viagem - DestinoOrigem'] = blank

#Prazo_Viagem_T2
base_backlog['Prazo_Viagem_T2'] = base_backlog['Aux_Prazo_Viagem - transp_origem']
base_backlog['Prazo_Viagem_T2'] = base_backlog['Prazo_Viagem_T2'].astype('Float32').astype('Int32')

#Prazo Redespacho
base_backlog["Prazo Chao T2"] = np.vectorize(prazo_chao, otypes=[np.ndarray])(base_backlog['Aux_Prazo_Chao - FF'], base_backlog['Prazo Redespacho'],base_backlog['Prazo_Viagem_T2'])


#criar Chave Pedido e SKU
base_backlog.insert(loc=num_colunas, column='Chave Pedido e SKU', value=base_backlog['02. Pedido'].astype(str) + base_backlog['07. Codigo SKU'].astype(str)) #Insere na ultima coluna a chave pedido e nota
base_backlog['Aux_Prazo_Viagem - transp_origem'] = base_backlog['Aux_Prazo_Viagem - transp_origem'].astype('Float32').astype('Int32')
base_backlog['Prazo Chao T2'] = base_backlog['Prazo Chao T2'].astype('Float32').astype('Int32')


#remove duplicadas da Chave Pedido e SKU
base_backlog.drop_duplicates('Chave Pedido e SKU', inplace=True)

#dropa coluna Chave Pedido e SKU
base_backlog.drop(columns='Chave Pedido e SKU', inplace=True)

base_backlog = base_backlog.drop(columns=['codigo_reserva', 'idfk_pedido_compra', 'idfk_armazem', 'origem_reserva', 'chaveorigem', 'prazo_redespacho_query', 'datetime_cadastro', 'Aux_Prazo_Chao - FF'])
print('dropa colunas')

print(base_backlog['Prazo Redespacho'])

base_backlog.to_csv(r'C:\Users\eliana.rodrigues\Downloads\Giro_Total.csv', sep=';', index=False)

base_backlog.to_excel(
    r'C:\Users\eliana.rodrigues\Downloads\Giro_Total.xlsx',
    index=False)

endereco = r'C:\Users\eliana.rodrigues\Downloads'


#Copiando o arquivo para a rede
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads'
old_file_path = os.path.join(endereco, "Giro_Total.csv")
new_file_path = os.path.join(pasta_nova, "Giro_Total.csv")
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")

subprocess.call("Dados_CX.py", shell=True)

print(datetime.today().strftime('%H:%M'))

time.sleep(3)