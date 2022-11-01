import subprocess
import time
import os
from selenium import webdriver
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import datetime


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Realiza o download do WMS Pernambuco
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


# #Fazer download do chromedriver compativel com o seu Chrome e depois colocar o endereço do executável
browser = webdriver.Chrome(executable_path="C:\Python\chromedriver.exe")
browser.get("http://54.233.220.111:8080/siltwms/#tarefasprincipais")
browser.maximize_window()
time.sleep(3)

#encontrando na pagina HTMl local de usuario e senha.
usuario = browser.find_element_by_id("LoginDialog_loginText")
senha = browser.find_element_by_id("LoginDialog_passwordText")
armazem = browser.find_element_by_id("LoginDialog_armazemComboBox")
botao_login = browser.find_element_by_id('LoginDialog_loginButton')

#colocando as credenciais.
usuario.send_keys("AMARTINS")
senha.send_keys("Ketlyn01")
armazem.click()
time.sleep(2)
pernambuco = browser.find_element_by_xpath("//div[@id='LoginDialog_armazemComboBox-BULKY LOG CABO (PE)']")
pernambuco.click()
time.sleep(1)
botao_login.click()
time.sleep(3)

#clica em acompanhamento de notas
#acompanhamento = browser.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div[1]/div/div[2]/div/div/div/div[3]/div[2]')
acompanhamento = browser.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div[1]/div/div[2]/div/div/div/div[2]/div[1]')
acompanhamento.click()
time.sleep(2)

#Exibir "TODOS"
exibir = browser.find_element_by_xpath(
    '/html/body/div[1]/div[3]/div[2]/div[1]/div/div[2]/div[2]/div/div/div[1]/div/div/div/div[2]/div[1]/div/table/tbody/tr/td[1]/table/tbody/tr/td[1]/table/tbody/tr[2]/td[2]/em/button')
exibir.click()
time.sleep(2)
todas = browser.find_element_by_xpath('/html/body/div[12]/div/div[6]/a')
todas.click()

#Periodo 30 dias
time.sleep(10)
periodo = browser.find_element_by_xpath(
    '/html/body/div[1]/div[3]/div[2]/div[1]/div/div[2]/div[2]/div/div/div[1]/div/div/div/div[2]/div[1]/div/table/tbody/tr/td[1]/table/tbody/tr/td[2]/table/tbody/tr[2]/td[2]/em/button')
periodo.click()
time.sleep(2)
dias30 = browser.find_element_by_xpath('/html/body/div[12]/div/div[5]/a')
dias30.click()

time.sleep(17)
print("1 minuto")
#clica em exportar para CSV
exportar = browser.find_element_by_xpath('//*[@id="ExportGridPlugin_exportButton"]')
exportar.click()
time.sleep(2)
export_csv = browser.find_element_by_xpath('/html/body/div[12]/div/div[2]/a')
export_csv.click()
print("cliquei")

#Busca na pasta se o arquivo foi baixado
partialFileName = "Exportacao_"
fileDir = r"C:\Users\eliana.rodrigues\Downloads"
fileExt = r".csv"
newlist = []
cont = 0
while len(newlist) == 0:
    time.sleep(5)
    items = os.listdir(fileDir)
    for names in items:
        if names.startswith(partialFileName):
            if names.endswith(fileExt):
                newlist.append(names)
    cont = cont + 1
    print(cont, newlist)
time.sleep(5)
browser.close()

#Renomeia arquivo
wms_pe = str(newlist[0])
old_file = os.path.join(fileDir, wms_pe)
new_file = os.path.join(fileDir, "WMS_pe.csv")
os.rename(old_file, new_file)
print("Arquivo Renomeado")

#pegando as colunas escolhidas do wms
colunas_wms_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Colunas_WMS.txt", sep='\t')
num_linhas = colunas_wms_df.shape[0]
list = []
for i in range(0, num_linhas):
    list.append(colunas_wms_df.values[i, 0])

#Ajustando as colunas do arquivo
wms_df = pd.read_csv('C:/Users/eliana.rodrigues/Downloads/WMS_pe.csv', sep=',', usecols=list)
print(str(wms_df.shape[0]) +" linhas")
wms_df = wms_df[list]
wms_df.rename(columns = {'Nota Fiscal': '01. NF Madeira',
                         'Pedido de Venda': '02. Pedido',
                         'Status da Nota Fiscal': '06. Step da NF/OC',
                         'Valor Total da NF': '09. Valor Total NF',
                         'Peso dos Volumes': '10. Peso NF (kg)',
                         'Qtde. de Volumes': '11. Qntd. Volumes NF',
                         'Entrega': '12. Nome Cliente',
                         'Transportadora': '24. Transp. Entrega',
                         'Data Importação / Cadastro': '37. Data Compra Cliente',
                         'Processado em': '42. Data Expedicao',
                         'UF Destinatário': '87. UF Destino',
                         'Classificação Tipo Pedido': 'Classificacao Tipo Pedido',
                         'Título Romaneio' : 'Rota'
                         } , inplace = True)
wms_df.insert(loc=7, column = '15. Origem', value = 'CD PE Fulfillment')

wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('BULKY LOG - ARUJA', 'ARUJA - BULKY', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('BULKY LOG - BETIM', 'BETIM - BULKY L', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('BULKY LOG - ITAJAI', 'ITAJAI - BULKYL', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('BULKY LOG - JUNDIAI', 'BULKY LOG - JUN', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('BULKY LOG - PINHAIS', 'BULKY - PINHAIS', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('BULKY LOG - RIO DE JANEIRO', 'RIO - BULKYLOG', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('BULKY LOG TRANSPORTES LTDA', 'FULFILLMENT - B', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('FAVORITA TRANSPORTES LTDA', 'FAVORITA', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('FCB BRASIL TRANSPORTES', 'FCB BRASIL', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('FULFILLMENT - BULKY LOG', 'FULFILLMENT - B', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('JADLOG LOGISTICA LTDA', 'JADLOG', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('PR DLOG', 'PR DLOG', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('RS - DOMINALOG', 'RS - DOMINALOG', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('SP - DOMINALOG', 'SP - DOMINALOG', regex=True)
wms_df['24. Transp. Entrega'] = wms_df['24. Transp. Entrega'].replace('TLOG RJ', 'TLOG RJ', regex=True)

#trocando os acentos
wms_df = wms_df.replace('á', 'a', regex=True)
wms_df = wms_df.replace('Á', 'A', regex=True)
wms_df = wms_df.replace('ã', 'a', regex=True)
wms_df = wms_df.replace('Ã', 'A', regex=True)
wms_df = wms_df.replace('â', 'a', regex=True)
wms_df = wms_df.replace('Â', 'A', regex=True)
wms_df = wms_df.replace('à', 'a', regex=True)
wms_df = wms_df.replace('À', 'A', regex=True)
wms_df = wms_df.replace('é', 'e', regex=True)
wms_df = wms_df.replace('ê', 'e', regex=True)
wms_df = wms_df.replace('É', 'E', regex=True)
wms_df = wms_df.replace('Ê', 'E', regex=True)
wms_df = wms_df.replace('í', 'i', regex=True)
wms_df = wms_df.replace('î', 'i', regex=True)
wms_df = wms_df.replace('Í', 'i', regex=True)
wms_df = wms_df.replace('Î', 'i', regex=True)
wms_df = wms_df.replace('Ó', 'O', regex=True)
wms_df = wms_df.replace('Ô', 'O', regex=True)
wms_df = wms_df.replace('Õ', 'O', regex=True)
wms_df = wms_df.replace('ó', 'o', regex=True)
wms_df = wms_df.replace('ô', 'o', regex=True)
wms_df = wms_df.replace('õ', 'o', regex=True)
wms_df = wms_df.replace('û', 'u', regex=True)
wms_df = wms_df.replace('ú', 'u', regex=True)
wms_df = wms_df.replace('Ú', 'U', regex=True)
wms_df = wms_df.replace('Û', 'U', regex=True)
wms_df = wms_df.replace('ç', 'c', regex=True)
wms_df = wms_df.replace('Ç', 'C', regex=True)
wms_df = wms_df.replace('ª', 'a', regex=True)
wms_df = wms_df.replace('º', 'o', regex=True)



print(str(wms_df.shape[0]) +" linhas")
wms_df.to_csv('C:/Users/eliana.rodrigues/Downloads/WMS_pe.csv', sep=';', index=False)

#Copia para a rede
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads'
old_file_path = os.path.join(fileDir, "WMS_pe.csv")
new_file_path = os.path.join(pasta_nova, "WMS_pe.csv")
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")


print("Concluído")