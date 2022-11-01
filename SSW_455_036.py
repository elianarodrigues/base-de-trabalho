import time
import pyautogui
from datetime import datetime, timedelta
import shutil
import os
from selenium import webdriver
import subprocess

import pandas as pd

import ColunasCalculadas

print(datetime.today().strftime('%H:%M'))
start_time = time.time()


login = "erodrigu"

#Fazer download do chromedriver compativel com o seu Chrome e depois colocar o endereço do executável
browser = webdriver.Chrome(executable_path="C:\Python\chromedriver.exe")
browser.get("https://sistema.ssw.inf.br/bin/ssw0422")
browser.maximize_window()
pyautogui.doubleClick(1798, 118)
time.sleep(3)

#encontrando na pagina HTMl local de usuario e senha.
dominio = browser.find_element_by_id("1")
cpf = browser.find_element_by_id("2")
usuario = browser.find_element_by_id("3")
senha = browser.find_element_by_id("4")
botao_login = browser.find_element_by_id('5')
time.sleep(2)

#colocando as credenciais.
dominio.send_keys("BLK")
cpf.send_keys("10741554917")
usuario.send_keys(login)
senha.send_keys("eli4321")
time.sleep(2)
botao_login.click()
time.sleep(10)

#Entrando na tela 455
browser.get("https://sistema.ssw.inf.br/bin/ssw0230")
time.sleep(2)
d = datetime.today() - timedelta(days=30)
d1 = datetime.today().strftime('%d%m%y')
d = d.strftime('%d%m%y') + d1
time.sleep(2)

#colocando o periodo do download
data = browser.find_element_by_name("f11")
data.clear()
data.send_keys(d)
time.sleep(1)

excel = browser.find_element_by_id("35")
excel.clear()
excel.send_keys("E")
time.sleep(1)
funcao_a = browser.find_element_by_id("37")
funcao_b = browser.find_element_by_id("38")
funcao_f = browser.find_element_by_id("39")
funcao_a.send_keys("A")
time.sleep(1)
funcao_b.send_keys("B")
time.sleep(1)
funcao_f.send_keys("F")
time.sleep(10)

#Fazendo download
botao_down = browser.find_element_by_id("40")
botao_down.click()
time.sleep(10)

#Entrando na tela do download
browser.get("https://sistema.ssw.inf.br/bin/ssw1440")
time.sleep(20)
user = ""
i = 2
j = 0
#Buscando na tabela o login do usuario, se o download esta completo e o botao de baixar.
while i < 101:
    print(str(j) +"a tentativa")
    x = "//tr[@class='srtr2'][" + str(i) + "]/td[@class='srtd2'][4]/div[@class='srdvl']"
    user = browser.find_element_by_xpath(x)
    if user.text == login:
        c = "//tr[@class='srtr2'][" + str(i) + "]/td[@class='srtd2'][7]/div[@class='srdvl']"
        concluido = browser.find_element_by_xpath(c)
        if concluido.text == "Concluído":
            b = "//tr[@class='srtr2'][" + str(i) + "]/td[@class='srtd2'][9]/div[@class='srdvl']/a[@class='sra']/u"
            baixar = browser.find_element_by_xpath(b)
            time.sleep(1)
            baixar.click()
            break
        else:
            i = 1
            j = j + 1
            browser.refresh()
            time.sleep(8)
    i = i + 1
time.sleep(5)

#verifica se o arquivo foi baixado
fileDir = r"C:\Users\eliana.rodrigues\Downloads"
fileExt = r".sswweb"
newlist = []
cont = 0
while len(newlist) == 0:
    time.sleep(5)
    items = os.listdir(fileDir)
    for names in items:
        if names.endswith(fileExt):
            newlist.append(names)
    cont = cont + 1
    print(cont, newlist)
time.sleep(15)
ssw455 = str(newlist[0])

#renomeia o arquivo para um auxiliar
old_file = os.path.join(fileDir, ssw455)
new_file = os.path.join(fileDir, "SSW_455_teste.csv")
os.rename(old_file, new_file)

print('Entra na tela 455 pra puxar mês passado')
#chama função que vai abrir o 455 pra baixar o mês anterior pra venda de serviço
subprocess.call("ssw_teste.py", shell=True)

#ajusta o arquivo
#Tira a primeira linha
line_to_delete = 1
initial_line = 1
file_lines = {}
#abre o arquivo e le as linhas
with open(new_file) as f:
    content = f.readlines()
for line in content:
    file_lines[initial_line] = line.strip()
    initial_line += 1

#Deleta a primeira linha
f = open(new_file, "w")
for line_number, line_content in file_lines.items():
    if line_number != line_to_delete:
        f.write('{}\n'.format(line_content))
f.close()

print('cria copia ssw_455_teste')
#faz uma copia o ssw 455 teste
src=r'C:\Users\eliana.rodrigues\Downloads\SSW_455_teste.csv'
des=r'C:\Users\eliana.rodrigues\Downloads\SSW_455_testeCopia.csv'
shutil.copy2(src, des)

################################################
#      DATABASE - SSW 455
#pega as colunas escolhidas do ssw455
colunas_ssw455_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Colunas_SSW_455.txt", sep='\t')
num_linhas = colunas_ssw455_df.shape[0]
list = []
for i in range(0, num_linhas):
    list.append(colunas_ssw455_df.values[i, 0])

#Deleta a primeira coluna do arquivo
novo = r"C:\Users\eliana.rodrigues\Downloads\SSW_455.csv"
df = pd.read_csv(new_file, sep=';', encoding='windows-1252', usecols=list)
#coloca na ordem da lista
df = df[list]
df = ColunasCalculadas.ajustaLetras(df)
df.to_csv(novo, sep=';', index=False)
time.sleep(10)
#apaga o arquivo auxiliar
os.remove(new_file)

#copia o arquivo para a pasta
endereco = r"C:\Users\eliana.rodrigues\Downloads"
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads'
old_file_path = os.path.join(endereco, "SSW_455.csv")
new_file_path = os.path.join(pasta_nova, "SSW_455.csv")
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido - 455")

time.sleep(15)

#calcula a data
d = datetime.today() - timedelta(days=5)
d1 = datetime.today().strftime('%d%m%y')
d = d.strftime('%d%m%y') + d1

#Entra na tela do 036
browser.get("https://sistema.ssw.inf.br/bin/ssw0146")
time.sleep(5)
browser.refresh()
time.sleep(5)

#Coloca data no campo
data = browser.find_element_by_name("f5")
data.clear()
data.send_keys(d)
time.sleep(1)

#Coloca a opçao de baixar em excel
excel = browser.find_element_by_id("11")
excel.clear()
excel.send_keys("S")
time.sleep(5)

#Verifica se tem o arquivo baixado
fileDir = r"C:\Users\eliana.rodrigues\Downloads"
fileExt = r".sswweb"
newlist = []
cont = 0
while len(newlist) == 0:
    time.sleep(5)
    items = os.listdir(fileDir)
    for names in items:
        if names.endswith(fileExt):
            newlist.append(names)
    cont = cont + 1
    print(cont, newlist)

#Modifica o tipo do arquivo para um arquivo auxiliar
ssw036 = str(newlist[0])
old_file = os.path.join(fileDir, ssw036)
new_file = os.path.join(fileDir, "SSW_036_teste.csv")
os.rename(old_file, new_file)

#ajusta o arquivo
#Tira a primeira linha
line_to_delete = 1
initial_line = 1
file_lines = {}

#Le o arquivo
with open(new_file) as f:
    content = f.readlines()
for line in content:
    file_lines[initial_line] = line.strip()
    initial_line += 1

#Deleta a primeira linha
f = open(new_file, "w")
for line_number, line_content in file_lines.items():
    if line_number != line_to_delete:
        f.write('{}\n'.format(line_content))
f.close()

#      DATABASE - SSW 036
# Criando lista de colunas do SSW036
colunas_ssw36_df = pd.read_csv(r"G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\01. Auxiliares\Colunas_SSW_036.txt", sep='\t')
num_linhas = colunas_ssw36_df.shape[0]
list = []
for i in range(0, num_linhas):
    list.append(colunas_ssw36_df.values[i, 0])
print("SSW 036")

#Deleta a primeira coluna e cria o arquivo novo
novo = r"C:\Users\eliana.rodrigues\Downloads\SSW_036.csv"
df1 = pd.read_csv(new_file, sep=';', encoding='windows-1252', usecols=list)
df1 = df1[list]
df1 = ColunasCalculadas.ajustaLetras(df1)
df1.to_csv(novo, sep=';', index=False)
time.sleep(10)
os.remove(new_file)

#copia o arquivo para a rede
endereco = r"C:\Users\eliana.rodrigues\Downloads"
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads'
old_file_path = os.path.join(endereco, "SSW_036.csv")
new_file_path = os.path.join(pasta_nova, "SSW_036.csv")
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido - 036")

browser.close()

print(datetime.today().strftime('%H:%M'))
start_time = time.time()

subprocess.call("VS.py", shell=True)

print('Concluido')

