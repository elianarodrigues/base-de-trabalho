import time
import pandas as pd
import pyautogui
from datetime import datetime, timedelta
from selenium import webdriver
import os
import shutil
import xlrd
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype

print(datetime.today().strftime('%H:%M'))
start_time = time.time()


browser = webdriver.Chrome(executable_path="C:\Python\chromedriver.exe")
browser.get("https://eagle.madeiramadeira.com.br/login")
browser.maximize_window()
pyautogui.doubleClick(1798, 118)
browser.set_window_rect(x=0, y=0, width=1500, height=1000)

time.sleep(3)
usuario = browser.find_element_by_name("email")
senha = browser.find_element_by_name("password")
botao_login = browser.find_element_by_id('login_button')
usuario.send_keys("eliana.rodrigues@madeiramadeira.com.br")
senha.send_keys("atxqn2875")
botao_login.click()
time.sleep(5)
browser.get("https://eagle.madeiramadeira.com.br/sacs/list-pendencies")
time.sleep(10)
data = browser.find_element_by_xpath("/html/body/div[12]/div/div/div[3]/div/div[1]/div[3]/label/button")
data.click()
time.sleep(10)

#Busca na pasta se o arquivo foi baixado
partialFileName = "eagle-sac-pendencias"
fileDir = r"C:\Users\eliana.rodrigues\Downloads"
fileExt = r".xls"
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
sac = str(newlist[0])
old_file = os.path.join(fileDir, sac)
new_file = os.path.join(fileDir, "listagem-pendencias.xls")
os.rename(old_file, new_file)
print("Arquivo Renomeado")

# #Copia para a rede
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\06. BackAuxiliares'
old_file_path = os.path.join(fileDir, "listagem-pendencias.xls")
new_file_path = os.path.join(pasta_nova, "listagem-pendencias.xls")
shutil.copy(old_file_path, new_file_path)

print("Arquivo Movido")

print(datetime.today().strftime('%H:%M'))
start_time = time.time()


print("Concluído")

