import time
import pyautogui
from datetime import datetime, timedelta
from selenium import webdriver

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
time.sleep(20)
browser.get("https://eagle.madeiramadeira.com.br/manifest/transfers")
data = browser.find_element_by_name("create-date")
email_export = browser.find_element_by_id("email-to-export")
time.sleep(10)
dia = 20

d3 = datetime.today() - timedelta(days=dia)
d = datetime.today().strftime('%d/%m/%y')
d3 = d3.strftime('%d/%m/%y')
periodo = d3 + ' a ' + d
data.send_keys(periodo)
time.sleep(2)
botao_status = browser.find_element_by_xpath('/html/body/div[12]/div/div/div[1]/section/div/div[3]/div/div/button/span')
botao_status.click()
time.sleep(2)
botao_todos = browser.find_element_by_xpath('/html/body/div[12]/div/div/div[1]/section/div/div[3]/div/div/ul/li[2]/a/label/div/span/input')
botao_todos.click()

pesquisar = browser.find_element_by_xpath('/html/body/div[12]/div/div/div[1]/section/div/div[5]/button')
pesquisar.click()

if dia == 0:
    time.sleep(15)
else:
    time.sleep(3 * dia)

exportar = browser.find_element_by_xpath('/html/body/div[12]/div/div/div[1]/section/div/div[5]/a[2]')
exportar.click()
time.sleep(8)

email_export.send_keys("processos.logistica@bulkylog.com.br")
time.sleep(2)
download = browser.find_element_by_xpath('/html/body/div[13]/div/div/div[3]/button')
download.click()
time.sleep(20)

browser.close()
print('Concluido')
