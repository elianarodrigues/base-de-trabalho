import time
import os
from selenium import webdriver
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

base_wmsjun = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\WMS_Jundiai.csv', sep=';')
#base_wmses = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\WMS_Es.csv', sep=';')
base_wmspe = pd.read_csv(r'C:\Users\eliana.rodrigues\Downloads\WMS_pe.csv', sep=';')

base_wmstodas = pd.DataFrame()

base_wmstodas = base_wmstodas.append(base_wmsjun)
base_wmstodas = base_wmstodas.append(base_wmspe)
#base_wmstodas = base_wmstodas.append(base_wmses)

base_wmstodas.to_csv('C:/Users/eliana.rodrigues/Downloads/WMS.csv', sep=';', index=False)

#Copia para a rede
fileDir = r"C:\Users\eliana.rodrigues\Downloads"
pasta_nova = r'G:\Drives compartilhados\MM - Logística - TRANSPORTES\SERVIDOR\0-Bases_Nova\00. Downloads'
old_file_path = os.path.join(fileDir, "WMS.csv")
new_file_path = os.path.join(pasta_nova, "WMS.csv")
shutil.copy(old_file_path, new_file_path)
print("Arquivo Movido")
print("Concluído")

