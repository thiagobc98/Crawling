from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
from datetime import datetime
import datetime
import dotenv
import os
from utils.sheets import Sheets
from selenium.webdriver.common.keys import Keys
import re


dotenv.load_dotenv()

CODE_SHEETS_VALIDAR_CNAB = os.getenv("CODE_SHEETS_VALIDAR_CNAB")
DADOS_SISTEMA_COMISSAO = 'Dados_sistema- Comissão'
DADOS_SISTEMA_DESPESAS = 'Dados_sistema- Despesas'
UPESTATE_SISTEMA_USER = os.getenv("UPESTATE_SISTEMA_USER")
UPESTATE_SISTEMA_PASS = os.getenv("UPESTATE_SISTEMA_PASS")
sheet = Sheets(CODE_SHEETS_VALIDAR_CNAB)

def insert_date(driver, xpath, date_value):
    element = driver.find_element("xpath", xpath)
    element.click()
    sleep(0.5)
    # Limpa o campo completamente
    element.clear()
    sleep(0.5)
    # Divide a data em dia, mês e ano
    day, month, year = date_value.split("/")
    # Insere o dia
    element.send_keys(day)
    sleep(0.5)
    # Move para o próximo campo (mês) com TAB
    element.send_keys(Keys.TAB)
    element.send_keys(month)
    sleep(0.5)
    # Move para o próximo campo (ano) com TAB
    element.send_keys(Keys.TAB)
    element.send_keys(year)
    sleep(0.5)

def insert_key(driver, xpath, value):
    element = driver.find_element("xpath", xpath)
    element.clear()
    element.send_keys(value)

def click(driver, xpath):
    driver.find_element("xpath", xpath).click()

def create_chrome_driver():
    options = webdriver.ChromeOptions()
    return webdriver.Chrome(options=options)


def login_super(driver):
    print('Abrindo navegador...')
    driver.get("https://app.upestate.com.br/login/restrito")

    print('Fazendo login...')
    insert_key(driver, '//*[@id="user"]', UPESTATE_SISTEMA_USER) 
    sleep(2)
    insert_key(driver, '//*[@id="password"]', UPESTATE_SISTEMA_PASS) 
    sleep(2)
    click(driver, '//*[@id="login"]/div[5]/div/button')
    sleep(2)

    contrato_url = "https://app.upestate.com.br/relatorio-comissao-parceiros" # Alterar aqui se quiser de despesa
    driver.get(contrato_url)
    sleep(3)

    original_window = driver.current_window_handle

    print('Verifica se uma nova guia foi aberta e muda para ela')
    for handle in driver.window_handles:
        if handle != original_window:
            driver.switch_to.window(handle)
            break


    # ---------------------------------------------------------------------------------
    #                                 ALTERAR AQUI 
    # ---------------------------------------------------------------------------------

    data_inicio = "09/25/2024" 
    data_fim = "10/24/2024" 
    parceiro = 'VIA M EMPREENDIMENTOS IMOBILIARIOS LTDA' 

    # ---------------------------------------------------------------------------------


    print('Aplicando filtros...')
    click(driver, '//*[@id="data-init"]')
    insert_date(driver, '//*[@id="data-init"]', data_inicio) 
    sleep(1)
    click(driver, '//*[@id="data-end"]')
    insert_date(driver, '//*[@id="data-end"]', data_fim ) 
    sleep(1)
    click(driver, '//*[@id="select2-id-part-container"]')
    sleep(1)
    insert_key(driver, '/html/body/span/span/span[1]/input', parceiro) 
    sleep(1)
    click(driver, '/html/body/span/span/span[2]')
    sleep(1)
    click(driver, '//*[@id="filter-partner-commission"]/div/div[5]/div/input[2]')
    sleep(5)

    print('Capturando valores após aplicar filtros')
    page_source = driver.page_source
    site = BeautifulSoup(page_source, "html.parser")
    valores = site.find_all("div", class_="right-align")
    valores_lista = [valor.get_text(strip=True) for valor in valores]
    df_valores = pd.DataFrame(valores_lista, columns=["Valor Líquido"])
    
    

    linhas = site.find_all("div", class_=re.compile(r"^r-col op-3 w-row( uti)?$"))

    codigos = []
    faturas = []

    for i, linha in enumerate(linhas):
        colunas = linha.find_all("div", class_="t-col-1 _01 normal _100porcento")
        
        if len(colunas) > 1:
            
            codigo_texto = colunas[1].get_text(separator="<br>").strip()
            codigo_texto = codigo_texto.strip("<br>")  
            print(f"Linha {i}: Código e fatura brutos (limpos):", codigo_texto)

            
            partes = codigo_texto.split("<br>")
            
            if len(partes) == 2:
                codigo, fatura = partes
                codigos.append(codigo)
                faturas.append(fatura)
            else:
                print(f"A linha {i} não tem exatamente dois valores. Ignorando.")
                print(f"Conteúdo encontrado: {codigo_texto}")
                continue

    df_dados = pd.DataFrame({
        'Código': codigos,
        'Fatura': faturas
    })

    print(df_dados)
    df_final = pd.concat([df_dados.reset_index(drop=True), df_valores.reset_index(drop=True)], axis=1)

    return df_final


    
    return df_final

def main():
    print('Inicializando o Selenium')
    driver = create_chrome_driver()
    df_final = login_super(driver)
    driver.quit()    
    sheet.clear_sheets(DADOS_SISTEMA_COMISSAO)
    sheet.upload_to_sheets(df_final, DADOS_SISTEMA_COMISSAO)
    # sheet.clear_and_upload(DADOS_SISTEMA_DESPESAS)
    # sheet.upload_to_sheets(df_final, DADOS_SISTEMA_DESPESAS)


def time_start_pipeline():
    start_time = datetime.datetime.now()
    print("\n\nInício do pipeline:", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    return start_time

def time_end_pipeline(start_time):
    end_time = datetime.datetime.now()
    print("Fim do pipeline:", end_time.strftime("%Y-%m-%d %H:%M:%S"))
    
    elapsed_time = end_time - start_time
    print("Tempo decorrido:", str(elapsed_time).split(".")[0])



if __name__ == "__main__":
    time_start_time = time_start_pipeline()
    main()
    time_end_pipeline(time_start_time)
