import pandas as pd
from utils.sheets import Sheets
from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
import os
import dotenv
import requests 
from bs4 import BeautifulSoup
import re
from selenium.webdriver.support.ui import WebDriverWait

dotenv.load_dotenv()
IMOVIEW_ALIANCA_USER = os.getenv("IMOVIEW_ALIANCA_USER")
IMOVIEW_ALIANCA_PASS = os.getenv("IMOVIEW_ALIANCA_PASS")
PATH_DOWNLOAD_CHROME = os.getenv("PATH_DOWNLOAD_CHROME")

CODE_SHEETS_DADOS_IMOVIEW_ALIANCE = os.getenv("CODE_SHEETS_DADOS_IMOVIEW_ALIANCE")
DADOS_CONTRATOS_IMOVIEW = 'contratos'


def create_chrome_driver() -> webdriver:
    chromeOptions = webdriver.ChromeOptions()
    # Configurar o caminho da pasta de download
    chromeOptions.add_experimental_option('prefs', {
        'download.default_directory': PATH_DOWNLOAD_CHROME,
        'download.prompt_for_download': False,  # Evita que o Chrome pergunte onde salvar os downloads
        'download.directory_upgrade': True,
        'safebrowsing.enabled': True  # Ativa a verificação de segurança para downloads
    })
    chromeOptions.add_argument("--disable-save-password-bubble")
    chromeOptions.add_argument("start-maximized")
    # chromeOptions.add_argument("--headless")
    chromeOptions.add_argument("--disable-dev-shm-usage")
    chromeOptions.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=chromeOptions)
    return driver

def quit_chrome(driver: webdriver)->None:
    print('Fechando Navegador...')
    driver.close()
    driver.quit()

def insert_key(driver: webdriver, xpath: str, key: str) -> None: 
    driver.find_element(By.XPATH, xpath).send_keys(key)
    sleep(1)

def click(driver: webdriver, xpath: str) -> None: 
    driver.find_element(By.XPATH, xpath).click()
    sleep(1)



df_contratos = pd.read_csv(r'C:\Users\thico\Desktop\Dev-Data\Git-Hub\ml-analise-credito\Contratos - contratos-alugueis-2024-10-23-085356.csv', encoding='utf-8')
df_contratos = df_contratos[['Codigo']]

def extrair_dados_contrato(driver: webdriver, contrato_id: int) -> pd.DataFrame:    
    dados = {}
    campos_com_prefixo = [
        ('1', ['Seguradora', 'Nº apólice', 'Início', 'Término', 'Situação']),
        ('2', ['Seguradora', 'Corretora seguro', 'Nº apólice', 'Início', 'Término', 'Valor total', 'Situação'])
    ]

    contrato_url = f"https://app.imoview.com.br/ContratoAluguel/Detalhes/{contrato_id}"
    print(f'Abrindo o contrato {contrato_id} pelo link: {contrato_url}')
    driver.get(contrato_url)
    sleep(3)

    original_window = driver.current_window_handle

    print('Verifica se uma nova guia foi aberta e muda para ela')
    for handle in driver.window_handles:
        if handle != original_window:
            driver.switch_to.window(handle)
            break

    print('Captura o HTML da nova guia')
    page_source = driver.page_source
    site = BeautifulSoup(page_source, "html.parser")
    
    # Extraindo os campos comuns
    campos_comuns = [
        'Código', 'Situação', 'Status', 'Valor aluguel', 'Motivo status', 
        'Unidade', 'Padrão', 'Forma de cobrança', 'Forma de recebimento', 
        'Carteira boleto', 'Ramo atividade', 'Carteira transferência', 'Destinação contrato', 
        'Índice reajuste', 'Aluguel garantido', 'Tempo de garantia do aluguel', 
        'Advogado responsável', 'Responsável pelo contrato', 'Correspondência para', 
        'Forma correspondência', 'Retém IRRF', 'Repasse ISS retido', 'Data aviso', 'Data previsão rescisão',
        'Motivo rescisão', 'Complemento motivo rescisão',
        'Prazo contrato', 'Início do contrato', 'Fim do contrato', 
        'Próximo reajuste', 'Data último reajuste', 'Dia início período', 
        'Data inclusão', 'Data última alteração', 'Dia venc. aluguel', 
        'Data ativação', 'Vencimento aluguel inicial', 'Renovação automática', 
        'Como fazer repasse?', 'Forma de repasse', 'Banco repasse', 
        'Forma transferência', 'Forma correspondência', 'Valor venal do imóvel', 
        'Locatário', 'Imóvel', 'Locadores', 'Negócio', 'Taxa administração', 
        'Taxa intermediação', 'Taxa adm rescisão', 'Taxa adm multa', 
        'Taxa adm juros', 'Taxa adm correção monetária', 'Cobrar 13º', 
        'Multas por atraso', 'Juros por atraso', 'Correção monetária (índice)', 
        'Multa', 'Juros', 'Correção monetária', 'Desconto pontualidade', 
        'Observação garantia', 'Rescisão', 'Texto acerto de contas', 
        'Anotações internas'
    ]

    for campo in campos_comuns:
        elemento = site.find("h4", string=campo)
        if elemento is not None:
            valor = elemento.find_next('span').text.strip()
            dados[campo] = valor
        else:
            dados[campo] = None

    # Extraindo os campos com prefixo "1" normalmente
    for prefixo, campos in campos_com_prefixo:
        if prefixo == '1':
            for campo in campos:
                elemento = site.find("h4", string=campo)
                if elemento is not None:
                    valor = elemento.find_next('span').text.strip()
                    dados[f"{prefixo}_{campo}"] = valor
                else:
                    dados[f"{prefixo}_{campo}"] = None

    # Extraindo os campos com prefixo "2" do painel "Garantia" (painel9)
    painel_garantia = site.find('div', id='painel9')
    if painel_garantia:
        for campo in campos_com_prefixo[1][1]:  # Campos com prefixo "2"
            elemento = painel_garantia.find("h4", string=campo)
            if elemento is not None:
                valor = elemento.find_next('span').text.strip()
                dados[f"2_{campo}"] = valor
            else:
                dados[f"2_{campo}"] = None

    # Extraindo 'Titular repasse' de forma separada
    titular_repasse = site.find('div', class_='col-sm-12 col-md-12').find('h4').get_text()
    if titular_repasse is not None:
        dados['Titular repasse'] = titular_repasse
    else:
        dados['Titular repasse'] = None

    # Extraindo 'Locatario solidario' de forma separada
    panel_body = site.find('div', {'id': 'painel8'})
    locatario_solidario = ""

    if panel_body:
        span = panel_body.find('span')
        if span and "Não possui locatário solidário" not in span.text:
            locatario_solidario = span.text.strip()
    dados['Locatario solidario'] = locatario_solidario

    # Extraindo 'apos_12_meses_não_cobrar_multa' de forma separada
    apos_12_meses_não_cobrar_multa_element = site.find('span', id='aposXMesesNaoCobrarRescisaoTaxaLabel')
    if apos_12_meses_não_cobrar_multa_element is not None:
        dados['apos_12_meses_não_cobrar_multa'] = apos_12_meses_não_cobrar_multa_element.text
    else:
        dados['apos_12_meses_não_cobrar_multa'] = None

    # Extraindo fiadores
    fiador_section = site.find('div', id='painel9')
    fiadores = []

    if fiador_section:
        link_elements = fiador_section.find_all('a', class_='list-group-item')
        for link in link_elements:
            fiadores.append(link.get_text().strip())

    if fiadores:
        for i, fiador in enumerate(fiadores, start=1):
            dados[f'Fiador {i}'] = fiador
    else:
        dados['Fiador'] = None

    # Extraindo o tipo de garantia do 'h4' dentro do painel de garantia
    garantia_element = site.find('div', id='painel9').find('h4')
    garantia_valor = None

    if garantia_element:
        garantia_valor = garantia_element.get_text().strip()
    dados['Garantia'] = garantia_valor

    # Extraindo dados da tabela "Aditivos de prazo"
    aditivos_de_prazo = []
    tabela_aditivos = site.find('h4', string='Aditivos de prazo')
    if tabela_aditivos:
        tabela_aditivos = tabela_aditivos.find_next('table', {'data-toggle': 'table'})
        if tabela_aditivos:
            linhas = tabela_aditivos.find('tbody').find_all('tr')
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) == 4:
                    descricao = colunas[0].text.strip()
                    meses = colunas[1].text.strip()
                    inicio = colunas[2].text.strip()
                    fim = colunas[3].text.strip()

                    # Criando um dicionário para cada linha
                    aditivo = {
                        'Descrição': descricao,
                        'Meses': meses,
                        'Início': inicio,
                        'Fim': fim
                    }
                    aditivos_de_prazo.append(aditivo)
    dados['aditivos_de_prazo'] = aditivos_de_prazo


    # Extraindo dados da tabela "Esteiras"
    esteiras = []
    painel_esteira = site.find('h3', class_='panel-title', string=lambda text: text and 'Esteiras' in text)
    if painel_esteira:
        tabela_esteiras = painel_esteira.find_next('table', {'data-toggle': 'table'})
        if tabela_esteiras:
            linhas = tabela_esteiras.find('tbody').find_all('tr')
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) == 4:
                    codigo = colunas[0].text.strip()
                    data_inicio = colunas[1].text.strip()
                    situacao = colunas[2].text.strip()
                    etapa = colunas[3].text.strip()

                    # Criando um dicionário para cada linha
                    esteira = {
                        'Código': codigo,
                        'Data início': data_inicio,
                        'Situação': situacao,
                        'Etapa': etapa
                    }
                    esteiras.append(esteira)
    dados['esteiras'] = esteiras

    # Extraindo dados da tabela "Vistorias"
    vistorias = []
    painel_vistoria = site.find('h3', class_='panel-title', string=lambda text: text and 'Vistorias' in text)
    if painel_vistoria:
        tabela_vistorias = painel_vistoria.find_next('table', {'data-toggle': 'table'})
        if tabela_vistorias:
            linhas = tabela_vistorias.find('tbody').find_all('tr')
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) == 5:
                    codigo = colunas[0].text.strip()
                    situacao = colunas[1].text.strip()
                    data = colunas[2].text.strip()
                    tipo = colunas[3].text.strip()
                    imovel = colunas[4].text.strip()

                    # Criando um dicionário para cada linha
                    vistoria = {
                        'Código': codigo,
                        'Situação': situacao,
                        'Data': data,
                        'Tipo': tipo,
                        'Imóvel': imovel
                    }
                    vistorias.append(vistoria)
    dados['vistorias'] = vistorias

    # Extraindo dados da tabela "Parceiros"
    parceiros = []
    painel_parceiros = site.find('h3', class_='panel-title', string=lambda text: text and 'Parceiros' in text)
    if painel_parceiros:
        tabela_parceiros = painel_parceiros.find_next('table', {'data-toggle': 'table'})
        if tabela_parceiros:
            linhas = tabela_parceiros.find('tbody').find_all('tr')
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) == 3:
                    nome = colunas[0].text.strip()
                    taxa_inter = colunas[1].text.strip()
                    taxa_adm = colunas[2].text.strip()

                    # Criando um dicionário para cada linha
                    parceiro = {
                        'Nome': nome,
                        'Taxa Inter.': taxa_inter,
                        'Taxa Adm.': taxa_adm
                    }
                    parceiros.append(parceiro)
    dados['parceiros'] = parceiros


    # Extraindo dados da tabela "Envelopes digitais"
    envelopes_digitais = []
    painel_envelopes = site.find('h3', class_='panel-title', string=lambda text: text and 'Envelopes digitais' in text)

    if painel_envelopes:
        tabela_envelopes = painel_envelopes.find_next('table', {'data-toggle': 'table'})
        if tabela_envelopes:
            linhas = tabela_envelopes.find('tbody').find_all('tr')
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) == 5:  # Verifica se a linha possui 5 colunas
                    codigo = colunas[0].text.strip()
                    situacao = colunas[1].text.strip()
                    nome = colunas[2].text.strip()
                    destinatarios = colunas[3].text.strip()
                    contrato = colunas[4].text.strip()

                    # Criando um dicionário para cada linha
                    envelope = {
                        'Código': codigo,
                        'Situação': situacao,
                        'Nome': nome,
                        'Destinatários': destinatarios,
                        'Contrato': contrato
                    }
                    envelopes_digitais.append(envelope)

    # Adicionando os dados ao dicionário principal
    dados['envelopes_digitais'] = envelopes_digitais

    # Extraindo dados da tabela "Documentos e anexos"
    documentos_anexos = []
    painel_documentos = site.find('h3', class_='panel-title', string=lambda text: text and 'Documentos e anexos' in text)

    if painel_documentos:
        tabela_documentos = painel_documentos.find_next('table', {'data-toggle': 'table'})
        if tabela_documentos:
            linhas = tabela_documentos.find('tbody').find_all('tr')
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) == 4:  # Verifica se a linha possui 4 colunas
                    descricao = colunas[0].text.strip()
                    contrato_assinado = colunas[1].text.strip()
                    laudo_vistoria_assinado = colunas[2].text.strip()
                    acao_link = colunas[3].find('a')['href'] if colunas[3].find('a') else None

                    # Criando um dicionário para cada linha
                    documento = {
                        'Descrição': descricao,
                        'Contrato Assinado': contrato_assinado,
                        'Laudo Vistoria Assinado': laudo_vistoria_assinado,
                        'Ação': acao_link
                    }
                    documentos_anexos.append(documento)
    dados['Documentos_anexos'] = [documentos_anexos for _ in range(len(dados))]
    
    df = pd.DataFrame([dados])
    driver.switch_to.window(original_window)

    return df

def login_super(driver: webdriver):
    print('Abrindo navegador...')
    driver.get("https://app.imoview.com.br/Login/LogOn")

    print('Fazendo login...')
    insert_key(driver, '//*[@id="campo_email_login"]', IMOVIEW_ALIANCA_USER)
    sleep(2)
    click(driver, '//*[@id="botao_continuar_email_login"]')
    sleep(2)
    click(driver, '//*[@id="container"]/div/div[1]/form/div[2]/div/div/div[3]/div')
    sleep(1)
    click(driver, '//*[@id="container"]/div/div[1]/form/div[2]/div/div/div[3]/div/div/div/ul/li[2]')
    sleep(2)
    insert_key(driver, '//*[@id="container"]/div/div[1]/form/div[2]/div/div/div[4]/div/input', IMOVIEW_ALIANCA_PASS)
    sleep(2)
    click(driver, '//*[@id="botao_acessar_sistema"]')
    sleep(2)
    click(driver, '//*[@id="modalGeneric"]/div[2]/div/div[3]/button')
    sleep(3)
    click(driver, '//*[@id="mainnav-menu"]/li[5]/a')
    sleep(3)
    click(driver, '//*[@id="Pesquisar"]')
    sleep(2)

def main():
    print('Inicializando o Selenium')
    driver = create_chrome_driver()
    login_super(driver)
    # DataFrame para armazenar todos os dados extraídos
    df_resultado = pd.DataFrame()

    for index, row in df_contratos.iterrows():
        codigo = row['Codigo']
        print(f'Extraindo dados do contrato {codigo}')
        df_contrato = extrair_dados_contrato(driver, codigo)
        df_resultado = pd.concat([df_resultado, df_contrato], ignore_index=True)

    

    print('Dados extraídos:')
    print(df_resultado)
    return df_resultado

if __name__ == "__main__":
    df_resultado = main()


colunas_ordenadas = [
    'Código', 'Situação', 'Status', 'Valor aluguel', 'Motivo status',
       'Unidade', 'Padrão', 'Forma de cobrança', 'Forma de recebimento',
       'Carteira boleto', 'Ramo atividade', 'Carteira transferência', 'Destinação contrato',
       'Índice reajuste', 'Aluguel garantido', 'Tempo de garantia do aluguel',
       'Advogado responsável', 'Responsável pelo contrato',
       'Correspondência para', 'Forma correspondência', 'Retém IRRF',
       'Repasse ISS retido', 'Data aviso', 'Data previsão rescisão',
        'Motivo rescisão', 'Complemento motivo rescisão', 'Prazo contrato', 'Início do contrato',
       'Fim do contrato', 'Próximo reajuste', 'Data último reajuste',
       'Dia início período', 'Data inclusão', 'Data última alteração',
       'Dia venc. aluguel', 'Data ativação', 'Vencimento aluguel inicial',
       'Renovação automática', 'Titular repasse', 'Como fazer repasse?', 'Forma de repasse',
       'Banco repasse', 'Forma transferência', '1_Seguradora', 'Corretora seguro', '1_Nº apólice', '1_Início',
       '1_Término', 'Valor seguro', 'Valor venal do imóvel', 'Locatário', 'Imóvel', 'Locadores', 
       'Negócio','Locatario solidario', 'Taxa administração', 'Taxa intermediação', 'Taxa adm rescisão',
       'Taxa adm multa', 'Taxa adm juros', 'Taxa adm correção monetária',
       'Cobrar 13º', 'Multas por atraso', 'Juros por atraso',
       'Correção monetária (índice)', 'Multa', 'Juros', 'Correção monetária',
       'Desconto pontualidade', 'Rescisão',  'apos_12_meses_não_cobrar_multa',
       'Texto acerto de contas', 'Anotações internas', 'Garantia', '2_Seguradora', '2_Corretora seguro',
       '2_Nº apólice', '2_Início', '2_Término', '2_Valor total', '2_Situação', 
        'Fiador 1', 'Fiador 2', 'Fiador 3', 'Fiador 4', 'Fiador 5', 'Fiador 6', 'Observação garantia', 'aditivos_de_prazo', 
        'esteiras', 'vistorias', 'parceiros', 'envelopes_digitais', 'Documentos_anexos',
]

df_final = df_resultado.reindex(columns=colunas_ordenadas)

sheet = Sheets(CODE_SHEETS_DADOS_IMOVIEW_ALIANCE)

print("Limpando planilha pagina SOMA - EXTRATOS_LOCATARIO_PARTE_1")
sheet.clear_sheets(DADOS_CONTRATOS_IMOVIEW)
print("Fazendo upload para a planilha na pagina SOMA - EXTRATOS_LOCATARIO_PARTE_1")
sheet.upload_to_sheets(df_final, DADOS_CONTRATOS_IMOVIEW)


