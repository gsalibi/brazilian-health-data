import os
import csv
import json
import xlrd
import requests
import mysql.connector


def get_current_data_url():
    url = 'https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalGeral'
    params = {'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-GB,en;q=0.5',
              'X-Parse-Application-Id': 'unAFkcaNDeXajurGB7LChj8SgQYS2ptm',
              'Origin: https://covid.saude.gov.br': 'Connection: keep-alive',
              'Referer': 'https://covid.saude.gov.br/',
              'Pragma': 'no-cache', 'Cache-Control': 'no-cache', 'TE': 'Trailers'}
    req = requests.get(url, params=params)
    url_content = req.content.decode()
    result = json.loads(url_content)
    return result["results"][0]["arquivo"]["url"]


def get_xlsx_from_url(output_path: str):
    # Save file from url
    url = get_current_data_url()
    req = requests.get(url)
    url_content = req.content

    xlsx_file = open(os.getcwd() + output_path, 'wb')
    xlsx_file.write(url_content)
    xlsx_file.close()


def xlsx_to_csv(xlsx_path:str, output_path: str):
    wordbook = xlrd.open_workbook(os.getcwd() + xlsx_path)
    sheet = wordbook.sheet_by_name('Sheet 1')
    csv_file = open(os.getcwd() + output_path, 'w')
    writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

    for rownum in range(sheet.nrows):
        writer.writerow(sheet.row_values(rownum))

    csv_file.close()


def full_ibge_code(cod6: str):
    a = int(cod6[0])
    b = (int(cod6[1]) * 2) % 10 + (int(cod6[1]) * 2) // 10
    c = int(cod6[2])
    d = (int(cod6[3]) * 2) % 10 + (int(cod6[3]) * 2) // 10
    e = int(cod6[4])
    f = (int(cod6[5]) * 2) % 10 + (int(cod6[5]) * 2) // 10
    digit = (10 - (a + b + c + d + e + f) % 10) % 10
    return cod6 + str(digit)


def insert_csv_into_db(csv_path: str):
    # set BD
    mydb = mysql.connector.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        passwd=os.environ['DB_PASS'],
        database=os.environ['DB_NAME']
    )
    cursor = mydb.cursor()
    select_sql = 'SELECT LastUpdate FROM Diseases ORDER BY idDisease;'
    insert_sql = 'INSERT INTO Outbreaks (NumberOfCases, FatalCases, DiseaseID, Date, CityID) \
                VALUES (%s, %s, "5", %s, %s);'

    # get last update dates
    cursor.execute(select_sql)
    last_update = cursor.fetchall()
    mydb.commit()
    last_date = last_update[4][0].strftime('%Y-%m-%d')
    new_date = last_date

    csv_file = open(os.getcwd() + csv_path)
    lines = csv_file.readlines()[1:]
    reader = csv.reader(lines, delimiter=',')
    

    # CSV HEADER
    # "regiao","estado","municipio","coduf","codmun","codRegiaoSaude","nomeRegiaoSaude","data",
    # "semanaEpi","populacaoTCU2019","casosAcumulado","casosNovos","obitosAcumulado","obitosNovos",
    # "Recuperadosnovos","emAcompanhamentoNovos"
    for row in reader:
        # only ibge mun code rows
        if len(row[4]) > 2:
            print("Current: " + str(reader.line_num))
            ibge_cod = full_ibge_code(row[4][:6])
            date = row[7]
            total_cases = row[10]
            total_deaths = row[12]

            if date > last_date and int(total_cases) > 0:
                print("Updating line number: " + str(reader.line_num))
                val = (total_cases, total_deaths, date, ibge_cod)
                cursor.execute(insert_sql, val)
                new_date = max(new_date, date)

    if new_date > last_date:
        cursor.execute('UPDATE Diseases SET LastUpdate = "' +
        new_date + '" WHERE idDisease = 5;')
    mydb.commit()


get_xlsx_from_url('/data/covid_br.xlsx')
xlsx_to_csv('/data/covid_br.xlsx', 'data/covid_br.csv')
insert_csv_into_db('/data/covid_br.csv')
