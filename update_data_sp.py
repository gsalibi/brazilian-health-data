from bs4 import BeautifulSoup
import requests
import csv
import sys
import datetime
import mysql.connector
import os


def fullIBGE(cod6: str):
    a = int(cod6[0])
    b = (int(cod6[1]) * 2) % 10 + (int(cod6[1]) * 2) // 10
    c = int(cod6[2])
    d = (int(cod6[3]) * 2) % 10 + (int(cod6[3]) * 2) // 10
    e = int(cod6[4])
    f = (int(cod6[5]) * 2) % 10 + (int(cod6[5]) * 2) // 10
    digit = (10 - (a + b + c + d + e + f) % 10) % 10
    return cod6 + str(digit)


def convert_to_css(url: str, output_name: str):
    source = requests.get(url).text
    soup = BeautifulSoup(source, 'lxml', from_encoding="cp1252")

    td = soup.find_all('td')

    weeks = []
    for element in td:
        if "SE" in element.text:
            if len(element.text) > 5:
                break
            weeks.append(element.text)

    text = "municipio_ibge"
    for week in weeks:
        text += ',' + week
    text += '\n'
    for i in range(len(td)):
        if len(str(td[i].text).split(' ')[0]) >= 6:
            try:
                teste = int(str(td[i].text).split(' ')[0])
                text += fullIBGE(str(td[i].text).split(' ')[0])
                total_sum = 0
                for j in range(i, len(weeks) + i):
                    total_sum += int(td[j + 1].text.replace('.', ''))
                    text += ',' + str(total_sum)
                text += '\n'

            except ValueError:
                continue
    csv_file = open('data/' + output_name + '.csv', 'wb')
    csv_file.write(text.encode())
    csv_file.close()


def insert_csv_into_DB(csv_path: str, disease_id: int):
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
                VALUES (%s, %s, %s, %s, %s);'

    # get last update dates
    cursor.execute(select_sql)
    last_update = cursor.fetchall()
    last_date = last_update[disease_id - 1][0].strftime('%Y-%m-%d')
    new_date = last_date
    mydb.commit()

    # DRS,GVE,Região de Saúde, município,SE01,SE02,SE03,SE04,SE05,SE06,SE07,SE08,SE09,SE10,SE11,SE12,SE13,SE14,SE15,SE16,SE17,SE18,TOTAL
    csv_file = open(csv_path)
    lines = csv_file.readlines()[1:]
    reader = csv.reader(lines, delimiter=',')

    for row in reader:
        print("Current: " + str(reader.line_num))
        ibge_cod = row[0]
        for col in range(1, len(row)):
            total_cases = row[col]
            week = "2020-W" + str(col - 1)
            date = datetime.datetime.strptime(
                week + '-6', "%Y-W%W-%w").strftime('%Y-%m-%d')
            if date > last_date and int(total_cases) > 0:
                print("Updating line number: " + str(reader.line_num))
                val = (total_cases, None, disease_id, date, ibge_cod)
                cursor.execute(insert_sql, val)
                new_date = max(new_date, date)

    if new_date > last_date:
        cursor.execute('UPDATE Diseases SET LastUpdate = "' +
                       new_date + '" WHERE idDisease = ' + str(disease_id) + ';')
    mydb.commit()


if len(sys.argv) != 2:
    print("usage: python update_data_sp.py outbreak_name")
    print("  outbreak_name: dengue")
    print("                 zika")
    print("                 chikungunya")
else:
    if sys.argv[1] not in ['dengue', 'zika', 'chikungunya']:
        print("invalid argument")
    else:
        url = 'http://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/'
        if sys.argv[1] == 'dengue':
            url += 'dengue/2020/dengue20_se.htm'
            convert_to_css(url, 'dengue_sp')
            insert_csv_into_DB('data/dengue_sp.csv', 1)
        elif sys.argv[1] == 'zika':
            url += 'zika/zika20_se.htm'
            convert_to_css(url, 'zika_sp')
            insert_csv_into_DB('data/zika_sp.csv', 3)
        else:
            url += 'chikung/chikung20_se.htm'
            convert_to_css(url, 'chikungunya_sp')
            insert_csv_into_DB('data/chikungunya_sp.csv', 4)
