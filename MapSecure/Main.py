# -*- coding: utf-8 -*-
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from TwitterScrapperRoHec import query_tweets
from os import listdir
import openpyxl
import datetime as dt

#Counters
REPORT_SUMMARY_ID = 702
REPORT_ID = 0
TWEETDOC_ID = 250

#Semaphores
MUTEX_BUILD_REPORT_SUMMARIES = 0
MUTEX_RETRIEVE_LAST_NEWS = 1
MUTEX_PROCESS_EXCEL = 1

#Dictionaries
DIC_PRECISION_INDEX = {"Lugar" : 5, "Avenida" : 4, "Colonia" : 3, "Delegacion" : 2}
DIC_VIOLENCE_INDEX = {"asalto" : 2, "asaltar" : 2, "violacion" : 3, "violar" : 3, "secuestro" : 3,
                     "secuestrar" : 3, "homicidio" : 3, "feminicidio" : 3, "robo" : 2, "robar" : 2,
                     "acoso" : 1, "acosar" : 1, "hurto" : 1, "hurtar" : 1, "desbalijar" : 1, "balazo" : 3,
                     "balacear" : 3, "golpearon" : 1, "acuchillar" : 2, "alerta amber" : 2, "desaparecido" : 1,
                     "raptar" : 2, "balacera" : 2}

YESTERDAY_DATE = dt.date.today() - dt.timedelta(1)


def load_array(file_name):
    file = open(file_name, 'r', encoding='utf-8')
    arr_of_lines = []
    for line in file:
        line.lower()
        arr_of_lines.append(line.strip("\n"))
    return arr_of_lines


def create_new_report_summary(location, crime, num_crimes, precision_index, violence_index):
    global REPORT_SUMMARY_ID
    users_ref = ref.child('ReportSummary' + str(REPORT_SUMMARY_ID))
    users_ref.set({
        'Ubicacion': location,
        'Crimen': crime,
        'numDelitos': num_crimes,
        'IndicePrecison': precision_index,
        'gravedadDelito' : violence_index
    })
    REPORT_SUMMARY_ID += 1


def create_new_report(message):
    global REPORT_ID
    users_ref = ref.child('Denuncia' + str(REPORT_SUMMARY_ID))
    users_ref.set({
        'Mensaje': message
    })
    REPORT_ID += 1


def build_report_summaries(source_directory):
    global MUTEX_BUILD_REPORT_SUMMARIES
    if MUTEX_BUILD_REPORT_SUMMARIES == 0:
        MUTEX_BUILD_REPORT_SUMMARIES = 1
        for fileName in listdir(source_directory):
            try:
                file = open(source_directory + '/' + fileName, 'r', encoding='utf-8')
                lines = [line for line in file]
                header = lines[0]
                lines = lines[1:]
                lines.sort()
                lines.insert(0, header)
                counter = 0
                for i in range(1, len(lines)):
                    try:
                        if lines[i][:15] != lines[i-1][:15]:
                            counter += 1
                    except:
                        continue
                if counter >= 1:
                    header_line = lines[0][7:-2].split(":")
                    create_new_report_summary(header_line[1], header_line[3], counter,
                                              DIC_PRECISION_INDEX[header_line[0]], DIC_VIOLENCE_INDEX[header_line[3]])
            except:
                continue


def retrieve_last_news(source_directory):
    global MUTEX_RETRIEVE_LAST_NEWS
    if MUTEX_RETRIEVE_LAST_NEWS == 0:
        MUTEX_RETRIEVE_LAST_NEWS = 1
        for fileName in listdir(source_directory):
            file = open(source_directory + '/' + fileName, 'r', encoding='utf-8')
            lines = [line for line in file]
            lines = lines[1:]
            lines.sort()
            for i in range(1, len(lines)):
                try:
                    if lines[i][:15] != lines[i-1][:15]:
                        create_new_report(lines[i])
                except:
                    continue


def process_excel_government_statistics(filename):
    global MUTEX_PROCESS_EXCEL
    if MUTEX_PROCESS_EXCEL == 0:
        MUTEX_PROCESS_EXCEL = 1
        doc = openpyxl.load_workbook(filename)
        doc.get_sheet_names()
        hoja = doc.get_sheet_by_name('Hoja1')
        for fila in hoja.rows:
            for columna in fila:
                if type(columna.value) == int:
                    numDelitos = columna.value
                else:
                    delegacion = (columna.value).lower()
            try:
                create_new_report_summary(delegacion, "asalto", numDelitos, 2, 2)
            except:
                continue


def search(crime, arr_locations, location_type, recent_search=False):
    global  TWEETDOC_ID
    for location in arr_locations:
        file = open('Output' + location_type + '/' + 'tdoc' + str(TWEETDOC_ID), 'w', encoding='utf-8')
        file.write("Header[" + location_type + ":" + location + ":Delito:" + crime + "]\n")
        query = crime + " AND " + location
        if recent_search:
            for tweet in query_tweets(query, 500, begindate=YESTERDAY_DATE, poolsize=10):
                try:
                    file.write(tweet.text + "\n")
                    file.write()
                except:
                    continue
        else:
            for tweet in query_tweets(query, 500, poolsize=10):
                try:
                    file.write(tweet.text + "\n")
                    file.write()
                except:
                    continue
        file.close()
        TWEETDOC_ID += 1

cred = credentials.Certificate('Files/firebaseCredentials.json')

# Initialize the app with a service account, granting admin privileges
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://sec1-870c5.firebaseio.com/'
})
ref = db.reference('')

if __name__ == '__main__':
    arr_delitos = load_array("Files/Delitos.txt")
    arr_avenidas = load_array("Files/Avenidas.txt")
    arr_colonias = load_array("Files/Colonias.txt")
    arr_delegaciones = load_array("Files/Delegaciones.txt")
    arr_lugares = load_array("Files/Lugares.txt")

    for delito in arr_delitos:
        if delito == "END":
            MUTEX_BUILD_REPORT_SUMMARIES = 0
            MUTEX_PROCESS_EXCEL = 0
            break
        # search(delito, arr_avenidas, "Avenida")
        # search(delito, arr_colonias, "Colonia")
        # search(delito, arr_delegaciones, "Delegacion")
        # search(delito, arr_lugares, "Lugar")
        search(delito, arr_avenidas, "Avenida", recent_search=True)
        # search(delito, arr_colonias, "Colonia", recent_search=True)
        # search(delito, arr_delegaciones, "Delegacion", recent_search=True)
        # search(delito, arr_lugares, "Lugar", recent_search=True)



# process_excel_government_statistics("FIles/DatosGobAsaltos.xlsx")

# build_report_summaries('OutputLugar')
# build_report_summaries('OutputColonia')
# build_report_summaries('OutputDelegacion')
# build_report_summaries('OutputAvenida')


# retrieve_last_news('OutputColonia')
# retrieve_last_news('OutputLugar')
# retrieve_last_news('OutputDelegacion')
retrieve_last_news('OutputAvenida')