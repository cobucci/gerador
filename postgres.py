# -*- coding: latin-1 -*-
from builtins import list
import requests
import json
import geojson
import numpy as np
import csv
import os
from random import *
import time
from datetime import *
import psycopg2
import random

vetor = ['a', 'b', 'c', 'd', 'e', 'f', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
type = ["ROAD_CLOSED", "ACCIDENT", "WEATHERHAZARD", "JAM"]
subtypes = ["HAZARD_WEATHER_FLOOD",
            "HAZARD_ON_SHOULDER_ANIMALS",
            "ACCIDENT_MINOR",
            "HAZARD_ON_ROAD_OIL",
            "ROAD_CLOSED_EVENT",
            "ACCIDENT_MAJOR",
            "HAZARD_ON_ROAD_CONSTRUCTION",
            "HAZARD_ON_ROAD",
            "JAM_STAND_STILL_TRAFFIC",
            "HAZARD_WEATHER_FOG",
            "ROAD_CLOSED_CONSTRUCTION",
            "HAZARD_ON_ROAD_ROAD_KILL",
            "JAM_MODERATE_TRAFFIC",
            "HAZARD_ON_ROAD_CAR_STOPPED",
            "HAZARD_WEATHER",
            "ROAD_CLOSED_HAZARD",
            "HAZARD_ON_ROAD_POT_HOLE",
            "HAZARD_ON_SHOULDER_CAR_STOPPED",
            "HAZARD_ON_ROAD_OBJECT",
            "JAM_HEAVY_TRAFFIC",
            "HAZARD_ON_SHOULDER",
            "HAZARD_ON_SHOULDER_MISSING_SIGN",
            "HAZARD_WEATHER_HAIL",
            "HAZARD_ON_ROAD_ICE",
            "HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT",
            "NULL"]
quantRuas = 3871

d = ""
t = 0

road_type = [7, 0, 1, 5, 4, 2, 6, 17, 20]


def gerarAlerta(quantidade, d, d2):
    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

    for x in range(quantidade):

        id = gerarId()
        # print(id)
        uuid = gerarUuid()
        # print(uuid)
        pub_mills = str(gerarPubMillis())
        dt = gerarData(d, d2)
        data = str(dt)
        # data = "2020-12-07 21:57:49"
        # print(data)
        rt = str(gerarRoadType())
        listaRetorno = gerarLocation()
        location = gerarStringLocation(listaRetorno)
        # print(location)
        street = listaRetorno[2]
        # print(street)
        city = "Joinville"
        country = "BR"
        magvar = str(gerarMagvar())
        reliability = str(gerarReliability())
        # print(reliability)
        report_description = ""
        reportRating = str(gerarReporRating())
        # print(reportRating)
        confidence = str(gerarConfidence())
        # print(confidence)
        type1 = gerarType()
        # print(t)
        st = gerarSubType()
        # print(st)
        reportBy = False
        thumbs = 0
        jammuui = ""
        datafile_ID = str(gerarDFI())

        geomAux1 = "SELECT ST_GeomFromText('POINT("
        latText = str(listaRetorno[0])
        lonText = str(listaRetorno[1])
        geomAux1 += latText
        geomAux1 += " "
        geomAux1 += lonText
        geomAux1 += ")', 4326)"
        # print(geomAux1)

        cur.execute(geomAux1)
        mobile_records = cur.fetchone()
        aux = ""
        palavra = str(mobile_records)
        for x in palavra:
            if x != "(" and x != "'" and x != "," and x != ")":
                aux += x

        # print(aux)

        req = "INSERT INTO waze.alerts values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        values = [id, uuid, pub_mills, data, rt, location, street, city, country, magvar, reliability,
                  report_description,
                  reportRating, confidence, type1, st, reportBy, thumbs, jammuui, datafile_ID, aux]
        cur = con.cursor()
        cur.execute(req, values)
        con.commit()

        print("Id alerta = " + id + " | tempo = " + data)

    # closing database connection.
    if (con):
        cur.close()
        con.close()
        print("Conexao com o PostgreSQL fechada\n\n")


def gerarJam(quantidade, d, d2):
    for x in range(quantidade):
        lat = []
        lon = []
        id = gerarId()
        uuid = gerarUuid()
        pub_mills = str(gerarPubMillis())
        dt = gerarData(d, d2)
        data = str(dt)
        startNode = ""
        endNode = ""
        rt = gerarRoadType()
        city = "Joinville"
        country = "BR"
        delay = gerarDelay()
        speed = gerarSpeed()
        speedKMH = speed * 4.18
        # print(speed, speedKMH)
        length = gerarLength()
        turnType = "NONE"
        level = gerarLevel()
        blocking = ""
        lat, lon, line, street = gerarLine()
        type1 = "NONE"
        turnLine = ""
        datafile_ID = str(gerarDFI())

        geomAux1 = ""
        if len(lat) == 1:
            geomAux1 = "SELECT ST_GeomFromText('POINT("
            latText = str(lat[0])
            lonText = str(lon[0])
            geomAux1 += latText
            geomAux1 += " "
            geomAux1 += lonText
            geomAux1 += ")', 4326)"
            print(geomAux1)
        else:
            geomAux1 = "SELECT ST_GeomFromText('LINESTRING("
            for x in range(len(lat)):
                if x != 0:
                    geomAux1 += ","
                    latText = str(lat[x])
                    lonText = str(lon[x])
                    geomAux1 += latText
                    geomAux1 += " "
                    geomAux1 += lonText
                else:
                    latText = str(lat[x])
                    lonText = str(lon[x])
                    geomAux1 += latText
                    geomAux1 += " "
                    geomAux1 += lonText

            geomAux1 += ")', 4326)"
        # print(geomAux1)

        con = psycopg2.connect(host='127.0.0.1', database='ic',
                               user='lucas', password='')
        cur = con.cursor()
        cur.execute(geomAux1)
        mobile_records = cur.fetchone()
        aux = ""
        palavra = str(mobile_records)
        for x in palavra:
            if x != "(" and x != "'" and x != "," and x != ")":
                aux += x

        req = "INSERT INTO waze.jams values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        values = [id, uuid, pub_mills, data, startNode, endNode, rt, street, city, country, delay, speed, speedKMH,
                  length, turnType, level, blocking, line, type1, None, datafile_ID, aux]
        cur = con.cursor()
        cur.execute(req, values)
        con.commit()

        print("Id jam = " + id + " | tempo = " + data)

    # closing database connection.
    if (con):
        cur.close()
        con.close()
        print("PostgreSQL connection is closed\n\n")


def gerarLine():
    arquivo = "ruas&CoordenadasOrdenadas.txt"
    nomeRuas = "ruas.txt"

    lat = []
    lon = []

    cont = 0
    rand = randrange(0, quantRuas - 1)  # faixa de inteiro
    with open(nomeRuas, encoding="utf8", errors='ignore') as txt_reader:
        line = txt_reader.readline()
        while rand != cont:
            cont += 1
            line = txt_reader.readline()
        with open(arquivo, encoding="utf8", errors='ignore') as reader:
            line1 = reader.readline()
            nome = line1
            while line1:
                if nome == line:
                    line1 = reader.readline()
                    while line1.startswith("-"):
                        aux = line1.split()
                        latAux = float(aux[0])
                        lonAux = float(aux[1])
                        lat.append(latAux)
                        lon.append(lonAux)
                        line1 = reader.readline()

                    if len(lat) == 1:
                        linestring = "["
                        listaRetorno = [lat[0], lon[0]]
                        linestring = gerarStringLocation(listaRetorno)
                        linestring += "]"
                        return lat, lon, linestring, line
                    else:
                        linestring = "["
                        randNumeroDeCoordenadas = randrange(1, len(lat))
                        if randNumeroDeCoordenadas == len(lat):
                            for j in range(len(lat)):
                                if j != 0:
                                    linestring += ", "
                                listaRetorno = [lat[j], lon[j]]
                                linestring += gerarStringLocation(listaRetorno)
                            linestring += "]"
                            return lat, lon, linestring, line

                        else:
                            linestring = "["
                            randCoordInicial = randrange(0, len(lat) - randNumeroDeCoordenadas)
                            for j in range(randNumeroDeCoordenadas):
                                if j != 0:
                                    linestring = linestring + ", "
                                listaRetorno = [lat[j + randCoordInicial], lon[j + randCoordInicial]]
                                linestring += gerarStringLocation(listaRetorno)
                            linestring += "]"
                            return lat, lon, linestring, line
                else:
                    line1 = reader.readline()
                    nome = line1
            lat.clear()
            lon.clear()


def gerarLevel():
    r = randint(1, 5)
    return r


def gerarLength():
    r = randint(14, 25273)
    return r


def gerarDelay():
    r = randint(-1, 11773)
    return r


def gerarSpeed():
    r = random.uniform(0.0, 21.0)
    return r


def gerarId():
    id = ""
    for x in range(40):
        r = randrange(0, len(vetor))
        id = id + vetor[r]
    return id


def gerarUuid():
    id = ""
    for x in range(8):
        r = randrange(0, len(vetor))
        id = id + vetor[r]

    id += "-"
    for i in range(3):
        for j in range(4):
            r = randrange(0, len(vetor))
            id = id + vetor[r]
        id += "-"

    for x in range(12):
        r = randrange(0, len(vetor))
        id = id + vetor[r]

    return id


def gerarPubMillis():
    r = randint(1506463716372, 1536087473449)
    return r


def gerarData(d, d2):
    d = d.split()
    for x in range(len(d)):
        aux = d[x]
        aux = int(aux)
        d[x] = aux
    timestamp = datetime(year=d[0], month=d[1], day=d[2], hour=d[3], minute=d[4], second=d[5], microsecond=d[6] * 1000)
    td = timedelta(days=(d[0] * 2020) + (d[1] * 30) + d[2], seconds=d[5], microseconds=d[6] * 1000, milliseconds=d[6],
                   minutes=d[4], hours=d[3])
    # print(td.total_seconds())

    d2 = d2.split()
    for x in range(len(d2)):
        aux = d2[x]
        aux = int(aux)
        d2[x] = aux
    timestamp1 = datetime(year=d2[0], month=d2[1], day=d2[2], hour=d2[3], minute=d2[4], second=d2[5],
                          microsecond=d2[6] * 1000)
    td1 = timedelta(days=(d2[0] * 2020) + (d2[1] * 30) + d2[2], seconds=d2[5], microseconds=d2[6] * 1000,
                    milliseconds=d2[6],
                    minutes=d2[4], hours=d2[3])

    tst = td.total_seconds()
    tst1 = td1.total_seconds()
    difMili = (tst1 - tst) * 1000

    r = randint(0, difMili)

    mili = timedelta(milliseconds=r)
    new_timestamp = timestamp
    new_timestamp += mili

    return new_timestamp


def gerarRoadType():
    r = randint(0, len(road_type) - 1)
    return road_type[r]


def gerarLocation():
    listaRetorno = []
    arquivo = "Ruas&Coordenadas.txt"
    nomeRuas = "ruas.txt"
    lat = []
    lon = []
    cont = 0
    rand = randrange(0, quantRuas - 1)  # faixa de inteiro
    with open(nomeRuas, encoding="utf8", errors='ignore') as txt_reader:
        line = txt_reader.readline()
        while rand != cont:
            cont += 1
            line = txt_reader.readline()
        with open(arquivo, encoding="utf8", errors='ignore') as reader:
            line1 = reader.readline()
            nome = line1
            while line1:
                if nome == line:
                    line1 = reader.readline()
                    while line1.startswith("-"):
                        aux = line1.split()
                        latAux = float(aux[0])
                        lonAux = float(aux[1])
                        lat.append(latAux)
                        lon.append(lonAux)
                        line1 = reader.readline()
                    rand1 = randrange(0, len(lat) - 1)  # faixa de inteiro
                    # print("%s (%.6f , %.6f)\n" % (nome, lat[rand1], lon[rand1]))
                    listaRetorno.append(str(lat[rand1]))
                    listaRetorno.append(str(lon[rand1]))
                    listaRetorno.append(nome)
                    return listaRetorno

                else:
                    line1 = reader.readline()
                    nome = line1


def gerarStringLocation(listaRetorno):
    s1 = str(listaRetorno[0])
    s2 = str(listaRetorno[1])
    l1 = '{"x":'
    l1 += s1
    l2 = ',"y":'
    l1 += l2
    l1 += s2
    l2 = '}'
    l1 += l2
    # print(l1)
    return l1


def gerarMagvar():
    r = randint(0, 359)
    return r


def gerarReliability():
    r = randint(5, 10)
    return r


def gerarReporRating():
    r = randint(0, 5)
    return r


def gerarConfidence():
    r = randint(0, 5)
    return r


def gerarType():
    r = randint(0, 3)
    return type[r]


def gerarSubType():
    r = randint(0, 25)
    return subtypes[r]


def gerarDFI():
    r = randint(1, 182974)
    return r


def geograficCell():
    print("Iniciando funcao : geograficCell")
    latTopLeft = -48.933195
    lonTopLeft = -26.139358
    latBottomRight = -48.727638
    lonBottomRight = -26.433591

    quantLat = input("Digite a quantidade LINHAS : ")
    quantLon = input("Digite a quantidade COLUNAS : ")
    quantLatStr = quantLat
    quantLonStr = quantLon
    quantLat = int(quantLat)
    quantLon = int(quantLon)
    quantCelulas = quantLat * quantLon
    print("Quantidade TOTAL de CELULAS GEOGRAFICAS = %d" % quantCelulas)

    tamLat = -1 * ((latTopLeft - latBottomRight) / quantLat)  # definindo o tamanho total da latitude no mapa
    tamLon = -1 * ((lonTopLeft - lonBottomRight) / quantLon)  # definindo o tamanho total da longitude no mapa

    # utilizados na funcao registrosPorCG
    round(tamLon, 6)
    round(tamLat, 6)

    auxList = []
    auxList.append(latTopLeft)
    auxList.append(lonTopLeft)
    auxList.append(tamLat)
    auxList.append(tamLon)
    auxList.append(quantLat)
    auxList.append(quantLon)

    cont = 1
    tabela = "Tabela-" + quantLonStr + "x" + quantLatStr
    tabelaCSV = tabela + ".csv"
    with open(tabelaCSV, 'w', newline='\n', encoding='utf-8') as csvFile:  # escrevendo na tabela
        writer = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvData = ['left', 'top', "right", "bottom", "id"]
        # Serão necessários 2 conjuntos de coordenadas. (left, top) e (right, bottom), alem do id.
        # (left, top) são as coordenadas superiores da esquerda de um retangulo.
        # (right, bottom) são as coordenadas inferiores da direita de um retangulo
        writer.writerow(csvData)
        for i in range(quantLon):
            for j in range(quantLat):
                left = latTopLeft + (j * tamLat)
                top = lonTopLeft + (i * tamLon)
                right = left + tamLat
                bottom = top + tamLon
                id = cont
                cont += 1
                writer.writerow(["%.6f" % left, "%.6f" % top, "%.6f" % right, "%.6f" % bottom, "%d" % id])
    print("Terminando funcao : geograficCell")
    bitMap(tabela, auxList)


def bitMap(tabela, auxList):
    print("Iniciando a funcao : bitMap")
    # colocando as ruas em uma lista
    vRuas = []
    localizacao = []
    cg = []
    with open("Ruas&Coordenadas.txt", encoding="utf8", errors='ignore') as txt_reader:
        line = txt_reader.readline()
        while line:
            if not line.startswith("-"):
                vRuas.append(line.rstrip('\n'))
            line = txt_reader.readline()

    # pegando a quantidade de celulas geografica
    quantCelulas = -1  # pq tem a linha com os nomes dos campos
    tabelaCSV = tabela + ".csv"
    with open(tabelaCSV, encoding="utf8", errors='ignore') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            quantCelulas += 1

    # lembrar que as celulas geograficas comecam no 1
    readingCsvParameters = []
    readingCsvParameters.append(tabelaCSV)
    readingCsvParameters.append(" ")
    for r in vRuas:
        readingCsvParameters[1] = r
        localizacao = readingCSV(readingCsvParameters)
        iLoc = []  # lista de celulas geograficas com valores inteiros
        for i in range(len(localizacao)):
            iLoc.append(int(localizacao[i]))
        l1 = []
        for i in range(quantCelulas):
            if i + 1 not in iLoc:
                l1.append(0)
            else:
                l1.append(1)
        cg.append(l1)

        nameFile = "Ruas&BitMap-" + tabela + ".txt"
        with open(nameFile, "a") as writter:
            writter.write("%s\n" % r)
            for i in l1:
                writter.write("%d " % i)
            writter.write("\n\n")
    print("Terminando a funcao : bitMap")
    print("Iniciando a funcao : gerador")
    gerador(auxList)


def gerador(auxList):


    op = int(input("Digite:\n1->ALERTA\n2->JAM\n3->SAIR\n"))

    op2 = int(input("Digite a quantidades de registros de ocorrências por CG's : "))
    auxList.append(op2)
    listaInput = input("Digite as GC`s em ordem crescente separados por espaco: ")
    listaGC = listaInput.split()

    if op == 1:
        alertaRegistrosPorGcEspecifico(auxList, listaGC)

    elif op == 2:
        jamRegistrosPorGcEspecifico(auxList, listaGC)




def alertaRegistrosPorGcEspecifico(auxList, listaGC):
    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

    d = input(
        "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 57 48 140):  ")
    d2 = input(
        "Digite a data final separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 59 48 140):  ")

    arquivo = "Ruas&Coordenadas.txt"
    identificador = 1
    lat = 0.0
    lon = 0.0

    latTopLeft = auxList[0]
    lonTopLeft = auxList[1]
    tamLat = auxList[2]
    tamLon = auxList[3]
    quantLat = auxList[4]
    quantLon = auxList[5]
    quantidade = auxList[6]

    listaAux = []
    for x in range(len(listaGC)):
        aux = int(listaGC[x])
        listaAux.append(aux)

    for i in range(quantLon):
        for j in range(quantLat):
            if identificador in listaAux:
                left = latTopLeft + (j * tamLat)
                top = lonTopLeft + (i * tamLon)
                right = left + tamLat
                bottom = top + tamLon
                rua = []
                vlat = []
                vlon = []
                # print("%f %f | %f %f" % (left, top, right, bottom))

                with open(arquivo, encoding="utf8", errors='ignore') as txt_reader:
                    line = txt_reader.readline()
                    while line:
                        if not line.startswith("-"):
                            nomeRua = line
                            # print(nomeRua)
                        elif line.startswith("-"):
                            aux = line.split()
                            lat = float(aux[0])
                            lon = float(aux[1])
                            # print("%f - %f " % (lat, lon))
                        line = txt_reader.readline()

                        if lat >= left and lat < right:
                            if lon <= top and lon > bottom:
                                rua.append(nomeRua)
                                vlat.append(lat)
                                vlon.append(lon)

                # print("CG %d - %d" % (id, len(rua)))
                if len(rua) == 0:
                    print("GC %d vazia" % identificador)
                else:
                    print("GC %d " % identificador)
                    for x in range(quantidade):
                        rand = randrange(0, len(rua) - 1)  # faixa de inteiro
                        print("%s %f %f" % (rua[rand], vlat[rand], vlon[rand]))
                        id = gerarId()
                        uuid = gerarUuid()
                        pub_mills = str(gerarPubMillis())
                        dt = gerarData(d, d2)
                        data = str(dt)
                        rt = str(gerarRoadType())
                        listaRetorno = []
                        listaRetorno.append(str(vlat[rand]))
                        listaRetorno.append(str(vlon[rand]))
                        listaRetorno.append(rua[rand])
                        location = gerarStringLocation(listaRetorno)
                        street = listaRetorno[2]
                        city = "Joinville"
                        country = "BR"
                        magvar = str(gerarMagvar())
                        reliability = str(gerarReliability())
                        report_description = ""
                        reportRating = str(gerarReporRating())
                        confidence = str(gerarConfidence())
                        type1 = gerarType()
                        st = gerarSubType()
                        reportBy = False
                        thumbs = 0
                        jammuui = ""
                        datafile_ID = str(gerarDFI())

                        geomAux1 = "SELECT ST_GeomFromText('POINT("
                        latText = str(listaRetorno[0])
                        lonText = str(listaRetorno[1])
                        geomAux1 += latText
                        geomAux1 += " "
                        geomAux1 += lonText
                        geomAux1 += ")', 4326)"
                        # print(geomAux1)

                        cur.execute(geomAux1)
                        mobile_records = cur.fetchone()
                        aux = ""
                        palavra = str(mobile_records)
                        for x in palavra:
                            if x != "(" and x != "'" and x != "," and x != ")":
                                aux += x

                        # print(aux)

                        req = "INSERT INTO waze.alerts values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        values = [id, uuid, pub_mills, data, rt, location, street, city, country, magvar, reliability,
                                  report_description,
                                  reportRating, confidence, type1, st, reportBy, thumbs, jammuui, datafile_ID, aux]
                        cur = con.cursor()
                        cur.execute(req, values)
                        con.commit()

                        print("Alerta numero = " + str(identificador) + " | tempo = " + data + "\n")

                print("\n\n")
                rua.clear()
                vlat.clear()
                vlon.clear()
            identificador += 1


    if con:
        cur.close()
        con.close()
        print("Conexao com o PostgreSQL fechada\n\n")

def readingCSV(readingCsvParameters):

    highwayCoords = []
    with open(readingCsvParameters[0], encoding="utf8", errors='ignore') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        # pegar as coordenadas da rua e colocar em uma lista
        with open("Ruas&Coordenadas.txt", encoding="utf8", errors='ignore') as txt_reader:
            line = txt_reader.readline()
            n = readingCsvParameters[1] + "\n"
            while line:
                # print(n)
                if line == n:
                    line = txt_reader.readline()
                    while line.startswith("-"):
                        highwayCoords.append(line.split())
                        line = txt_reader.readline()
                line = txt_reader.readline()
        # terminei de pegar as coordenadas da rua

        # verificar se a rua esta dentro de cada celula geografica
        line_count = 0
        celulas = []
        for row in csv_reader:
            if line_count == 1:
                for hc in highwayCoords:
                    # passando os valores para float
                    lonHighway = float(hc[0])  # longitude da rua
                    latHighway = float(hc[1])  # latitude da rua
                    lonRowS = float(row[0][:10])  # longitude superior da celula geografica
                    lonRowI = float(row[2][:10])  # longitude inferior da celula geografica
                    latRowS = float(row[1][:10])  # latitude superior da celula geografica
                    latRowI = float(row[3][:10])  # latitude inferior da celula geografica
                    if lonHighway >= lonRowS and lonHighway <= lonRowI and latHighway <= latRowS and latHighway >= latRowI:
                        if row[4] not in celulas:
                            celulas.append(row[4])
            line_count = 1
        # print(celulas)
        return celulas


def jamRegistrosPorGcEspecifico(auxList, listaGC):

    d = input(
        "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 57 48 140):  ")
    d2 = input(
        "Digite a data final separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 59 48 140):  ")

    arquivo = "ruas&CoordenadasOrdenadas.txt"
    identificador = 1
    lat = 0.0
    lon = 0.0

    latTopLeft = auxList[0]
    lonTopLeft = auxList[1]
    tamLat = auxList[2]
    tamLon = auxList[3]
    quantLat = auxList[4]
    quantLon = auxList[5]
    quantidade = auxList[6]

    listaAux = []
    for x in range(len(listaGC)):
        aux = int(listaGC[x])
        listaAux.append(aux)

    for i in range(quantLon):
        for j in range(quantLat):
            if identificador in listaAux:
                left = latTopLeft + (j * tamLat)
                top = lonTopLeft + (i * tamLon)
                right = left + tamLat
                bottom = top + tamLon
                rua = []
                vlat = []
                vlon = []

                with open(arquivo, encoding="utf8", errors='ignore') as txt_reader:
                    line = txt_reader.readline()
                    while line:
                        if not line.startswith("-"):
                            nomeRua = line
                            # print(nomeRua)
                        elif line.startswith("-"):
                            aux = line.split()
                            lat = float(aux[0])
                            lon = float(aux[1])
                            # print("%f - %f " % (lat, lon))
                        line = txt_reader.readline()

                        if lat >= left and lat < right:
                            if lon <= top and lon > bottom:
                                rua.append(nomeRua)
                                vlat.append(lat)
                                vlon.append(lon)

                    # escolher as ruas
                    if len(rua) == 0:
                        print("GC %d vazia" % identificador)
                    else:
                        print("GC %d " % identificador)
                        for x in range(quantidade):
                            randCidade = 0
                            if len(rua) > 1:
                                randCidade = randrange(0, len(rua) - 1)  # faixa de inteiro
                            vLatAux = []
                            vLonAux = []
                            latUsar = []
                            lonUsar = []
                            for i in range(len(rua)):
                                if rua[i] == rua[randCidade]:
                                    vLatAux.append(vlat[i])
                                    vLonAux.append(vlon[i])

                            if len(vLatAux) == 1:
                                """
                                print(
                                    "REGISTRO %d - %s %f %f" % (x + 1, rua[randCidade], vLatAux[0], vLonAux[0]))
                                
                                """
                                gerarJamCG(rua[randCidade], vLatAux, vLonAux, d, d2)

                            else:
                                randNumeroDeCoordenadas = randrange(1, len(vLatAux))
                                if randNumeroDeCoordenadas == len(vLatAux):
                                    for j in range(len(vLatAux)):
                                        """
                                        print("REGISTRO %d - %s %f %f" % (
                                        x + 1, rua[randCidade], vLatAux[j], vLonAux[j]))
                                        """
                                        latUsar.append(vLatAux[j])
                                        lonUsar.append(vLonAux[j])

                                else:
                                    randCoordInicial = randrange(0, len(vLatAux) - randNumeroDeCoordenadas)
                                    for j in range(randNumeroDeCoordenadas):
                                        """
                                        print("REGISTRO %d - %s %f %f" % (
                                        x + 1, rua[randCidade], vLatAux[j + randCoordInicial],
                                        vLonAux[j + randCoordInicial]))
                                        """
                                        latUsar.append(vLatAux[j + randCoordInicial])
                                        lonUsar.append(vLonAux[j + randCoordInicial])

                                gerarJamCG(rua[randCidade], vLatAux, vLonAux, d , d2)

                print("")
                rua.clear()
                vlat.clear()
                vlon.clear()
            identificador += 1


def gerarJamCG(nomeRua, vLatAux, vLonAux, d, d2):

    id = gerarId()
    uuid = gerarUuid()
    pub_mills = str(gerarPubMillis())
    dt = gerarData(d, d2)
    data = str(dt)
    startNode = ""
    endNode = ""
    rt = gerarRoadType()
    city = "Joinville"
    country = "BR"
    delay = gerarDelay()
    speed = gerarSpeed()
    speedKMH = speed * 4.18
    length = gerarLength()
    turnType = "NONE"
    level = gerarLevel()
    blocking = ""
    lat = vLatAux
    lon = vLonAux
    line = gerarLineJamCG(lat, lon)
    street = nomeRua
    type1 = "NONE"
    turnLine = ""
    datafile_ID = str(gerarDFI())

    geomAux1 = ""
    if len(lat) == 1:
        geomAux1 = "SELECT ST_GeomFromText('POINT("
        latText = str(lat[0])
        lonText = str(lon[0])
        geomAux1 += latText
        geomAux1 += " "
        geomAux1 += lonText
        geomAux1 += ")', 4326)"
        print(geomAux1)
    else:
        geomAux1 = "SELECT ST_GeomFromText('LINESTRING("
        for x in range(len(lat)):
            if x != 0:
                geomAux1 += ","
                latText = str(lat[x])
                lonText = str(lon[x])
                geomAux1 += latText
                geomAux1 += " "
                geomAux1 += lonText
            else:
                latText = str(lat[x])
                lonText = str(lon[x])
                geomAux1 += latText
                geomAux1 += " "
                geomAux1 += lonText

        geomAux1 += ")', 4326)"
    # print(geomAux1)

    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()
    cur.execute(geomAux1)
    mobile_records = cur.fetchone()
    aux = ""
    palavra = str(mobile_records)
    for x in palavra:
        if x != "(" and x != "'" and x != "," and x != ")":
            aux += x

    req = "INSERT INTO waze.jams values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    values = [id, uuid, pub_mills, data, startNode, endNode, rt, street, city, country, delay, speed, speedKMH,
              length, turnType, level, blocking, line, type1, None, datafile_ID, aux]
    cur = con.cursor()
    cur.execute(req, values)
    con.commit()

    print("Id jam = " + id + " | tempo = " + data)

    # closing database connection.
    if (con):
        cur.close()
        con.close()
        print("PostgreSQL connection is closed\n\n")


def gerarLineJamCG(lat, lon):

    if len(lat) == 1:
        linestring = "["
        listaRetorno = [lat[0], lon[0]]
        linestring = gerarStringLocation(listaRetorno)
        linestring += "]"
        return linestring
    else:
        linestring = "["
        for j in range(len(lat)):
            if j != 0:
                linestring += ", "
            listaRetorno = [lat[j], lon[j]]
            linestring += gerarStringLocation(listaRetorno)
        linestring += "]"
        return linestring


def main():

    iniciarPrograma = int(
        input("1->Utilizar todas as ruas\n2->Dividir por celular geografica\n3->Fechar programa\n"))
    while iniciarPrograma == 1 or iniciarPrograma == 2:

        if iniciarPrograma == 1:
            op = 0
            while op > 3 or op < 1:
                op = int(input("1->Alerta\n2->Jam\n3->SAIR\n"))

            while op == 1 or op == 2:
                d = input(
                    "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 57 48 140):  ")
                d2 = input(
                    "Digite a data final separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 59 48 140):  ")

                if op == 1:
                    op2 = int(input("Digite a quantidade de Alertas : "))
                    gerarAlerta(op2, d, d2)

                if op == 2:
                    op2 = int(input("Digite a quantidade de Jams : "))
                    gerarJam(op2, d, d2)

                op = int(input("1->Alerta\n2->Jam\n3-SAIR\n"))
                while op > 3 or op < 1:
                    op = int(input("1->Alerta\n2->Jam\n3->SAIR\n"))

        elif iniciarPrograma == 2:
            geograficCell()

        iniciarPrograma = int(input("1->Utilizar todas as ruas\n2->Dividir por celular geografica\n3->Fechar programa\n"))


main()
