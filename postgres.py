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
import os.path

vetor = ['a', 'b', 'c', 'd', 'e', 'f', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] #letras e numeros usados pelo BD waze em alguns campos
type = ["ROAD_CLOSED", "ACCIDENT", "WEATHERHAZARD", "JAM"] # opcoes do campo "type" do bd do waze. O subtype eh a mesma ideia
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

road_type = [7, 0, 1, 5, 4, 2, 6, 17, 20] #todos os valores possivels da coluna "road_tyoe" do bd do waze

#todos as funcoes "gerar..." sao para gerar dados de forma randomica para introduzir no BD
def gerarAlerta(quantidade, d, d2, path):

    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

    for x in range(quantidade):

        id = gerarId()
        uuid = gerarUuid()
        pub_mills = str(gerarPubMillis())
        dt = gerarData(d, d2)
        data = str(dt)
        rt = str(gerarRoadType())
        listaRetorno = gerarLocation(path)
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

        cur.execute(geomAux1)
        mobile_records = cur.fetchone()
        aux = ""
        palavra = str(mobile_records)
        for x in palavra:
            if x != "(" and x != "'" and x != "," and x != ")":
                aux += x

        req = "INSERT INTO waze.alerts values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        values = [id, uuid, pub_mills, data, rt, location, street, city, country, magvar, reliability,
                  report_description,
                  reportRating, confidence, type1, st, reportBy, thumbs, jammuui, datafile_ID, aux]
        cur = con.cursor()
        cur.execute(req, values)
        con.commit()

        print("Id alerta = " + id + " | tempo = " + data)

    # closing database connection.
    if con :
        cur.close()
        con.close()
        print("Conexao com o PostgreSQL fechada\n\n")


def gerarJam(quantidade, d, d2, path):


    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

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
    if con:
        cur.close()
        con.close()
        print("PostgreSQL connection is closed\n\n")


def gerarLine(path):

    arquivo = os.path.join(path, "Ruas&CoordenadasOrdenadas.txt")
    nomeRuas = os.path.join(path, "Ruas.txt")

    lat = []
    lon = []

    cont = 0
    quantRuas = quantidadeRuas(path)
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


def gerarLocation(path):

    quantRuas = quantidadeRuas(path)
    listaRetorno = []
    arquivo = os.path.join(path, "Ruas&Coordenadas.txt")
    nomeRuas = os.path.join(path, "Ruas.txt")
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

#vai criar um csv contendo os limites (lat e lon) de cada celula geografica
def geograficCell(coord, path):

    print("Iniciando funcao : geograficCell")
    latTopLeft = coord[0]
    lonTopLeft = coord[1]
    latBottomRight = coord[2]
    lonBottomRight = coord[3]

    quantLat = input("Digite a quantidade LINHAS : ")
    quantLon = input("Digite a quantidade COLUNAS : ")
    quantLatStr = quantLat
    quantLonStr = quantLon
    quantLat = int(quantLat)
    quantLon = int(quantLon)
    quantCelulas = quantLat * quantLon
    print("Quantidade TOTAL de CELULAS GEOGRAFICAS = %d" % quantCelulas)

    tamLat = -1 * ((latTopLeft - latBottomRight) / quantLat)  # definindo o tamanho total da latitude no mapa
    tamLon = -1*((lonTopLeft - lonBottomRight)/quantLon) #definindo o tamanho total da longitude no mapa


    #utilizados na funcao registrosPorCG
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
    nomeCompleto = os.path.join(path, tabelaCSV)
    with open(nomeCompleto, 'w', newline='\n', encoding='utf-8') as csvFile: #escrevendo na tabela
        writer = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvData = ['left', 'top', "right", "bottom", "id"]
        # Serao necessarios 2 conjuntos de coordenadas. (left, top) e (right, bottom), alem do id.
        #(left, top) sao as coordenadas superiores da esquerda de um retangulo.
        # (right, bottom) sao as coordenadas inferiores da direita de um retangulo
        writer.writerow(csvData)
        for i in range(quantLon):
            for j in range(quantLat):
                left = latTopLeft + (j*tamLat)
                top = lonTopLeft + (i * tamLon)
                right = left + tamLat
                bottom = top + tamLon
                id = cont
                cont += 1
                writer.writerow(["%.6f" % left, "%.6f" % top , "%.6f" % right, "%.6f" % bottom , "%d" % id])
    print("Terminando funcao : geograficCell")
    bitMap(tabela, auxList, path)



#vai criar um bimap de cada rua, onde para cada celula geografica, a rua em questao tera valores 0 (nao esta presente na CG) ou 1 (esta presente na CG)
def bitMap(tabela, auxList, path):

    print("Iniciando a funcao : bitMap")
    # colocando as ruas em uma lista
    vRuas = []
    localizacao = []
    cg = []

    nomeCompleto = os.path.join(path, "Ruas&Coordenadas.txt")
    with open(nomeCompleto, encoding="utf8", errors='ignore') as txt_reader:
        line = txt_reader.readline()
        while line:
            if not line.startswith("-"):
                vRuas.append(line.rstrip('\n'))
            line = txt_reader.readline()

    # pegando a quantidade de celulas geografica
    quantCelulas = -1  # pq tem a linha com os nomes dos campos
    tabelaCSV = tabela + ".csv"
    tabelaNomeCompleto = os.path.join(path, tabelaCSV)
    with open(tabelaNomeCompleto, encoding="utf8", errors='ignore') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            quantCelulas += 1

    # lembrar que as celulas geograficas comecam no 1
    readingCsvParameters = []
    readingCsvParameters.append(tabelaCSV)
    readingCsvParameters.append(" ")
    for r in vRuas:
        readingCsvParameters[1] = r
        localizacao = readingCSV(readingCsvParameters, path)
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
        ruasBitMapNomeCompleto = os.path.join(path, nameFile)
        with open(ruasBitMapNomeCompleto, "a") as writter:
            writter.write("%s\n" % r)
            for i in l1:
                writter.write("%d " % i)
            writter.write("\n\n")
    print("Terminando a funcao : bitMap")
    print("Iniciando a funcao : gerador")
    gerador(auxList, path)


#usuario vai escolher alerta ou jams
def gerador(auxList, path):
    op = int(input("Digite:\n1->ALERTA\n2->JAM\n3->SAIR\n"))

    op2 = int(input("Digite a quantidades de registros de ocorrências por CG's : "))
    auxList.append(op2)
    listaInput = input("Digite as GC`s em ordem crescente separados por espaco: ")
    listaGC = listaInput.split()

    if op == 1:
        alertaRegistrosPorGcEspecifico(auxList, listaGC, path)

    elif op == 2:
        ordenarCoord(path)
        jamRegistrosPorGcEspecifico(auxList, listaGC, path)


#gera alertas em um conjunto de CG`s especificado pelo usuario
def alertaRegistrosPorGcEspecifico(auxList, listaGC, path):

    #conexao com o BD
    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

    d = input(
        "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 57 48 140):  ")
    d2 = input(
        "Digite a data final separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 59 48 140):  ")

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

    #transformando a lista dos indices dos CG's para uma lista de inteiros inteiro
    listaAux = []
    for x in range(len(listaGC)):
        aux = int(listaGC[x])
        listaAux.append(aux)

    #vai indo de CG em CG
    for i in range(quantLon):
        for j in range(quantLat):

            #se o identificador é igual a algum valor da lista de CG's. Identificador++ a cada final de loop
            if identificador in listaAux:
                #calculo para determinar as latitudes e longitudes da limitantes da CG
                left = latTopLeft + (j * tamLat)
                top = lonTopLeft + (i * tamLon)
                right = left + tamLat
                bottom = top + tamLon
                rua = []
                vlat = []
                vlon = []

                nomeCompleto = os.path.join(path, "Ruas&Coordenadas.txt")
                with open(nomeCompleto, encoding="utf8", errors='ignore') as txt_reader:
                    line = txt_reader.readline()
                    while line:
                        if not line.startswith("-"): #se nao comeca com "-", entao a linha é um nome
                            nomeRua = line
                        elif line.startswith("-"): # a linha é uma coordenada
                            aux = line.split()
                            lat = float(aux[0])
                            lon = float(aux[1])
                        line = txt_reader.readline()

                        #se a latitude e longitude esta contida na CG, entao a rua faz parte da CG
                        if lat >= left and lat < right:
                            if lon <= top and lon > bottom:
                                rua.append(nomeRua)
                                vlat.append(lat)
                                vlon.append(lon)

                if len(rua) == 0:
                    print("GC %d vazia" % identificador)
                else:
                    print("GC %d " % identificador)
                    for x in range(quantidade):
                        #geracao aleatoria dos dados para introduzir no BD
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

                        #geomAux1 vai conter o select que é usado para realizar a funcao ST_GeomFromText, que pega uma linestring e transforma em geom
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


                        #insere no BD
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


def readingCSV(readingCsvParameters, path):

    highwayCoords = []
    nomeCompleto = os.path.join(path, readingCsvParameters[0])
    with open(nomeCompleto, encoding="utf8", errors='ignore') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        # pegar as coordenadas da rua e colocar em uma lista
        nomeCompleto = os.path.join(path, "Ruas&Coordenadas.txt")
        with open(nomeCompleto, encoding="utf8", errors='ignore') as txt_reader:
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

#armazena o nome da rua e as coordenadas do jam (para a funcao gerarJamCG)
def jamRegistrosPorGcEspecifico(auxList, listaGC, path):
    d = input(
        "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 57 48 140):  ")
    d2 = input(
        "Digite a data final separado por espaco -> ano mes dia hora minuto segundo milissegundo (ex: 2020 12 7 21 59 48 140):  ")

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

    #transforma a lista de CG's digitado pelo usuario em uma lista de inteiros
    listaAux = []
    for x in range(len(listaGC)):
        aux = int(listaGC[x])
        listaAux.append(aux)

    #vai de CG em CG
    for i in range(quantLon):
        for j in range(quantLat):
            #verifica se a CG esta na lista digitada pelo usuario
            if identificador in listaAux:
                #calculo para definir limite da CG
                left = latTopLeft + (j * tamLat)
                top = lonTopLeft + (i * tamLon)
                right = left + tamLat
                bottom = top + tamLon
                rua = []
                vlat = []
                vlon = []

                nomeCompleto = os.path.join(path, "Ruas&CoordenadasOrdenadas.txt")
                with open(nomeCompleto, encoding="utf8", errors='ignore') as txt_reader:
                    line = txt_reader.readline()
                    while line:
                        if not line.startswith("-"):
                            nomeRua = line
                        elif line.startswith("-"):
                            aux = line.split()
                            lat = float(aux[0])
                            lon = float(aux[1])

                        line = txt_reader.readline()
                        #verifica se a lat e a lon da rua pertence a CG, se sim, insere a coordenada nos vetores
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

                        #loop com total de jams digitada pelo usuario
                        for x in range(quantidade):
                            randCidade = 0
                            if len(rua) > 1:
                                randCidade = randrange(0, len(rua) - 1)  # faixa de inteiro
                            vLatAux = []
                            vLonAux = []
                            latUsar = []
                            lonUsar = []

                            for c in range(len(rua)):
                                if rua[c] == rua[randCidade]:
                                    vLatAux.append(vlat[c])
                                    vLonAux.append(vlon[c])

                            if len(vLatAux) == 1:
                                gerarJamCG(rua[randCidade], vLatAux, vLonAux, d, d2)

                            else:
                                randNumeroDeCoordenadas = randrange(1, len(vLatAux))
                                #se o numero randomico de quantas coordenadas vao ter no jam for igual ao total de coordenadas que tem o vetor de coordenadas da rua
                                if randNumeroDeCoordenadas == len(vLatAux):
                                    for a in range(len(vLatAux)):
                                        latUsar.append(vLatAux[a])
                                        lonUsar.append(vLonAux[a])

                                else:
                                    randCoordInicial = randrange(0, len(vLatAux) - randNumeroDeCoordenadas)
                                    #as coordenadas estao ordenadas, por isso a partir do primeiro ponto de jam, deve ser sequencial
                                    #ex : coord (1,2,3,4,5,6,7), se o jam começar na coord 5 e tiver 3 pontos de engarramento, entao as coords 5,6,7 estarao no jam
                                    for b in range(randNumeroDeCoordenadas):
                                        latUsar.append(vLatAux[b + randCoordInicial])
                                        lonUsar.append(vLonAux[b + randCoordInicial])

                                gerarJamCG(rua[randCidade], vLatAux, vLonAux, d, d2)

                print("")
                rua.clear()
                vlat.clear()
                vlon.clear()
            identificador += 1


#introduz o jam no BD
def gerarJamCG(nomeRua, vLatAux, vLonAux, d, d2):

    #cria os valores aleatorios para a tabela jam
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
    #se for só 1 ponto no jam
    if len(lat) == 1:
        geomAux1 = "SELECT ST_GeomFromText('POINT("
        latText = str(lat[0])
        lonText = str(lon[0])
        geomAux1 += latText
        geomAux1 += " "
        geomAux1 += lonText
        geomAux1 += ")', 4326)"
        print(geomAux1)

    #se for varios pontos
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


#gera a string de latitudes e longitudes no formato do BD
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


#cria uma nova pasta para armazenar os dados
def criarNovoDiretorio(nome):
    # define the name of the directory to be created
    path = "/home/lucas/PycharmProjects/ic2/" + nome
    try:
        os.mkdir(path)
        return path
    except OSError:
        print("Nao foi possivel criar a pasta")
        exit(0)


#ler o geojson e cria o arquivo Ruas&Coordenadas, onde esta todas as ruas e as coordenadas
def lerGeojson(nomeCompletoGeojson, path):

    #nomeCompletoGeojson é o caminho do geojson + nome do arquivo (/home/lucas/PycharmProjects/ic2/geojson/arquivo.geojson)
    #faco isso porque o geojson esta em uma pasta diferente do postgres.py

    print("Iniciando funcao : lerGeojson")
    escolha = 0
    while escolha > 2 or escolha < 1:
        escolha = int(input("1->Utilizar todo geojson\n2->Definir parametros\n"))

    #O usuario escolheu usar todos os dados do geojson (Vai ter ruas, avenidas, rodovias...)
    if escolha == 1:
        print("Criando arquivo : Ruas&Coordenadas.txt")
        vnome = []
        with open(nomeCompletoGeojson, encoding="utf8", errors='ignore') as f:
            data = geojson.load(f)

        #essa parte pega todas as cordenadas da rua
        for feature in data['features']:
            try:
                nome = feature['properties']['name']
                coordinates = feature['geometry']['coordinates']
                if not nome.startswith("-"):
                    if nome not in vnome:
                        vnome.append(nome)
                        for feature1 in data['features']:
                            try:
                                nome1 = feature1['properties']['name']
                                coordinates1 = feature1['geometry']['coordinates']
                                if nome == nome1:
                                    for i in range(len(coordinates1)):
                                        if coordinates1[i] not in coordinates:
                                            coordinates.append(coordinates1[i])

                            except:
                                pass

                        #printa no arquivo o nome da rua + todas as coordenadas dela
                        nomeCompleto = os.path.join(path, "Ruas&Coordenadas.txt")
                        with open(nomeCompleto, "a") as writter:
                            writter.write("%s\n" % nome)
                            # print("%s" % nome)
                            for i in range(len(coordinates)):
                                try:
                                    # print("%.6lf , %.6lf" % (coordinates[i][0], coordinates[i][1]))
                                    writter.write("%f %f" % (coordinates[i][0], coordinates[i][1]))
                                    writter.write("\n")
                                except:
                                    # print("%.6lf , %.6lf" % (coordinates[i], coordinates[i+1]))
                                    writter.write("%lf %lf" % (coordinates[i], coordinates[i + 1]))
                                    writter.write("\n")
                                    i = i + 1

            except:
                pass


    else:
        #o usuario definiu parametros na utilizacao do geojson
        #pega o nome e as coordenadas e printa no arquivo Ruas&Coordenadas
        vnome = []
        with open(nomeCompletoGeojson, encoding="utf8", errors='ignore') as f:
            data = geojson.load(f)

        p = input("Digite os parametros (ex : Rua Avenida Servidão Rodovia) : ")
        print("Criando arquivo : Ruas&Coordenadas.txt")
        parametros = p.split()

        for x in range(len(parametros)):
            vnome = []
            for feature in data['features']:
                try:
                    nome = feature['properties']['name']
                    coordinates = feature['geometry']['coordinates']
                    if nome.startswith(parametros[x]):
                        if nome not in vnome:
                            vnome.append(nome)
                            for feature1 in data['features']:
                                try:
                                    nome1 = feature1['properties']['name']
                                    coordinates1 = feature1['geometry']['coordinates']
                                    if nome == nome1:
                                        for i in range(len(coordinates1)):
                                            if coordinates1[i] not in coordinates:
                                                coordinates.append(coordinates1[i])

                                except:
                                    pass

                            nomeCompleto = os.path.join(path, "Ruas&Coordenadas.txt")
                            with open(nomeCompleto, "a") as writter:
                                writter.write("%s\n" % nome)
                                # print("%s" % nome)
                                for i in range(len(coordinates)):
                                    try:
                                        # print("%.6lf , %.6lf" % (coordinates[i][0], coordinates[i][1]))
                                        writter.write("%f %f" % (coordinates[i][0], coordinates[i][1]))
                                        writter.write("\n")
                                    except:
                                        # print("%.6lf , %.6lf" % (coordinates[i], coordinates[i+1]))
                                        writter.write("%lf %lf" % (coordinates[i], coordinates[i + 1]))
                                        writter.write("\n")
                                        i = i + 1

                except:
                    pass


#essa funcao vai pegar o limite da latitude e longitude do geojson
def getLatLongMaxMin(path):

    latMaior = -50.0
    lonMaior = -50.0
    latMenor = 1.0
    lonMenor = 1.0

    nlatMaior = ""
    nlonMaior = ""
    nlatMenor = ""
    nlonMenor = ""

    arq = os.path.join(path, "Ruas&Coordenadas.txt")
    with open(arq, "r", encoding="utf8", errors='ignore') as txt_reader:
        line = txt_reader.readline() # lendo cada linha do txt, procurando a Latitude e Longitude maior e menor
        nome = ""
        while line:
            if not line.startswith("-"):
                nome = line
            if line.startswith("-"):
                aux = line.split()
                lat = float(aux[0])
                lon = float(aux[1])

                if lat > latMaior:
                    latMaior = lat
                    nLatMaior = nome
                if lat < latMenor:
                    latMenor = lat
                    nLatMenor = nome
                if lon < lonMenor:
                    lonMenor = lon
                    nLonMenor = nome
                if lon > lonMaior:
                    lonMaior = lon
                    nLonMaior = nome

            line = txt_reader.readline()

        coord = []
        coord.append(latMenor)
        coord.append(lonMaior)
        coord.append(latMaior)
        coord.append(lonMenor)

        return coord

#cria o arquivo Ruas&CoordenadasOrdenadas, onde as coordenadas das ruas estao ordenadas
def ordenarCoord(path):

    nomeCompleto = os.path.join(path, "Ruas&Coordenadas.txt")
    nome = " "
    with open(nomeCompleto, encoding="utf8", errors='ignore') as txt_reader:
        line = txt_reader.readline()
        while line:
            if not line.startswith("-"):
                nome = line
                line = txt_reader.readline()
            if line.startswith("-"):
                vLat = []
                vLon = []
                while line.startswith("-"):
                    aux = line.split()
                    lat = float(aux[0])
                    lon = float(aux[1])
                    vLat.append(lat)
                    vLon.append(lon)
                    line = txt_reader.readline()

                ok = False
                while not ok:
                    ok = True
                    for i in range(len(vLat) - 1):
                        if vLat[i] < vLat[i+1] and vLon[i] > vLon[i+1]:
                            vLat[i], vLat[i+1], vLon[i], vLon[i+1] = vLat[i+1], vLat[i], vLon[i+1], vLon[i]
                            ok = False

                nomeCompleto = os.path.join(path, "Ruas&CoordenadasOrdenadas.txt")
                with open(nomeCompleto, "a") as writter:
                    writter.write("%s" % nome)
                    for x in range(len(vLat)):
                        writter.write("%.6f %.6f\n" % (vLat[x], vLon[x]))

#cria o arquivo Ruas, onde tem o nome de todas as ruas
def nomeRuas(path):

    arquivo = os.path.join(path, "Ruas&Coordenadas.txt")
    with open(arquivo) as txt_reader:
        line = txt_reader.readline()
        while line:
            if not line.startswith("-"):
                arquivoRua = os.path.join(path, "Ruas.txt")
                with open(arquivoRua, "a") as writter:
                    writter.write("%s" % line)
            line = txt_reader.readline()


#essa funcao analisa quantas ruas tem o geojson
def quantidadeRuas(path):

    arquivo = os.path.join(path, "Ruas.txt")
    i = 0
    with open(arquivo) as txt_reader:
        line = txt_reader.readline()
        while line:
            i += 1
            line = txt_reader.readline()
    return i

def main():

    escolha = int(input("1->NOVOS dados\n2->REUTILIZAR dados\n3->Fechar programa\n"))

    #novo ou reutilizar geojson
    while escolha == 1 or escolha == 2:
        path = ""
        if escolha == 1:
            nomeGeojson = input("Digite o nome do arquivo geojson: ")
            diretorio = '/home/lucas/PycharmProjects/ic2/geojson'
            nomeCompletoGeojson = os.path.join(diretorio, nomeGeojson + ".geojson")
            nomePasta = input("Digite o nome da pasta que deseja armazenar os arquivos: ")
            path = criarNovoDiretorio(nomePasta)
            lerGeojson(nomeCompletoGeojson, path)
            coord = getLatLongMaxMin(path)
            nomeRuas(path)
            ordenarCoord(path)

        else:
            nomeGeojson = input("Digite o nome do arquivo geojson: ")
            diretorio = '/home/lucas/PycharmProjects/ic2/geojson'
            nomeCompletoGeojson = os.path.join(diretorio, nomeGeojson + ".geojson")
            nomePasta = input("Digite o nome da pasta que estao os arquivos: ")
            diretorioPasta = '/home/lucas/PycharmProjects/ic2/'
            path = os.path.join(diretorioPasta, nomePasta)

        iniciarPrograma = 0
        while iniciarPrograma > 2 or iniciarPrograma < 1:
            iniciarPrograma = int(input("1->Dividir por celula geografica\n2->NAO Dividir por celular geografica\n"))

        # Dividir ou nao por celula geografica
        if iniciarPrograma == 1:
            coord = getLatLongMaxMin(path)
            geograficCell(coord, path)

        elif iniciarPrograma == 2:
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
                    gerarAlerta(op2, d, d2, path)

                if op == 2:
                    op2 = int(input("Digite a quantidade de Jams : "))
                    gerarJam(op2, d, d2, path)

                op = int(input("1->Alerta\n2->Jam\n3-SAIR\n"))
                while op > 3 or op < 1:
                    op = int(input("1->Alerta\n2->Jam\n3->SAIR\n"))

        escolha = int(input("\n\n\n1->Utilizar novo geojson\n2->Reutilizar geojson\n3->Fechar programa\n"))



main()
