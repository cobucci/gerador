# -*- coding: latin-1 -*-
from builtins import list

import requests
import json
import geojson
import csv
import os
from random import *
import time
from datetime import datetime
from datetime import timedelta


quantRuas = 3871
#nao estou utilizando, mas pode ser util no futuro
def readingFromGeojson():
    vnome = []
    #data = json.load(open(C:\Users\Lucas\PycharmProjects\gerador\venv\Lib, encoding='utf-8'))
    with open('allHighways.geojson', encoding="utf8", errors='ignore') as f:
        data = geojson.load(f)

    #nomesPossiveis = ["Rua", "Avenida"]
    for feature in data['features']:
        try:
            nome = feature['properties']['name']
            coordinates = feature['geometry']['coordinates']
            if nome.startswith("Rua") or nome.startswith("Avenida") or nome.startswith("Servidão"):
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
                    with open("Ruas&Coordenadas.txt", "a") as writter:
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
#nao estou utilizando, mas pode ser util no futuro
def queryWebSite():
    overpass_url = "http://overpass-api.de/api/interpreter"
    overpass_query = """
    [out:json];
    area["ISO3166-1"="DE"][admin_level=2];
    (node["amenity"="biergarten"](area);
     way["amenity"="biergarten"](area);
     rel["amenity"="biergarten"](area);
    );
    out center;
    """
    response = requests.get(overpass_url,
                            params={'data': overpass_query})
    data = response.json()

    for feature in data['features']:
        try:
            print(feature['properties']['name'])
            print(feature['geometry']['coordinates'])
            print("\n")
        except:
            pass

#criando o arquivo Ruas&Coordenadas
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

#funcao auxiliar para o nome das ruas
def nomeRuas():

    quantRuas=0
    with open("Ruas&Coordenadas.txt") as txt_reader:
        line = txt_reader.readline()
        while line:
            if not line.startswith("-"):
                with open("ruas.txt", "a") as writter:
                    writter.write("%s" % line)
                    quantRuas += 1
            line = txt_reader.readline()

    print(quantRuas)


def getLatLongMaxMin():

    latMaior = -50.0
    lonMaior = -50.0
    latMenor = 1.0
    lonMenor = 1.0

    nlatMaior = ""
    nlonMaior = ""
    nlatMenor = ""
    nlonMenor = ""

    with open("Ruas&Coordenadas.txt") as txt_reader:
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


        print("TOP LAT = %.6f %s" % (latMenor, nLatMenor))
        print("TOP LON = %.6f %s" % (lonMaior, nLonMaior))
        print("BOTTOM LAT = %.6f %s" % (latMaior, nLatMaior))
        print("BOTTOM LON = %.6f %s" % (lonMenor, nLonMenor))




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
    with open(tabelaCSV, 'w', newline='\n', encoding='utf-8') as csvFile: #escrevendo na tabela
        writer = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvData = ['left', 'top', "right", "bottom", "id"]
        # Ser?o necess?rios 2 conjuntos de coordenadas. (left, top) e (right, bottom), alem do id.
        #(left, top) s?o as coordenadas superiores da esquerda de um retangulo.
        # (right, bottom) s?o as coordenadas inferiores da direita de um retangulo
        writer.writerow(csvData)
        for i in range(quantLon):
            for j in range(quantLat):
                left = latTopLeft + (j*tamLat)
                top = lonTopLeft + (i * tamLon)
                right = left + tamLat
                bottom = top + tamLon
                id = cont
                cont += 1
                writer.writerow(["%.6f" % left , "%.6f" % top , "%.6f" % right, "%.6f" % bottom , "%d" % id])
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
    print("? necessario escolher entre ALERTAS ou CONGESTIONAMENTOS(JAMS) ou AMBOS")
    op = int(input("Digite:\n1->ALERTA\n2->JAM\n3->SAIR\n"))
    while op > 3 and op < 1:
        op = int(input("Digite:\n1->ALERTA\n2->JAM\n3->SAIR\n"))
    while op == 1 or op == 2:

        print("? NECESSARIO ESCOLHER UMA DAS OPCOES DE PARAMETRO PARA A CRIACAO DOS DADOS")
        ok1 = 0
        while (ok1 != 1):
            op1 = int(input(
                "Digite:\n1->Intervalo de tempo\n2->Quantidades de registros de ocorr?ncias por CGs\n3->Quantidade de registros de ocorr?ncias por CGs espec?ficos\n"))
            ok1 = 1

            # Intervalo de tempo
            if op1 == 1:
                op2 = int(input("Digite:\n1->Todos os CGs\n2->Conjunto de CGs:\n"))
                if op2 == 1:
                    op3 = int(input("Digite a quantidades de registros : "))
                    todosGCsTempoAlerta(op3)
                else:
                    op3 = int(input("Digite a quantidades de registros de ocorr?ncias por CG's : "))
                    auxList.append(op3)
                    listaInput = input("Digite as GC`s em ordem crescente separados por espaco: ")
                    listaGC = listaInput.split()
                    conjuntoEspecificoGCsTempoAlerta(listaGC, auxList)

            # Quantidades de registros de ocorr?ncias por CGs
            elif op1 == 2:
                op2 = int(input("Digite a quantidades de registros de ocorr?ncias por CG's : "))
                auxList.append(op2)
                if op == 1:
                    alertaRegistrosPorCG(auxList)
                else:
                    jamRegistrosPorCG(auxList)




            # Quantidade de registros de ocorr?ncias por CGs espec?ficos
            elif op1 == 3:
                op2 = int(input("Digite a quantidades de registros de ocorr?ncias por CG's : "))
                auxList.append(op2)
                listaInput = input("Digite as GC`s em ordem crescente separados por espaco: ")
                listaGC = listaInput.split()
                if op == 1:
                    alertaRegistrosPorGcEspecifico(auxList, listaGC)
                else:
                    jamRegistrosPorGcEspecifico(auxList, listaGC)

            else:
                ok1 = 0
                print("INVALIDO")
        op = int(input("Digite:\n1->ALERTA\n2->JAM\n3->SAIR\n"))
        while op > 3 and op < 1:
            op = int(input("Digite:\n1->ALERTA\n2->JAM\n3->SAIR\n"))


def alertaRegistrosPorCG(auxList):

    arquivo = "Ruas&Coordenadas.txt"
    id = 1
    lat = 0.0
    lon = 0.0

    latTopLeft = auxList[0]
    lonTopLeft = auxList[1]
    tamLat = auxList[2]
    tamLon = auxList[3]
    quantLat = auxList[4]
    quantLon = auxList[5]
    quantidade = auxList[6]

    for i in range(quantLon):
        for j in range(quantLat):
            left = latTopLeft + (j * tamLat)
            top = lonTopLeft + (i * tamLon)
            right = left + tamLat
            bottom = top + tamLon
            rua = []
            vlat = []
            vlon = []
            #print("%f %f | %f %f" % (left, top, right, bottom))

            with open(arquivo, encoding="utf8", errors='ignore') as txt_reader:
                line = txt_reader.readline()
                while line:
                    if not line.startswith("-"):
                        nomeRua = line
                        #print(nomeRua)
                    elif line.startswith("-"):
                        aux = line.split()
                        lat = float(aux[0])
                        lon = float(aux[1])
                        #print("%f - %f " % (lat, lon))
                    line = txt_reader.readline()

                    if lat >= left and lat < right:
                        if lon <= top and lon > bottom:
                            rua.append(nomeRua)
                            vlat.append(lat)
                            vlon.append(lon)

            #print("CG %d - %d" % (id, len(rua)))
            if len(rua) == 0:
                print("GC %d vazia" % id)
            else:
                print("GC %d " % id)
                for x in range(quantidade):
                    rand = randrange(0, len(rua)-1)  # faixa de inteiro
                    print("%s %f %f" % (rua[rand], vlat[rand], vlon[rand]))

            print("\n\n")
            id += 1
            rua.clear()
            vlat.clear()
            vlon.clear()

def alertaRegistrosPorGcEspecifico(auxList, listaGC):

    arquivo = "Ruas&Coordenadas.txt"
    id = 1
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
            if id in listaAux:
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
                    print("GC %d vazia" % id)
                else:
                    print("GC %d " % id)
                    for x in range(quantidade):
                        rand = randrange(0, len(rua)-1)  # faixa de inteiro
                        print("%s %f %f" % (rua[rand], vlat[rand], vlon[rand]))

                print("")
                rua.clear()
                vlat.clear()
                vlon.clear()
            id += 1

def registrosPorIntervaloTempo():


    d = input("Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo (ex: 2020 12 7 21 57 48):  ")
    t = int(input("Digite o intervalo de tempo em segundos: "))
    q = int(input("Digite a quantidade de interacoes: "))

    d = d.split()
    for x in range(len(d)):
        aux = d[x]
        aux = int(aux)
        d[x] = aux

    timestamp = datetime(year=d[0], month=d[1], day=d[2], hour=d[3], minute=d[4], second=d[5])
    d = timedelta(seconds=t)
    new_timestamp = timestamp

    for x in range(q):
        print(new_timestamp)
        new_timestamp += d


def todosGCsTempoAlerta(quantidade):


    arquivo = "Ruas&Coordenadas.txt"
    nomeRuas = "ruas.txt"
    #codificacao do tempo
    d = input("Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo (ex: 2020 12 7 21 57 48):  ")
    t = int(input("Digite o intervalo de tempo em segundos: "))
    d = d.split()
    for x in range(len(d)):
        aux = d[x]
        aux = int(aux)
        d[x] = aux

    timestamp = datetime(year=d[0], month=d[1], day=d[2], hour=d[3], minute=d[4], second=d[5])
    d = timedelta(seconds=t)
    new_timestamp = timestamp

    lat = []
    lon = []
    for x in range(quantidade):
        cont = 0
        rand = randrange(0, quantRuas-1)  # faixa de inteiro
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
                        rand1 = randrange(0, len(lat)-1)  # faixa de inteiro
                        print(new_timestamp)
                        new_timestamp += d
                        print("%s (%.6f , %.6f)\n" % (nome, lat[rand1], lon[rand1]))
                        break
                    else:
                        line1 = reader.readline()
                        nome = line1
                lat.clear()
                lon.clear()


def conjuntoEspecificoGCsTempoAlerta(listaGC, auxList):
    d = input(
        "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo (ex: 2020 12 7 21 57 48):  ")
    t = int(input("Digite o intervalo de tempo em segundos: "))
    print("")

    d = d.split()
    for x in range(len(d)):
        aux = d[x]
        aux = int(aux)
        d[x] = aux

    timestamp = datetime(year=d[0], month=d[1], day=d[2], hour=d[3], minute=d[4], second=d[5])
    d = timedelta(seconds=t)
    new_timestamp = timestamp

    arquivo = "Ruas&Coordenadas.txt"
    id = 1
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
            if id in listaAux:
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
                    print("GC %d vazia" % id)
                else:
                    print("GC %d " % id)
                    for x in range(quantidade):
                        rand = randrange(0, len(rua)-1)  # faixa de inteiro
                        print(new_timestamp)
                        new_timestamp += d
                        print("%s %f %f" % (rua[rand], vlat[rand], vlon[rand]))

                print("")
                rua.clear()
                vlat.clear()
                vlon.clear()
            id += 1


def ordenarCoord():
    arquivo = "Ruas&Coordenadas.txt"
    nome = " "
    with open(arquivo, encoding="utf8", errors='ignore') as txt_reader:
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

                with open("ruas&CoordenadasOrdenadas.txt", "a") as writter:
                    writter.write("%s" % nome)
                    for x in range(len(vLat)):
                        writter.write("%.6f %.6f\n" % (vLat[x], vLon[x]))

def jamRegistrosPorCG(auxList):

    """
    latTopLeft = -48.933195
    lonTopLeft = -26.139358
    tamLat = 0.068519
    tamLon = -0.098078
    quantLat = 3
    quantLon = 3
    quantidade = 5
    """

    arquivo = "ruas&CoordenadasOrdenadas.txt"
    id = 1
    lat = 0.0
    lon = 0.0


    latTopLeft = auxList[0]
    lonTopLeft = auxList[1]
    tamLat = auxList[2]
    tamLon = auxList[3]
    quantLat = auxList[4]
    quantLon = auxList[5]
    quantidade = auxList[6]


    for i in range(quantLon):
        for j in range(quantLat):
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
                    elif line.startswith("-"):
                        aux = line.split()
                        lat = float(aux[0])
                        lon = float(aux[1])
                    line = txt_reader.readline()

                    if lat >= left and lat < right:
                        if lon <= top and lon > bottom:
                            rua.append(nomeRua)
                            vlat.append(lat)
                            vlon.append(lon)

            #escolher as ruas
            if len(rua) == 0:
                print("GC %d vazia" % id)
            else:
                print("GC %d " % id)
                for x in range(quantidade):
                    randCidade = 0
                    if len(rua) > 1:
                        randCidade = randrange(0, len(rua)-1)  # faixa de inteiro
                    vLatAux = []
                    vLonAux = []
                    for i in range(len(rua)):
                        if rua[i] == rua[randCidade]:
                            vLatAux.append(vlat[i])
                            vLonAux.append(vlon[i])
                    if len(vLatAux) == 1:
                        print("REGISTRO %d - %s %f %f" % (x+1, rua[randCidade], vLatAux[0], vLonAux[0]))
                    else:
                        randNumeroDeCoordenadas = randrange(1, len(vLatAux))
                        if randNumeroDeCoordenadas >= len(vLatAux):
                            for j in range(len(vLatAux)):
                                print("REGISTRO %d - %s %f %f" % (x+1, rua[randCidade], vLatAux[j], vLonAux[j]))
                                #print("oi")
                        else:
                            randCoordInicial = randrange(0, len(vLatAux) - randNumeroDeCoordenadas)
                            for j in range(randNumeroDeCoordenadas):
                                print("REGISTRO %d - %s %f %f" % (x+1, rua[randCidade], vLatAux[j+randCoordInicial], vLonAux[j+randCoordInicial]))
            id += 1

def jamRegistrosPorGcEspecifico(auxList, listaGC):

    arquivo = "ruas&CoordenadasOrdenadas.txt"
    id = 1
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
            if id in listaAux:
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

                    # escolher as ruas
                    if len(rua) == 0:
                        print("GC %d vazia" % id)
                    else:
                        print("GC %d " % id)
                        for x in range(quantidade):
                            randCidade = 0
                            if len(rua) > 1:
                                randCidade = randrange(0, len(rua) - 1)  # faixa de inteiro
                            vLatAux = []
                            vLonAux = []
                            for i in range(len(rua)):
                                if rua[i] == rua[randCidade]:
                                    vLatAux.append(vlat[i])
                                    vLonAux.append(vlon[i])
                            if len(vLatAux) == 1:
                                print(
                                    "REGISTRO %d - %s %f %f" % (x + 1, rua[randCidade], vLatAux[0], vLonAux[0]))
                            else:
                                randNumeroDeCoordenadas = randrange(1, len(vLatAux))
                                if randNumeroDeCoordenadas == len(vLatAux):
                                    for j in range(len(vLatAux)):
                                        print("REGISTRO %d - %s %f %f" % (
                                        x + 1, rua[randCidade], vLatAux[j], vLonAux[j]))
                                        # print("oi")
                                else:
                                    randCoordInicial = randrange(0, len(vLatAux) - randNumeroDeCoordenadas)
                                    for j in range(randNumeroDeCoordenadas):
                                        print("REGISTRO %d - %s %f %f" % (
                                        x + 1, rua[randCidade], vLatAux[j + randCoordInicial],
                                        vLonAux[j + randCoordInicial]))

                print("")
                rua.clear()
                vlat.clear()
                vlon.clear()
            id += 1


def jamTodosGcTempo(quantidade):

    arquivo = "ruas&CoordenadasOrdenadas.txt"
    nomeRuas = "ruas.txt"
    # codificacao do tempo
    d = input(
        "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo (ex: 2020 12 7 21 57 48):  ")
    t = int(input("Digite o intervalo de tempo em segundos: "))
    d = d.split()
    for x in range(len(d)):
        aux = d[x]
        aux = int(aux)
        d[x] = aux

    timestamp = datetime(year=d[0], month=d[1], day=d[2], hour=d[3], minute=d[4], second=d[5])
    d = timedelta(seconds=t)
    new_timestamp = timestamp

    lat = []
    lon = []
    for x in range(quantidade):
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
                            print("REGISTRO %d - %s %f %f" % (x + 1, nome, lat[0], lon[0]))
                            print(new_timestamp)
                            new_timestamp += d
                        else:
                            randNumeroDeCoordenadas = randrange(1, len(lat))
                            if randNumeroDeCoordenadas == len(lat):
                                for j in range(len(lat)):
                                    print("REGISTRO %d - %s %f %f" % (x + 1, nome, lat[j], lon[j]))
                                    print(new_timestamp)
                                    new_timestamp += d


                            else:
                                randCoordInicial = randrange(0, len(lat) - randNumeroDeCoordenadas)
                                for j in range(randNumeroDeCoordenadas):
                                    print("REGISTRO %d - %s %f %f" % (
                                        x + 1, nome, lat[j + randCoordInicial],
                                        lon[j + randCoordInicial]))
                                    print(new_timestamp)
                                    new_timestamp += d
                        print("")
                        break
                    else:
                        line1 = reader.readline()
                        nome = line1
                lat.clear()
                lon.clear()


def jamConjuntoEspecificoTempo(listaGC, auxList):
    d = input(
        "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo (ex: 2020 12 7 21 57 48):  ")
    t = int(input("Digite o intervalo de tempo em segundos: "))
    print("")

    d = d.split()
    for x in range(len(d)):
        aux = d[x]
        aux = int(aux)
        d[x] = aux

    timestamp = datetime(year=d[0], month=d[1], day=d[2], hour=d[3], minute=d[4], second=d[5])
    d = timedelta(seconds=t)
    new_timestamp = timestamp

    arquivo = "ruas&CoordenadasOrdenadas.txt"
    id = 1
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
            if id in listaAux:
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
                    print("GC %d vazia" % id)
                else:
                    print("GC %d " % id)
                    for a in range(quantidade):
                        randEscolherRua = 0
                        if len(rua) > 1:
                            randEscolherRua = randrange(0, len(rua)-1)
                        ruaEscolhida = rua[randEscolherRua]
                        vLatAux = []
                        vLonAux = []
                        for x in range(len(rua)):
                            if ruaEscolhida == rua[x]:
                                vLatAux.append(vlat[x])
                                vLonAux.append(vlon[x])
                        randQuantidadeCoordenadas = 1
                        randCoordenadaInicial = 0
                        if len(vLatAux) > 1:
                            randQuantidadeCoordenadas = randrange(1, len(vLatAux))
                            randCoordenadaInicial = randrange(0, len(vLatAux) - randNumeroDeCoordenadas)
                        for x in range(randQuantidadeCoordenadas):
                            print("%s %f %f" % (ruaEscolhida, lat[x+ randCoordenadaInicial], lon[x + randCoordenadaInicial]))
                            print(new_timestamp)
                            new_timestamp += d

                print("")
                rua.clear()
                vlat.clear()
                vlon.clear()
            id += 1

def main():
    #geograficCell()
    #getLatLongMaxMin()
    #readingFromGeojson()
    #queryWebSite()
    #readingCSV()
    #benchmark()
    #alertaRegistrosPorCG()
    #alertaRegistrosPorGcEspecifico()
    #registrosPorIntervaloTempo()
    #nomeRuas()
    #todosGCsTempoAlerta(10)
    #ordenarCoord()
    #jamRegistrosPorCG(auxList)
    #jamTodosGcTempo(10)

main()

