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
from datetime import datetime
from datetime import timedelta
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


d = input(
        "Digite a data inicial separado por espaco -> ano mes dia hora minuto segundo (ex: 2020 12 7 21 57 48):  ")
t = int(input("Digite o intervalo de tempo em segundos: "))

road_type = [7, 0, 1, 5, 4, 2, 6, 17, 20]

def gerarAlerta(quantidade):


    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

    for x in range(quantidade):

        id = gerarId()
        #print(id)
        uuid = gerarUuid()
        #print(uuid)
        pub_mills = str(gerarPubMillis())
        dt = gerarData(d, t, 1+x)
        data = str(dt)
        #data = "2020-12-07 21:57:49"
        #print(data)
        rt = str(gerarRoadType())
        listaRetorno = gerarLocation()
        location = gerarStringLocation(listaRetorno)
        #print(location)
        street = listaRetorno[2]
        #print(street)
        city = "Joinville"
        country = "BR"
        magvar = str(gerarMagvar())
        reliability = str(gerarReliability())
        #print(reliability)
        report_description = ""
        reportRating = str(gerarReporRating())
        #print(reportRating)
        confidence = str(gerarConfidence())
        #print(confidence)
        type1 = gerarType()
        #print(t)
        st = gerarSubType()
        #print(st)
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
        #print(geomAux1)


        cur.execute(geomAux1)
        mobile_records = cur.fetchone()
        aux = ""
        palavra = str(mobile_records)
        for x in palavra:
            if x != "(" and x != "'" and x != "," and x != ")":
                aux += x

        #print(aux)


        req = "INSERT INTO waze.alerts values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        values = [id, uuid, pub_mills, data, rt, location, street, city, country, magvar, reliability, report_description,
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


def gerarJam(quantidade):

    for x in range(quantidade):

        lat = []
        lon = []
        id = gerarId()
        uuid = gerarUuid()
        pub_mills = str(gerarPubMillis())
        dt = gerarData(d, t, 1 + x)
        data = str(dt)
        startNode = ""
        endNode = ""
        rt = gerarRoadType()
        city = "Joinville"
        country = "BR"
        delay = gerarDelay()
        speed = gerarSpeed()
        speedKMH = speed*4.18
        #print(speed, speedKMH)
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
                    #SELECT ST_GeomFromText('LINESTRING(-71.160281 42.258729,-71.160837 42.259113,-71.161144 42.25932)',4269);
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
        #print(geomAux1)


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
        values = [id, uuid, pub_mills, data, startNode, endNode, rt, street, city, country, delay, speed, speedKMH, length, turnType, level, blocking, line, type1, None, datafile_ID, aux]
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
                                    linestring = linestring +  ", "
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
    r = randint (1, 5)
    return r

def gerarLength():
    r = randint(14 ,25273)
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

def gerarData(d, t, cont):

    d = d.split()
    for x in range(len(d)):
        aux = d[x]
        aux = int(aux)
        d[x] = aux

    timestamp = datetime(year=d[0], month=d[1], day=d[2], hour=d[3], minute=d[4], second=d[5])
    d = timedelta(seconds=t)
    new_timestamp = timestamp

    for x in range(cont):
        new_timestamp += d

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
        with open(arquivo,encoding="utf8", errors='ignore') as reader:
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
                    #print("%s (%.6f , %.6f)\n" % (nome, lat[rand1], lon[rand1]))
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
    #print(l1)
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

#nao to usando, mas pode ser util
def conectar():
    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()
    cur.execute("SELECT ST_GeomFromText('POINT(-71.064544 42.28787)')")
    geom = cur.fetchone()
    print(geom)
    con.close()

    """
     con = psycopg2.connect(host='localhost', database='waze',
                            user='postgres', password='lucas')
     cur = con.cursor()
     sql = "insert into cidade values (default,'Joinville','SC')"
     cur.execute(sql)
     con.commit()
     con.close()



     import psycopg2
     con = psycopg2.connect(host='localhost', database='regiao',
     user='postgres', password='postgres123')
     cur = con.cursor()
     sql = 'create table cidade (id serial primary key, nome varchar(100), uf varchar(2))'
     cur.execute(sql)
     sql = "insert into cidade values (default,'S?o Paulo,'SP')"
     cur.execute(sql)
     con.commit()
     cur.execute('select * from cidade')
     recset = cur.fetchall()
     for rec in recset:
     print (rec)
     con.close()
     """


def main():
    op = int(input("1->Alerta\n2->Jam\n3->SAIR\n"))
    while op == 1 or op == 2:
        if op == 1:
            op2 = int(input("Digite a quantidade de Alertas : "))
            gerarAlerta(op2)

        if op == 2:
            op2 = int(input("Digite a quantidade de Jams : "))
            gerarJam(op2)

        op = int(input("1->Alerta\n2->Jam\n3-SAIR\n"))


main()

