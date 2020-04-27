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
import pandas as pd

idDuplicados = []

def conexao():

    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()
    query = "select street, pub_utc_date, line , id from waze.jams limit 500;"
    cur.execute(query)
    retornoBD = cur.fetchall()
    dadosEmArquivos(retornoBD)
    retirarDuplicados()


def dadosEmArquivos(retornoBD):
    diretorio = '/home/lucas/PycharmProjects/ic2/bancoDados'
    for i in range(len(retornoBD)):
        try:
            for root, dirs, files in os.walk(diretorio):
                if retornoBD[i][0]+".csv" in files:
                    try:
                        nomeCompletoArquivo = os.path.join(diretorio, retornoBD[i][0] + ".csv")
                        with open(nomeCompletoArquivo, 'a', newline='\n', encoding='utf-8') as csvFile:
                            writer = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                            data = str(retornoBD[i][1])
                            line = str(retornoBD[i][2])
                            id = str(retornoBD[i][3])
                            csvData = [data, line, id]
                            writer.writerow(csvData)
                    except:
                        continue
                else:
                    try:
                        nomeCompletoArquivo = os.path.join(diretorio, retornoBD[i][0] + ".csv")
                        with open(nomeCompletoArquivo, 'w', newline='\n', encoding='utf-8') as csvFile:
                            writer = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                            csvData = ["date", "line", "id"]
                            writer.writerow(csvData)
                            data = str(retornoBD[i][1])
                            line = str(retornoBD[i][2])
                            id = str(retornoBD[i][3])
                            csvData = [data, line, id]
                            writer.writerow(csvData)
                    except:
                        continue

        except:
            continue

def retirarDuplicados():

    diretorio = '/home/lucas/PycharmProjects/ic2/bancoDados'
    listaArquivos = []
    for root, dirs, files in os.walk(diretorio):
        listaArquivos = files


    for i in range(len(listaArquivos)):

        nomeRua = listaArquivos[i]
        nomeCompletoArquivo = os.path.join(diretorio, nomeRua)
        data = []
        line = []
        id = []
        with open(nomeCompletoArquivo, newline='\n', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            sinal = 0
            for row in reader:
                if sinal == 1:
                    data.append(row[0])
                    line.append(row[1])
                    id.append(row[2])
                sinal = 1

        #print(nomeRua)
        #print(data)
        indiceDosDuplicados, indicePermanecente = getIDdosDuplicados(data)
        #print(indicePermanecente)
        #print(indiceDosDuplicados)
        """
        
        """
        #if len(indiceDosDuplicados) > 0:
            #print(len(indiceDosDuplicados))
            #analisarCoordenada(indiceDosDuplicados, indicePermanecente, data, line, id)

        #1 segundo a mais

        for j in range(len(data)):
            d = data[j]
            d = d.split()


        dia = d[0]
        hr = d[1]
        #gerarData(dia, hr)
        #print(timestamp)



def gerarData(dia, hora):

    d = []
    year = ""
    mes = ""
    day = ""
    i = 0
    print(dia)

    while dia[i] != "-":
        year += dia[i]
        i += 1
    d.append(year)
    #print(year)

    i += 1
    while dia[i] != "-":
        mes += dia[i]
        i += 1
    d.append(mes)
    #print(mes)

    i += 1
    while dia[i] != "-":
        day += dia[i]
        i += 1
        if i == len(dia):
            break
    d.append(day)

    #print(year)
    #print(mes)
    #print(day)


    print(hora)
    h = ""
    h += hora[0]
    h += hora[1]
    m = ""
    m += hora[3]
    m += hora[4]
    s = ""
    s += hora[6]
    s += hora[7]
    mic = ""
    for i in range(9, len(hora)):
        mic += hora[i]

    d.append(h)
    d.append(m)
    d.append(s)
    d.append(mic)
    #print(d)

    for i in range(len(d)):
        aux = d[i]
        aux = int(aux)
        d[i] = aux

    #print(d)

    timestamp = datetime(year=d[0], month=d[1], day=d[2], hour=d[3], minute=d[4], second=d[5], microsecond=d[6])
    for x in range(0, 100000):
        mili = timedelta(microseconds=x)
        new_timestamp = timestamp
        new_timestamp += mili
        #print(new_timestamp)


def getIDdosDuplicados(lista):

    indiceDosDuplicados = []
    indicePermanecente = []
    #print("tamanho = %d" % len(lista))
    for i in range(len(lista)):
        for j in range(i+1, len(lista)):
            if lista[i] == lista[j] and j not in indiceDosDuplicados:
                indicePermanecente.append(i)
                indiceDosDuplicados.append(j)

    #print(indicePermanecente)
    #print(indiceDosDuplicados)
    return indiceDosDuplicados, indicePermanecente


def analisarCoordenada(indiceDosDuplicados, indicePermanecente, data, line, id):

    """
        [2, 4, 4, 5, 5, 7, 16, 21, 25, 33]
        [3, 19, 20, 12, 24, 9, 17, 22, 26, 35]
    """

    indiceQueJaFoi = []
    for i in range(len(indicePermanecente)):

        if i not in indiceQueJaFoi:
            indiceVez = indicePermanecente[i]
            #pegarCoordenadas permanente
            vplatline = []
            vplonline = []
            vplatline, vplonline = pegarCoordenada(line[indicePermanecente[i]])

            #pegarCoordenadas duplicada
            vdlatline = []
            vdlonline = []
            vdlatline, vdlonline = pegarCoordenada(line[indiceDosDuplicados[i]])

            #print(vplatline)
            #print(vplonline)
            #print(vdlatline)
            #print(vdlonline)

            indiceQueJaFoi.append(i)
            #vendo se tem alguma coordenada diferente no permanente e no duplicado
            for x in range(len(vdlatline)):
                verificarSeEstaNaLista(vplatline, vplonline, vdlatline, vdlonline)

            #procuro outro indice no vetor de permanentes igual ao da vez
            for j in range(i+1, len(indicePermanecente)):
                #pegar da outra galera
                if indicePermanecente[j] == indiceVez:
                    vdlatline = []
                    vdlonline = []
                    vdlatline, vdlonline = pegarCoordenada(line[indiceDosDuplicados[j]])
                    #print(vdlatline)
                    #print(vdlonline)

                #vendo se tem alguma coordenada diferente no permanente e no duplicado
                for x in range(len(vdlatline)):
                    verificarSeEstaNaLista(vplatline, vplonline, vdlatline, vdlonline)

                indiceQueJaFoi.append(j)


            #print("*")
            print(vplatline)
            print(vplonline)
            #print("------")
            if len(vplatline) > 0:
                linestring = criarLine(vplatline, vplonline)
                print(linestring)


def pegarCoordenada(line):

    vetorNumeros = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "."]
    lat = ""
    lon = ""
    i = 0
    vlat = []
    vlon = []

    sinal = 0
    #se sinal = 0, entao eh lat, se for 1 = lon
    #print(len(line))
    for i in range(len(line)):
        # [{'x': -48.850532, 'y': -26.334066}, {'x': -48.848051, 'y': -26.334057}, {'x': -48.847528, 'y': -26.334045}, {'x': -48.845524, 'y': -26.334038}]
        if line[i] == "-":

            #lat
            if sinal == 0:
                lat += line[i]
                i += 1
                while line[i] in vetorNumeros:
                    lat += line[i]
                    i += 1
                auxLat = str(lat)
                vlat.append(auxLat)
                #print(auxLat)
                sinal = 1
                lat = ""

            #lon
            else:
                lon += line[i]
                i += 1
                while line[i] in vetorNumeros:
                    lon += line[i]
                    i += 1
                auxLon = str(lon)
                vlon.append(auxLon)
                #print(auxLon)
                sinal = 0
                lon = ""

        else:
            i += 1
    #print(vlat)
    #print(vlon)

    return vlat, vlon


def verificarSeEstaNaLista(vlat, vlon, lat, lon):

    if lat not in vlat or lon not in vlon:
        vlat.append(lat)
        vlon.append(lon)
    return vlat, vlon

    #[{'x': -48.850532, 'y': -26.334066}, {'x': -48.848051, 'y': -26.334057}, {'x': -48.847528, 'y': -26.334045}, {'x': -48.845524, 'y': -26.334038}]

def criarLine(lat, lon):

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
            #print("%d - %d - %d" % (len(lat), j , len(lon)))
            #print("%s - %s" % (lat[j], lon[j]))
            listaRetorno = [lat[j], lon[j]]
            linestring += gerarStringLocation(listaRetorno)
        linestring += "]"
        return linestring


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


conexao()
#pegarCoordenada("[{'x': -48.850532, 'y': -26.334066}, {'x': -48.848051, 'y': -26.334057}, {'x': -48.847528, 'y': -26.334045}, {'x': -48.845524, 'y': -26.334038}]")


