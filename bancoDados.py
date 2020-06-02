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
from postgres import *
from dateutil.relativedelta import relativedelta
from datetime import datetime


def conexao():


    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()
    query = "select street from waze.jams limit 10"
    cur.execute(query)
    retornoBD = cur.fetchall()
    ruas = []
    ruasC = []
    for x in retornoBD:
        x = str(x)
        ruas.append(x)

    for x in ruas:
        if x != '':
            sp0 = x.split("(")
            sp1 = sp0[1]
            sp1 = sp1.split(",)")
            nr = str(sp1[0])
            if nr not in ruasC:
                ruasC.append(nr)

    for x in ruasC:
        print("comecei Jams - " + x)
        query = "select pub_utc_date, geom from waze.jams where street = "
        query += x
        query += " limit 1000"
        cur = con.cursor()
        cur.execute(query)
        retornoJams = cur.fetchall()

        print("comecei Alerts - " + x)
        query = "select pub_utc_date, geom from waze.alerts where street = "
        query += x
        query += " limit 1000"
        cur = con.cursor()
        cur.execute(query)
        retornoAlerts = cur.fetchall()
        print("comecei ST_Intersects - " + x)
        for i in retornoJams:
            for j in retornoAlerts:
                query = "select ST_Intersects('"
                query += str(i[1])
                query += "','"
                query += str(j[1])
                query += "')"
                #cur = con.cursor()
                cur.execute(query)
                result = cur.fetchone()
                if result[0]:
                    d1 = str(i[0])
                    d2 = str(j[0])
                    if d1[:7] == d2[:7]:
                        print("%s = %s - %s" % (x, str(i[0]), str(j[0])))
        print("Terminei - %s\n\n" % x)







    #print(ruas)
    #dadosEmArquivos(retornoBD)
    # queryAJ(retornoBD)
    # retirarDuplicados()

    """
    quantTuplas = input("Digite a quantidade de rows afetadas : ")
    query = "select street, pub_utc_date, line , id, geom,  from waze.jams limit "
    query += quantTuplas
    #print(query)
    cur.execute(query)
    retornoBD = cur.fetchall()
    dadosEmArquivos(retornoBD)
    #queryAJ(retornoBD)
    #retirarDuplicados()

    """

    #----------------------------

def procurarNoArquivo():

    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

    diretorio = '/home/lucas/PycharmProjects/ic2/bancoDados/'
    daux = diretorio + "/"
    listaArquivos = []
    for root, dirs, files in os.walk(diretorio):
        listaArquivos = files

    for i in range(len(listaArquivos)):

        nomeCompletoArquivo = os.path.join(diretorio, listaArquivos[i])
        with open(nomeCompletoArquivo, newline='\n', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            nomeRuaAux = listaArquivos[i]
            nomeR = nomeRuaAux.split(".csv")
            query = "select pub_utc_date, location, geom from waze.alerts where street='"
            query+= nomeR[0]
            query += "' limit 1000"
            cur.execute(query)
            retornoBD = cur.fetchall()
            for row in reader:
                geomJ = row[3]
               # geomA =


def correlacao():

    limiteRuas = "2"
    limiteJamAlerta = "3000"
    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()
    query = "select street from waze.jams limit "
    query += limiteRuas
    cur.execute(query)
    retornoBD = cur.fetchall()
    ruas = []
    ruasC = []
    for x in retornoBD:
        x = str(x)
        ruas.append(x)

    for x in ruas:
        if x != '':
            sp0 = x.split("(")
            sp1 = sp0[1]
            sp1 = sp1.split(",)")
            nr = str(sp1[0])
            if nr not in ruasC:
                ruasC.append(nr)

    for x in ruasC:

        print("comecei Alerts - " + x)
        query = "select pub_utc_date, location, type from waze.alerts where street = "
        query += x
        query += " limit "
        query += limiteJamAlerta
        #cur = con.cursor()
        cur.execute(query)
        retornoAlerts = []
        retornoAlertsSemDuplicados = []
        retornoAlerts = cur.fetchall()
        # retirar duplicados
        retornoAlertsSemDuplicados = duplicadosAlertas(retornoAlerts, retornoAlertsSemDuplicados, limiteJamAlerta)
        vezes = 0
        for i in range(100):

            if len(retornoAlertsSemDuplicados) <= int(limiteJamAlerta):
                vezes += 1
                offset = int(limiteJamAlerta) * vezes
                query = "select pub_utc_date, location, type from waze.alerts where street = "
                query += x
                query += " offset "
                query += str(offset)
                query += " limit "
                query += limiteJamAlerta
                cur.execute(query)
                retornoAlerts = cur.fetchall()
                retornoAlertsSemDuplicados = duplicadosAlertas(retornoAlerts, retornoAlertsSemDuplicados, limiteJamAlerta)
            else:
                break

        print(len(retornoAlertsSemDuplicados))
        #print(vezes)


        print("comecei Jams - " + x)
        query = "select pub_utc_date, line, delay, length, level from waze.jams where street = "
        query += x
        query += " limit "
        query += limiteJamAlerta
        cur.execute(query)
        retornoJams = cur.fetchall()
        retornoJamSemDuplicados = []
        vezes = 0
        for i in range(100):
            if len(retornoJamSemDuplicados) <= int(limiteJamAlerta):
                vezes += 1
                offset = int(limiteJamAlerta) * vezes
                query = "select pub_utc_date, line, delay, length, level from waze.jams where street = "
                query += x
                query += " offset "
                query += str(offset)
                query += " limit "
                query += limiteJamAlerta
                cur.execute(query)
                retornoJams = cur.fetchall()
                retornoJamSemDuplicados = duplicadosJams(retornoJams, retornoJamSemDuplicados, limiteJamAlerta)
                # print(query)
            else:
                break



        print(len(retornoJamSemDuplicados))
        #criarAquivos(retornoAlertsSemDuplicados, retornoJamSemDuplicados)
        calculoHorario(retornoAlertsSemDuplicados, retornoJamSemDuplicados, x)

def duplicadosJams(retornoJams, retornoJamSemDuplicados, limiteJamAlerta):

    #se hr for igual, fazer um novo registro (todas as coords, maior len, maior delay) e excluir todos os iguais
    for i in retornoJams:
        vdlat = []
        vdlon = []
        vdlat, vdlon = pegarCoordenada(i[1])
        duplicado = 0
        for j in retornoJamSemDuplicados:
            vplat = []
            vplon = []
            vplat, vplon = pegarCoordenada(j[1])
            if i[0] == j[0]:
                vplat, vplon = verificarSeEstaNaLista(vplat, vplon, vdlat, vdlon)
                if int(i[2]) > int(j[2]): #maior delay
                    j[2] = i[2]
                if int(i[3]) > int(j[3]): #maior length
                    j[3] = i[3]
                if int(i[4]) > int(j[4]): #maior level
                    j[4] = i[4]
                line = criarLine(vplat, vplon)
                j[1] = line
                duplicado = 1

        if duplicado == 0:
            aux = []
            aux.append(i[0])
            aux.append(i[1])
            aux.append(i[2])
            aux.append(i[3])
            aux.append(i[4])
            retornoJamSemDuplicados.append(aux)

    return retornoJamSemDuplicados








def duplicadosAlertas(retornoAlerts, retornoAlertsSemDuplicados, limiteJamAlerta):

    for i in retornoAlerts:
        if i not in retornoAlertsSemDuplicados:
            if len(retornoAlertsSemDuplicados) <= int(limiteJamAlerta):
                retornoAlertsSemDuplicados.append(i)
            else:
                return retornoAlertsSemDuplicados

    return retornoAlertsSemDuplicados



def calculoHorario(alertas, jams, rua):

    indiceJ = []
    indiceA = []

    with open('/home/lucas/PycharmProjects/ic2/correlacao.txt', "a") as writter:

        for i in range(0, len(alertas)):
            if i not in indiceA:
                registro = ["", "", "", 0, 0, 0, 0, 0]
                registro[0] = rua
                registro[1] = alertas[i][0]
                registro[2] = alertas[i][0]
                registro[3] = 1
                horaA = str(alertas[i][0])
                ha = horaA[:14]
                indiceA.append(i)
                min1 = int(horaA[14])
                #totalLevel = 0
                #print("comecou")
                for j in range(i+1, len(alertas)):
                    if j not in indiceJ:
                        #horaA = str(alertas[i][0])
                        horaB = str(alertas[j][0])
                        #ha = horaA[:14]
                        hj = horaB[:14]
                        if ha == hj:
                            #min1 = int(horaA[14])
                            min2 = int(horaB[14])
                            if (min1 <= 2 and min2 <= 2) or (min1 > 2 and min2 > 2):
                                #print("entrei alerta")
                                #print("%s - %s || %s - %s" % (horaA, horaB, alertas[i][1], alertas[j][1]))
                                if registro[1] > alertas[j][0]:
                                    registro[1] = alertas[j][0]
                                if registro[2] < alertas[j][0]:
                                    registro[2] = alertas[j][0]
                                registro[3] += 1
                                indiceA.append(j)

                #print("%s - %s -  %s - %d" % (registro[0], registro[1], registro[2], registro[3]))
                #comecar os jams
                for k in range(0, len(jams)):
                    if k not in indiceJ:
                        horaJ = str(jams[k][0])
                        hj = horaJ[:14]
                        #print("%s - %s" % (ha, hj))
                        if ha == hj:
                            min2 = int(horaJ[14])
                            if (min1 <= 2 and min2 <= 2) or (min1 > 2 and min2 > 2):
                                #print("entrei jam")
                                #print("%s - %s" % (horaA, horaJ))
                                indiceJ.append(k)
                                if registro[1] > jams[k][0]:
                                    registro[1] = jams[k][0]
                                if registro[2] < jams[k][0]:
                                    registro[2] = jams[k][0]
                                registro[4] += 1
                                #print("level = %d" % int(jams[k][4]))
                                registro[5] += int(jams[k][4])
                                if registro[6] < int(jams[k][2]):
                                    registro[6] = int(jams[k][2])
                                if registro[7] < int(jams[k][3]):
                                    registro[7] = int(jams[k][3])
                if registro[4] > 0:
                    #print("%d / %d" % (registro[5], registro[4]))
                    registro[5] = registro[5] / registro[4]

                print("%s - %s -  %s - %d - %d - %d - %d - %d" % (registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7]))
                writter.write("%s - %s -  %s - %d - %d - %d - %d - %d\n" % (registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7]))
        print("--")
        for i in range(0, len(jams)):
            if i not in indiceJ:
                indiceJ.append(i)
                registro = ["", "", "", 0, 0, 0, 0, 0]
                registro[0] = rua
                registro[1] = jams[i][0]
                registro[2] = jams[i][0]
                registro[4] = 1
                registro[5] = jams[i][4]
                horaJ = str(jams[i][0])
                h1 = horaJ[:14]
                min1 = int(horaJ[14])
                for j in range(i+1, len(jams)):
                    if j not in indiceJ:
                        horaB = str(jams[j][0])
                        h2 = horaB[:14]
                        if h1 == h2:
                            min2 = int(horaB[14])
                            if (min1 <= 2 and min2 <= 2) or (min1 > 2 and min2 > 2):
                                indiceJ.append(j)
                                if registro[1] > jams[j][0]:
                                    registro[1] = jams[j][0]
                                if registro[2] < jams[j][0]:
                                    registro[2] = jams[j][0]
                                registro[4] += 1
                                #print("level = %d" % int(jams[k][4]))
                                registro[5] += int(jams[j][4])
                                if registro[6] < int(jams[j][2]):
                                    registro[6] = int(jams[j][2])
                                if registro[7] < int(jams[j][3]):
                                    registro[7] = int(jams[j][3])
                if registro[4] > 0:
                    #print("%d / %d" % (registro[5], registro[4]))
                    registro[5] = registro[5] / registro[4]

                print("%s - %s -  %s - %d - %d - %d - %d - %d" % (registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7]))
                writter.write("%s - %s -  %s - %d - %d - %d - %d - %d\n" % (registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7]))


def criarAquivos(alertas, jams):

    valertas = []
    for a in alertas:
        valertas.append([a[0]])

    for i in range(len(valertas)):
        for j in range(i+1, len(valertas)):
            if valertas[i] > valertas[j]:
                aux = valertas[i]
                valertas[i] = valertas[j]
                valertas[j] = aux

    vjams = []
    for j in jams:
        vjams.append([j[0]])

    for i in range(len(vjams)):
        for j in range(i + 1, len(vjams)):
            if vjams[i] > vjams[j]:
                aux = vjams[i]
                vjams[i] = vjams[j]
                vjams[j] = aux

    with open("alertas.txt", "a") as writter:
        for a in valertas:
            writter.write("%s\n" % str(a[0]))

    with open("jams.txt", "a") as writter:
        for a in vjams:
            writter.write("%s\n" % str(a[0]))

def queryAJ(retornoBD):

    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

    diretorio = '/home/lucas/PycharmProjects/ic2/bancoDados/'
    daux = diretorio + "/"
    listaArquivos = []
    for root, dirs, files in os.walk(diretorio):
        listaArquivos = files

    for i in range(len(listaArquivos)):

        nomeCompletoArquivo = os.path.join(diretorio, listaArquivos[i])
        with open(nomeCompletoArquivo, newline='\n', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:

                if row[0] != "date":
                    data = row[0]
                    coord = row[1]

                    if data != "":

                        try:
                            timestamp = datetime.strptime(data, "%Y-%m-%d %H:%M:%S.%f")
                        except:
                            timestamp = datetime.strptime(data, "%Y-%m-%d %H:%M:%S")

                        mesAmais = timestamp + relativedelta(months=+1)

                        nomeRuaAux = listaArquivos[i]
                        nomeR = nomeRuaAux.split(".csv")
                        print(nomeR[0])
                        nomeRua = nomeR[0]
                        query = "select street, location, pub_utc_date, geom from waze.alerts where street = '"
                        query += nomeRua
                        query += "' or pub_utc_date >= '"
                        query += str(timestamp)
                        query += "' and pub_utc_date <= '"
                        query += str(mesAmais)
                        query += "' limit 50"

                        cur = con.cursor()
                        cur.execute(query)
                        retornoAlerts = cur.fetchall()

                        #print(retornoAlerts)
                        #print("------")

                        for a in retornoAlerts:

                            geomJ = str(row[3])
                            geomA = a[3]
                            #print(a[2])
                            query = "select ST_Intersects('"
                            query += geomJ
                            query += "','"
                            query += geomA
                            query += "')"
                            #print(query)

                            cur = con.cursor()
                            cur.execute(query)
                            retornoA = cur.fetchone()
                            #print(retornoA[0])
                            if retornoA[0] == "True":
                                print("Verdade")

                            """
                            if retornoA[0] == "False":
                                print("Nao foi")
                            elif retornoA[0] == "True":
                                print("Foi")
    
                            """



                    print("-----------")


                    #transformar a coord em 2 vets de lat e lon
                    #transformar a location em 2 vets de lat e lon
                    #ver se a location eh igual a a alguma coord dele





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
                            geom = str(retornoBD[i][4])
                            csvData = [data, line, id, geom]
                            writer.writerow(csvData)
                    except:
                        continue
                else:
                    try:
                        nomeCompletoArquivo = os.path.join(diretorio, retornoBD[i][0] + ".csv")
                        with open(nomeCompletoArquivo, 'w', newline='\n', encoding='utf-8') as csvFile:
                            writer = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                            csvData = ["date", "line", "id", "geom"]
                            writer.writerow(csvData)
                            data = str(retornoBD[i][1])
                            line = str(retornoBD[i][2])
                            id = str(retornoBD[i][3])
                            geom = str(retornoBD[i][4])
                            csvData = [data, line, id, geom]
                            writer.writerow(csvData)
                    except:
                        continue

        except:
            continue



def retirarDuplicados():

    diretorio = '/home/lucas/PycharmProjects/ic2/bancoDados'
    daux = diretorio + "/"
    listaArquivos = []
    for root, dirs, files in os.walk(diretorio):
        listaArquivos = files


    for i in range(len(listaArquivos)):

        nomeRua = listaArquivos[i]
        d = nomeRua.split(".csv")
        print(d[0])
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

        #para cada arquivo, ver a hora + 1... 1 de cada linha
        vIndice = []
        #print(len(data))
        for x in range(len(data)):

            dataAtual = str(data[x])
            vplat = []
            vplon = []
            vplat, vplon = pegarCoordenada(line[x])
            vdlat = []
            vdlon = []
            daigual = ""
            for j in range(19):  # pegar ate os min
                daigual += dataAtual[j]

            for k in range(len(data)):
                if x != k and k not in vIndice:
                    dataAnalisada = str(data[k])
                    #igual
                    if data[k].startswith(daigual):
                        #colocar em vlat, vlon
                        vdlat, vdlon = pegarCoordenada(line[k])
                        #print("%s - %s" % (data[k], daigual))

                        #marcar os indices que vao sair
                        if x not in vIndice:
                            vIndice.append(x)
                        vIndice.append(k)

            #diferente + 1
            if len(vdlat) > 0:
                recursiva(data[x], data, vdlat, vdlon, line, vIndice)
                #comparar vp com vd
                vplat, vplon = analisarVetorCoordenadas(vplat, vplon, vdlat, vdlon)
                #criar nova line com vp
                linestring = criarLine(vplat, vplon)
                #ver quais as linhas que vao ser eliminadas

                #print(linestring)
                for j in range(len(vIndice)):
                    #excluirRegistro(id[vIndice[j]])
                    print("ia excluir o %s" % id[vIndice[j]])
                #gerarJam2(dataAtual, linestring, d[0], vplat, vplon)
                #criar um novo registro
                #query de exclusao

def recursiva(dataAgora, data, vdlat, vdlon, line, vIndice):

    lataux = []
    lonaux = []
    dataAgora = gerarHora(dataAgora)
    dataAgora = str(dataAgora)
    sinal = 0

    #add 1 min e ve se alguem tem esse horario
    for x in range(len(data)):

        #verifica se o indice ja foi igual a alguem
        if x not in vIndice:
            #print(dataAgora)
            #print(data[x])
            #print("%s - %s" % (dataAgora, data[x]))
            if dataAgora == data[x]:
                sinal = 1
                lataux, lonaux = pegarCoordenada(line[x])
                #print("coloquei o %d" % x)
                vIndice.append(x)

    #se tiver alguem com o horario+1, vai entrar aqui
    if sinal == 1:
        for i in range(len(lataux)):
            aux = 0
            for j in range(len(vdlat)):
                if vdlat[j] == lataux[i] and vdlon[j] == lonaux[i]:
                    aux = 1
            if aux == 0:
                vdlat.append(lataux[i])
                vdlon.append(lonaux[i])

        recursiva(dataAgora, data, vdlat, vdlon, line, vIndice)

def gerarHora(data):
    #dt_string = "12/11/2018 09:15:32.100000"
    #print(data)
    timestamp = ""
    try:
        timestamp = datetime.strptime(data, "%Y-%m-%d %H:%M:%S.%f")
        #print(timestamp)
    except:
        #print("-----------------")
        timestamp = datetime.strptime(data, "%Y-%m-%d %H:%M:%S")
        #print(timestamp)

    sec = timedelta(seconds=1)
    new_timestamp = timestamp
    new_timestamp += sec
    return new_timestamp

    #print(new_timestamp)
    #print("-----")



def gerarData(dia, hora, indice, data, line):

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
    vplat = []
    vplon = []
    vdlat = []
    vdlon = []
    sinal = 0
    indicesParaExcluirBD = []
    for x in range(0, 100000):
        mili = timedelta(microseconds=x)
        new_timestamp = timestamp
        new_timestamp += mili
        #print(new_timestamp)
        #ve se alguem tem esse valor de hora
        for i in range(len(data)):
            if i != indice:
                hrstr = str(new_timestamp)
                if data == hrstr:
                    sinal = 1
                    if i not in indicesParaExcluirBD:
                        indicesParaExcluirBD.append(i)
                    if indice not in indicesParaExcluirBD:
                        indicesParaExcluirBD.append(indicesParaExcluirBD)
                    vplat, vplon = pegarCoordenada(line[indice])
                    vdlat, vdlon = pegarCoordenada(line[i])
                    vplat, vplon = analisarVetorCoordenadas(vplat, vplon, vdlat, vdlon)

    #ja tenho o vetor
    #ja tenho o indice





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

def analisarVetorCoordenadas(vplat, vplon, vdlat, vdlon):

    for i in range(len(vdlat)):
        sinal = 0
        for j in range(len(vplat)):
            if vdlat[i] == vplat[j] and vdlon[i] == vplon[j]:
                sinal = 1

        if sinal == 0:
            vplat.append(vdlat[i])
            vplon.append(vdlon[i])


    return vplat, vplon


def gerarJam2(dataHora, line, street, lat, lon):


    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()

    id = gerarId()
    uuid = gerarUuid()
    pub_mills = str(gerarPubMillis())
    dt = dataHora
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

def excluirRegistro(id):


    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()
    query = "Delete from waze.jams where id= '"
    query += id
    query += "'"
    cur.execute(query)
    retornoBD = cur.fetchall()
    print(retornoBD)


def sql():
    con = psycopg2.connect(host='127.0.0.1', database='ic',
                           user='lucas', password='')
    cur = con.cursor()
    query = "SELECT a1.location, a2.location, j.line FROM waze.alerts a1, waze.alerts a2, waze.jams j WHERE a1.street = a2.street AND a1.pub_utc_date - a2.pub_utc_date < '00:30:00.000' AND a1.id != a2.id AND a1.street =j.street AND (a1.pub_utc_date - j.pub_utc_date <  '00:30:00.000' OR a2.pub_utc_date - j.pub_utc_date < '00:30:00.000') limit 10"
    cur.execute(query)
    retornoBD = cur.fetchall()
    for x in retornoBD:
        print(x)
        print("\n\n")
    #print(line)
    #print(line)

#retirarDuplicados()
#gerarHora()
#conexao()
#sql()
#queryAJ()
#procurarNoArquivo()
correlacao()
#calculoHorario()
