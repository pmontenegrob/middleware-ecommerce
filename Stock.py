# ============================================================
#Programa          : Stock.py
#Autor             : Dguzman
# FechaCreacion     : 01/01/2022
# FechaModificacion : 24/05/2022   wclaure
# Hora 15:37
# ============================================================
from calendar import month
from importlib.metadata import metadata
from msilib.schema import Error
from multiprocessing.sharedctypes import Value
from pickle import TRUE
import time
from time import sleep
from typing import final
from wsgiref import headers
import requests
import json
import datetime as dt
import sys
from requests.api import get
import conexion
from woocommerce import API
from requests.models import Response
import random
from datetime import datetime
from datetime import datetime

inicio_programa = datetime.now()
print("Inicio Stock :", inicio_programa)
wcapi = API(
    url="https://www.union.com.bo",
    consumer_key="ck_bc80ff6493da9bd2ec3460a38bfed23c3f8dcaab",
    consumer_secret="cs_2943b571edfbef17c4b64ab73356d0d5343ebfff",
    version="wc/v3",
    timeout=60
)

# 3 consume api de union e inserta en bd
urlU = 'http://rest-ec.union.com.bo:1616/rest/stockecom'

session = requests.session()
session.auth = ('ecommerce', 'ecommerce')
custom_header = {'TenantId': '01,0105'}
cn = conexion.conexion
print("Eliminando Tabla de Stock del Middleware")
with cn.cursor() as curs:
    consul = "TRUNCATE TABLE StockAlmacen"
    curs.execute(consul)
print("Tabla de Stock Eliminada")
time.sleep(2)
response = session.get(urlU)
cont = 0
#producs = wcapi.get("products?per_page=100")
#total_pages = int(producs.headers['X-WP-TotalPages'])
if response.ok:

    response_data = response.json()
    stoker = response_data['data']
    for stock in stoker:

        cont = cont+1
        #Sku = "011652"
        Sku = stock['Sku']

        try:

            traeJson = wcapi.get('products?sku={}'.format(Sku.strip())).json()
            #print (traeJson)

        except TimeoutError:
            print(f"Timeout Obtener producto de Ecommerce: {Sku.strip()}")
        except requests.exceptions.ConnectTimeout:
            print(
                f"Error al obtener producto de ConnectTIMEOUT: {Sku.strip()}")
        except requests.exceptions.ReadTimeout:
            print(f"Error al obtener producto de ReadTIMEOUT: {Sku.strip()}")

        if len(traeJson) > 0:
            Nombre = stock['Nombre']
            DescripcionCorta = stock['DescripcionCorta']
            descr2 = stock['descr']
            descr = descr2.capitalize()
            StockActual = int(stock['StockActual'])
            if StockActual <= 0:
                StockActual = 0
            PrecioPvp = float(stock['PrecioPvp'])
            Categoria = stock['Categoria']
            Imagenes = stock['Imagenes']
            Marca = stock['Marca']
            PrecioPvi = float(stock['PrecioPvi'])
            PrecioPvp = float(stock['PrecioPvp'])
            traee = traeJson[0]
            idMod = str(traee['id'])
            data = {
                "manage_stock": True,
                "stock_quantity": StockActual,
                "name": Nombre,
                "description": descr.strip(),
                "regular_price": str(PrecioPvp),
                "wcb2b_group_prices": [
                    {
                        "6246": {
                            "regular_price": str(PrecioPvi)
                        }
                    }
                ], 
                "meta_data": [
                    {
                        "key": "wcb2b_product_group_prices",
                        "value": {
                            "6246": {
                                "regular_price": str(PrecioPvi)
                            }
                        }
                    }
                ]
                # name,description,short_description,regular_price
            }
            try:
                wcapi.put("products/{}".format(idMod), data).json()
                # ReadTimeout

            except requests.exceptions.ConnectTimeout:
                print(
                    f"Ocurrio un error al sincronizar con Ecommerce  ConnectTimeout: {idMod.strip()} ")
                #print(f"Ocurrio un error al sincronizar con Ecommerce :  ")
                exit()
            except requests.exceptions.ReadTimeout:
                print(
                    f"Time out con sincronizacion con Ecommerce ReadTimeout ACTUALIZA: {idMod.strip()} ")
                #print(f"Ocurrio un error al sincronizar con Ecommerce :  ")
                try:
                    wcapi.put("products/{}".format(idMod), data, timeout=120).json()
                    
                except requests.exceptions.ReadTimeout:
                    print(
                    f"TIMEOUT ERROR ")

                #print(f"Ocurrio un error al sincronizar con Ecommerce :  ")
                except requests.exceptions.Timeout:
                    print("The request timed out.")
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred: {e}")
            #print(f"Actualizado con exito: {Sku}")
            try:
                with cn.cursor() as cursor:
                    consulta = "INSERT INTO StockAlmacen(Sku, Nombre, DescripcionCorta, StockActual, PrecioPvp, Categoria, Imagenes, Marca, PrecioPvi, descr)Values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                    cursor.execute(consulta, (Sku, Nombre, DescripcionCorta, StockActual,
                                   PrecioPvp, Categoria, Imagenes, Marca, PrecioPvi, descr))
            except:
                print("error al agregar producto a tabla de middleware")
                exit()
        else:
            print(f"PRODUCTO NO EXISTE: {Sku}")
            Nombre = stock['Nombre']
            DescripcionCorta = stock['DescripcionCorta']
            
            descr2 = stock['descr']
            descr = descr2.capitalize()
            StockActual = int(stock['StockActual'])
            if StockActual <= 0:
                StockActual = 0
            PrecioPvp = float(stock['PrecioPvp'])
            Categoria = stock['Categoria']
            sep = ">" 
            CategoriaA = list(reversed(Categoria.split(sep)))
            CategoriaId = []
            Imagenes = stock['Imagenes']
        

            index = 1
            CategoriaEcom2 =0
            CategoriaEcom=wcapi.get("products/categories?per_page=100")
            tp = int(CategoriaEcom.headers['X-WP-TotalPages'])


            while index<=tp:
                             
                CategoriaEcom2 = wcapi.get("products/categories?per_page=100&page="+str(index)).json() 
                #print(f"PAGINA  : {index}")
                for cate in CategoriaA:
                  indarray = 0
                  #print(f"categoria a buscar : {cate}")
                  while indarray < len(CategoriaEcom2):
                    idEcom= CategoriaEcom2[indarray]['id']
                    nombreEcom = CategoriaEcom2[indarray]['name']
                    cate2 = cate.strip()
                    if cate2 == nombreEcom:
                      #print("encontro")
                      
                      CategoriaId.append({"id":idEcom})
                      indarray += 100
                  
                    indarray += 1
                    #print(indarray)  
                index += 1
               

 
            #arreglo.append(dato)
            Marca = stock['Marca']
            PrecioPvi = float(stock['PrecioPvi'])
            data = {
                "manage_stock": True,
                "stock_quantity": StockActual,
                "name": Nombre,
                "description": descr,
                "short_description": DescripcionCorta,
                "regular_price": str(PrecioPvp),
                "sku": Sku,
                "type": "simple",
                "wcb2b_group_prices": [
                    {
                        "6246": {
                            "regular_price": str(PrecioPvi)
                        }
                    }
                ], 
                "meta_data": [
                    {
                        "key": "wcb2b_product_group_prices",
                        "value": {
                            "6246": {
                                "regular_price": str(PrecioPvi)
                            }
                        }
                    }
                ],
                # "categories":[{Categoria}], hay que trabajar
                #"categories":[{"id":939},{"id":940},{"id":688}],
                "categories":CategoriaId,
                "images": [
                    {
                        "src": "https://www.union.com.bo/wp-content/uploads/2022/08/generico.png"
                    }
                ]
                # name,descriptioPrecioPvpn,short_description,regular_price
            }
            try:
                #respuesta1=wcapi.get("products/categories").json()
                # print(wcapi.get("products/11633").json())
                respuesta = wcapi.post("products", data).json()
                #print(f"PRODUCTO CREADO : {respuesta['id']}")
                try:
                     print(f"PRODUCTO CREADO : {respuesta['id']}")
                except Exception :
                    print(f"PRODUCTO CREADO : {respuesta['message']}")
                    
                # ReadTimeout

            except requests.exceptions.ConnectTimeout:
                print(
                    f"Ocurrio un error al sincronizar con Ecommerce  ConnectTimeout Nuevo Prod: {idMod.strip()} ")
                #print(f"Ocurrio un error al sincronizar con Ecommerce :  ")
                exit()
            except requests.exceptions.ReadTimeout:
                print(
                    f"Time out con sincronizacion con Ecommerce TIMEOUT EN NUEVO PRODUCTO: {idMod.strip()} ")
                #print(f"Ocurrio un error al sincronizar con Ecommerce :  ")

        print(f"Actualizado con exito: {Sku} ")

    print("Total productos: ", cont)
    print("#3 Union --> Middleware")
time.sleep(2)
final_programa = datetime.now()
total = final_programa - inicio_programa
print("Proceso Finalizado de forma correcta !!!!")
print("Tiempo de ejecucion: ", total)
