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

# ==================== Función para dividir en bloques ====================
def chunks(lst, n):
    """Divide la lista lst en bloques de tamaño n"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
# ========================================================================

# ==================== Inicio del proceso ===============================
inicio_programa = datetime.now()
print("Inicio Stock :", inicio_programa)

wcapi = API(
    url="https://www.union.com.bo",
    consumer_key="ck_bc80ff6493da9bd2ec3460a38bfed23c3f8dcaab",
    consumer_secret="cs_2943b571edfbef17c4b64ab73356d0d5343ebfff",
    version="wc/v3",
    timeout=300  # Aumentamos timeout a 5 minutos por seguridad
)

urlU = 'http://rest-ec.union.com.bo:1616/rest/stockecom'
session = requests.session()
session.auth = ('ecommerce', 'ecommerce')
custom_header = {'TenantId': '01,0105'}
cn = conexion.conexion

# Limpiar tabla de stock
print("Eliminando Tabla de Stock del Middleware")
with cn.cursor() as curs:
    curs.execute("TRUNCATE TABLE StockAlmacen")
print("Tabla de Stock Eliminada")
time.sleep(2)

# Traemos todos los productos del middleware
response = session.get(urlU)
if not response.ok:
    raise Exception("No se pudo obtener stock del middleware")

stoker = response.json()['data']
cont = 0

# ==================== Procesar productos en bloques de 50 ====================
for stock_chunk in chunks(stoker, 50):
    for stock in stock_chunk:
        cont += 1
        Sku = stock['Sku'].strip()

        # Obtener producto de WooCommerce por SKU
        traeJson = []
        for attempt in range(3):
            try:
                traeJson = wcapi.get(f'products?sku={Sku}').json()
                break
            except requests.exceptions.RequestException as e:
                print(f"Error obtener producto {Sku} intento {attempt+1}: {e}")
                time.sleep(5)

        # Preparar datos comunes
        Nombre = stock['Nombre']
        DescripcionCorta = stock['DescripcionCorta']
        descr = stock['descr'].capitalize()
        StockActual = max(0, int(stock['StockActual']))
        PrecioPvp = float(stock['PrecioPvp'])
        Categoria = stock['Categoria']
        Imagenes = stock['Imagenes']
        Marca = stock['Marca']
        PrecioPvi = float(stock['PrecioPvi'])

        # ==================== Actualizar producto existente ====================
        if len(traeJson) > 0:
            idMod = str(traeJson[0]['id'])
            data = {
                "manage_stock": True,
                "stock_quantity": StockActual,
                "name": Nombre,
                "description": descr.strip(),
                "regular_price": str(PrecioPvp),
                "wcb2b_group_prices": [{"6246": {"regular_price": str(PrecioPvi)}}],
                "meta_data": [{"key": "wcb2b_product_group_prices", "value": {"6246": {"regular_price": str(PrecioPvi)}}}]
            }

            for attempt in range(3):
                try:
                    wcapi.put(f"products/{idMod}", data).json()
                    break
                except requests.exceptions.RequestException as e:
                    print(f"Error actualizar producto {Sku} intento {attempt+1}: {e}")
                    time.sleep(5)

            # Insertar en tabla de middleware
            with cn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO StockAlmacen(Sku, Nombre, DescripcionCorta, StockActual, PrecioPvp, Categoria, Imagenes, Marca, PrecioPvi, descr) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (Sku, Nombre, DescripcionCorta, StockActual, PrecioPvp, Categoria, Imagenes, Marca, PrecioPvi, descr)
                )

        # ==================== Crear producto nuevo si no existe ====================
        else:
            # Obtener categorías de WooCommerce en bloques de 100
            CategoriaA = list(reversed(Categoria.split(">")))
            CategoriaId = []
            page = 1
            while True:
                categorias_page = wcapi.get(f"products/categories?per_page=100&page={page}").json()
                if not categorias_page:
                    break
                for cate in CategoriaA:
                    cate = cate.strip()
                    for c in categorias_page:
                        if c['name'] == cate:
                            CategoriaId.append({"id": c['id']})
                page += 1

            data = {
                "manage_stock": True,
                "stock_quantity": StockActual,
                "name": Nombre,
                "description": descr,
                "short_description": DescripcionCorta,
                "regular_price": str(PrecioPvp),
                "sku": Sku,
                "type": "simple",
                "wcb2b_group_prices": [{"6246": {"regular_price": str(PrecioPvi)}}],
                "meta_data": [{"key": "wcb2b_product_group_prices", "value": {"6246": {"regular_price": str(PrecioPvi)}}}],
                "categories": CategoriaId,
                "images": [{"src": "https://www.union.com.bo/wp-content/uploads/2022/08/generico.png"}]
            }

            for attempt in range(3):
                try:
                    respuesta = wcapi.post("products", data).json()
                    print(f"PRODUCTO CREADO: {respuesta.get('id', respuesta.get('message'))}")
                    break
                except requests.exceptions.RequestException as e:
                    print(f"Error crear producto {Sku} intento {attempt+1}: {e}")
                    time.sleep(5)

        print(f"Procesado con éxito: {Sku}")

print("Total productos procesados:", cont)
final_programa = datetime.now()
total = final_programa - inicio_programa
print("Proceso Finalizado de forma correcta !!!!")
print("Tiempo de ejecución:", total)
