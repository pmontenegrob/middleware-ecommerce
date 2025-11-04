# ============================================================
#Programa          : Pedidos.py
#Autor             : Jccampos
# FechaCreacion     : 01/01/2022
# Hora 15:37
# ============================================================
import pyodbc
from calendar import month
from importlib.metadata import metadata
from multiprocessing.sharedctypes import Value
from pickle import TRUE
from requests.api import get
import time
from time import sleep
from wsgiref import headers
import requests
import json
import datetime as dt
import conexion
from woocommerce import API
from requests.models import Response
import random
from datetime import datetime
from datetime import datetime
from os import system
import sys 
from os.path import exists

print("Ejecutando sincronizacion...")
def truncate(num, n):
    integer = int(num * (10**n))/(10**n)
    return float(integer)
def search_in_array(key, array):
    for metaData in array:
        if (metaData["key"] == key):
            return metaData["value"]
    # Value not found
    return ""


wcapi = API(
    url="https://www.union.com.bo",
    consumer_key="ck_bc80ff6493da9bd2ec3460a38bfed23c3f8dcaab",
    consumer_secret="cs_2943b571edfbef17c4b64ab73356d0d5343ebfff",
    version="wc/v3"
)

varx = 0
tipo_cambio = 6.96
#orden = wcapi.get('orders').json()
ordenes = wcapi.get("orders?per_page=10").json()
print("1...")
inicio = datetime.now()
#log_str = inicio.strftime('%Y%m%d_%H%M%S')
log_str = inicio.strftime('%Y%m%d_')
archivo_log = f'log_configs_{log_str}.log'
if not exists('logs'):
    system('mkdir logs')

sw = False
cn = conexion.conexion

cn2 = conexion.conexion2

ii = 0
while ii < len(ordenes):
    try:
        daton = ordenes[ii]
        billing = daton['billing']
        cupon = daton['discount_total']
        cupon1 = float(cupon)
        
        shipping = daton['shipping']
        metaData = daton['meta_data']
        ni = metaData[1]
        me = daton['shipping_lines']
        pedido = daton['id']

        cursor = cn.cursor()
        consul = '''select*from PedidosWoo where Idpedido = {}'''.format(
            pedido)
        cursor.execute(consul)
        seleccionId = cursor.fetchone()
        if seleccionId == None:
            sw = False
        else:
            sw = True       
            dataId = str(seleccionId[1])

        s = daton['line_items']
        c = 0
        if sw == False:
            for prd in s:
                Desc = ''
                Tipodesc=0
                FechaPedido = daton['date_created']
                Idpedido = daton['id']
                EstadoPedido = daton['status']
                CodLocal = search_in_array('_cod_store', metaData)
                if CodLocal == "":
                    CodLocal = "01"
              
                CodClienteUnion = search_in_array('_cod_cliente', metaData)
                if CodClienteUnion == "" or   CodClienteUnion =='6954':
                    CodClienteUnion = "015452"

                nombre = shipping['first_name']
                Apellidos = shipping['last_name']
                Direccion = billing['address_1']
                Ciudad = billing['city']
                TipoDoc = search_in_array('_billing_tax_type', metaData)
                Email = billing['email']
                Telefono = billing['phone']
                # Buscar "_billing_tax_number" en metaData
                noni = search_in_array('_billing_tax_number', metaData)
                if noni == '' or noni == '0':
                    Nit = '1'
                else:
                    Nit = noni

                nombreF = metaData[4]
                appF = nombreF['value']

                NombreFactura = appF
            
                if NombreFactura == '':
                    NombreFactura = 'SN'
                sprueba = NombreFactura
                abs,bas = 'áéíóúüñÁÉÍÓÚÜÑ','aeiouunAEIOUUN'
                trans = str.maketrans(abs,bas)

                
                NombreFactura=sprueba.translate(trans)
                

                MetodoPago = daton['payment_method_title']
                Importededescuentodelcarrito = daton['discount_tax']
                Importedesubtotaldelpedido = daton['discount_total']
                if len(me) != 0:
                    med = me[0]
                    MetododeEnvio = med['method_title']
                    Importedeenviodelpedido = med['total']
                    Importetotaldelpedido = med['total']
                    Importetotaldeimpuestosdelpedido = med['total_tax']
                else:
                    MetododeEnvio = 'sd'
                    Importedeenviodelpedido = '0'
                    Importetotaldelpedido = '0'
                    Importetotaldeimpuestosdelpedido = '0'
                if len(s):
                    skk = s[c]
                    Sku = skk['sku']
                    Item = skk['id']
                    desProducto = skk['name']
                    Cantidad = skk['quantity']
                    
                    Precio = float(skk['subtotal']) / float(skk['quantity'])*tipo_cambio
                    Precio = truncate(Precio,2)



                    urlMU = 'http://rest-ec.union.com.bo:1616/rest/MIDDLEPRICE'
                    sessionmu = requests.session()
                    sessionmu.auth = ('ecommerce', 'ecommerce')
                    custom_header = {'TenantId': '01,0105'}
                    parameters = {
                        "cClient": CodClienteUnion,
                        "cProd": Sku
                    }
                    responsemu = sessionmu.get(
                    urlMU, headers=custom_header, params=parameters)
                    r = responsemu.json()
                    Precio_totvs = r['data'][0]['Precio']
                    if daton['currency'] == 'BOB':
                        Precio_totvs = Precio_totvs *  6.96
                    Precio = round(skk['price'],2)
                    if Precio_totvs != 0:
                        v1 = Precio * 100
                        v2= v1/Precio_totvs 
                        v3 = (v2 - 100) * -1 
                        Descuento_fin = round(v3,2)
                    else : 
                        Descuento_fin = 0  
                 



                    descuentoAux = skk['subtotal']
                    total = skk['total']
                    if Descuento_fin == 0:
                        Descuento2 = round(float(descuentoAux),2) - round(float(total),2)
                        auxcharly = Descuento2 / Cantidad
                        auxcharly2 = auxcharly * tipo_cambio
                        auxcharly3= truncate(float(auxcharly2),2)
                        auxcharly5 = auxcharly3 * Cantidad
                        Descuento = round(auxcharly5,2)
                    else:
                        Descuento = 0.00
                    Importe = skk['total']
                else:
                    Sku = 0
                    Item = 0
                    desProducto = ''
                    Cantidad = 0
                    Precio = 0
                    Importe = 0
                Desc1 = Descuento
               
                if Desc1 > 0.00:
                    tipodesc = 'VEN'   
                else:  
                    tipodesc = ''   
                porcdesc = ''
            
                if Importe == '0.00' and len(skk['meta_data']) > 0:
                    prueba1 = skk['meta_data'][0]['display_key']
                    if prueba1 == 'Promoción BOGO' or prueba1 == '_wc_bogof_rule_id' :
                        urlMU = 'http://rest-ec.union.com.bo:1616/rest/MIDDLEPRICE'
                        sessionmu = requests.session()
                        sessionmu.auth = ('ecommerce', 'ecommerce')
                        custom_header = {'TenantId': '01,0105'}
                        parameters = {
                            "cClient": CodClienteUnion,
                            "cProd": Sku
                        }
                        responsemu = sessionmu.get(
                        urlMU, headers=custom_header, params=parameters)
                        r = responsemu.json()
                        Precio_totvs = r['data'][0]['Precio']
                        Precio = Precio_totvs
                        Descuento_valor = Precio_totvs * tipo_cambio
                        v1 = 0.01 * 100
                        v2= v1/Descuento_valor 
                        v3 = (v2 - 100) * -1                            
                        porcdesc = truncate(float(v3),2)
                        tipodesc = 'VEN'

                else:  
                    porcdesc = '0.00'
                metadatos = skk['meta_data']
                if len(metadatos) != 0 :
                    if skk['meta_data'][0]['display_key'] == '_ywdpd_discounts' :
                        valores =  skk['meta_data'][0]['value']
                        Precio_base = float(valores['applied_discounts'][0]['price_base']) * tipo_cambio
                        Precio_Adjustado = float(valores['applied_discounts'][0]['price_adjusted']) * tipo_cambio
                        if Precio_base == 0.00 and Precio_Adjustado == 0.00:
                            #aqui vendra el servicio de precio 0
                            porcetaje_descuento = 99.97
                            tipodesc = 'REV'
                            porcdesc = porcetaje_descuento
                        else:
                            porcetaje_descuento = ((Precio_Adjustado / Precio_base * 100) - 100)  * -1
                            porcdesc = truncate(float(porcetaje_descuento),2)
                            if(porcdesc >= 100.0):
                                #calcula porcentaje de descuento regla de 3 simples.
                                Descuento_valor =  Precio_base * tipo_cambio
                                v1 = 0.01 * 100
                                v2= v1/Descuento_valor 
                                v3 = (v2 - 100) * -1                            
                                porcdesc = truncate(float(v3),2)
                            tipodesc = 'REV'
                if Descuento_fin >=1 and Descuento_fin < 99 :
                    porcdesc = Descuento_fin
                    tipodesc = 'REV'                                
                Tipodesc = tipodesc
                Porcdesc = ''
                Porcdesc = porcdesc
                Desc1d = 0.0
                Tipodescd = ''
                Tipodescd2 = '0.00'
                CodigodeCupon = 'no'
                Importededescuento = skk['total_tax']
                Importedeimmpuestosdeldescuento = skk['subtotal_tax']
                Uneg = CodLocal.strip()
                Cloc = CodLocal.strip()
                Stat = '0'
                name = daton['payment_method_title']
                with cn.cursor() as cursorr:
                    consulta = "INSERT INTO PedidosWoo(FechaPedido, Idpedido, EstadoPedido, Nombre, Apellidos, Direccion, Ciudad, TipoDoc, Email, Telefono, Nit, NombreFactura, MetodoPago, Importededescuentodelcarrito, Importedesubtotaldelpedido, MetododeEnvio, Importedeenviodelpedido, Importetotaldelpedido, Importetotaldeimpuestosdelpedido, SKU, Item, desProducto, Cantidad, Precio, Importe, CodigodeCupon, Importededescuento, Importedeimmpuestosdeldescuento,CodClienteUnion,Uneg, Cloc, Stat, Tipodesc, Desc1, Porcdesc)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                    try:
                        cursorr.execute(consulta, (FechaPedido, Idpedido, EstadoPedido, nombre, Apellidos, Direccion, Ciudad, TipoDoc, Email, Telefono, Nit, NombreFactura, MetodoPago, Importededescuentodelcarrito, Importedesubtotaldelpedido, MetododeEnvio,Importedeenviodelpedido, Importetotaldelpedido, Importetotaldeimpuestosdelpedido, Sku, Item, desProducto, Cantidad, Precio, Importe, CodigodeCupon, Importededescuento, Importedeimmpuestosdeldescuento, CodClienteUnion, Uneg, Cloc, Stat,Tipodesc, Desc1, Porcdesc))
                    except pyodbc.Error as ex:
                        sqlstate = ex.args[1]
                        print(sqlstate) 
                        exit()

                    # Inserta el item por el delivery
                    if c == 0:
                        if daton['currency'] == 'BOB':
                            fuerasextoanillo = 4*6.96
                        else:
                            fuerasextoanillo = 4
                        if float(Importedeenviodelpedido) > fuerasextoanillo:
                            Sku = "010740"
                            Item = 999
                            desProducto = "SERVICIO ENTREGA A DOMICILIO"
                            Cantidad = 1
                            Precio = Importedeenviodelpedido
                            Importe = Importedeenviodelpedido

                            cursor_2 = cn.cursor()
                            consulta = "INSERT INTO PedidosWoo(FechaPedido, Idpedido, EstadoPedido, Nombre, Apellidos, Direccion, Ciudad, TipoDoc, Email, Telefono, Nit, NombreFactura, MetodoPago, Importededescuentodelcarrito, Importedesubtotaldelpedido, MetododeEnvio, Importedeenviodelpedido, Importetotaldelpedido, Importetotaldeimpuestosdelpedido, SKU, Item, desProducto, Cantidad, Precio, Importe, CodigodeCupon, Importededescuento, Importedeimmpuestosdeldescuento,CodClienteUnion,Uneg, Cloc, Stat, Tipodesc, Desc1, Porcdesc)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                            cursor_2.execute(consulta, (FechaPedido, Idpedido, EstadoPedido, nombre, Apellidos, Direccion, Ciudad, TipoDoc, Email, Telefono, Nit, NombreFactura, MetodoPago, Importededescuentodelcarrito, Importedesubtotaldelpedido, MetododeEnvio,
                                             Importedeenviodelpedido, Importetotaldelpedido, Importetotaldeimpuestosdelpedido, Sku, Item, desProducto, Cantidad, Precio, Importe, CodigodeCupon, Importededescuento, Importedeimmpuestosdeldescuento, CodClienteUnion, Uneg, Cloc, Stat,Tipodescd,Desc1d,Tipodescd2))
                        else:
                            # PRODUCTO  DE ENTREGA A DOMICILIO
                            if float(Importedeenviodelpedido) > 0:
                                Sku = "010741"
                                Item = 999
                                desProducto = "SERVICIO ENTREGA A DOMICILIO"
                                Cantidad = 1
                                Precio = Importedeenviodelpedido
                                Importe = Importedeenviodelpedido

                                cursor_2 = cn.cursor()
                                consulta = "INSERT INTO PedidosWoo(FechaPedido, Idpedido, EstadoPedido, Nombre, Apellidos, Direccion, Ciudad, TipoDoc, Email, Telefono, Nit, NombreFactura, MetodoPago, Importededescuentodelcarrito, Importedesubtotaldelpedido, MetododeEnvio, Importedeenviodelpedido, Importetotaldelpedido, Importetotaldeimpuestosdelpedido, SKU, Item, desProducto, Cantidad, Precio, Importe, CodigodeCupon, Importededescuento, Importedeimmpuestosdeldescuento,CodClienteUnion,Uneg, Cloc, Stat, Tipodesc, Desc1, Porcdesc)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                                cursor_2.execute(consulta, (FechaPedido, Idpedido, EstadoPedido, nombre, Apellidos, Direccion, Ciudad, TipoDoc, Email, Telefono, Nit, NombreFactura, MetodoPago, Importededescuentodelcarrito, Importedesubtotaldelpedido, MetododeEnvio,
                                                 Importedeenviodelpedido, Importetotaldelpedido, Importetotaldeimpuestosdelpedido, Sku, Item, desProducto, Cantidad, Precio, Importe, CodigodeCupon, Importededescuento, Importedeimmpuestosdeldescuento, CodClienteUnion, Uneg, Cloc, Stat, Tipodescd, Desc1d,Tipodescd2))
                    c = c+1
            ii = ii+1
        else:
            ii = ii+1  

    except conexion.Error as e:

        print("Ocurrió un error al insertar: ", e)
        with open(f'logs/{archivo_log}', 'w') as log:
            log.write(f'{datetime.now()}: failjc2\n')

print("#1 ECommerce --> Middleware")

# 2 consume de midleware inserta en api union
urlMU = 'http://rest-ec.union.com.bo:1616/rest/pedido'

sessionmu = requests.session()
sessionmu.auth = ('ecommerce', 'ecommerce')
custom_header = {'TenantId': '01,0105'}

cursormu = cn.cursor()
consulmu = '''select*from PedidosWoo where Stat = 0 ORDER BY IdPedido,Item'''
cursormu.execute(consulmu)

seleccionId = cursormu.fetchone()


reprod = ""
productos = ""
sw = False
ss = False
sss = False
ssss = False
contador = 0
while seleccionId is not None:
    try:
       
        fecha = seleccionId[0].strftime('%d/%m/%Y')
        IdPedido = str(seleccionId[1])
        EstadoPedido = str(seleccionId[2])
        CodClienteUnion = str(seleccionId[28])
        Nombre = str(seleccionId[3])
        Apellidos = str(seleccionId[4])
        Direccion = str(seleccionId[5])
        Ciudad = str(seleccionId[6])

        TipoDoc = str(seleccionId[7])

        if TipoDoc == "ci":
            TypoDoc = "1"
        elif TipoDoc == "cex":
            TypoDoc = "2"
        elif TipoDoc == "pas":
            TypoDoc = "3"
        elif TipoDoc == "od":
            TypoDoc = "4"
        elif TipoDoc == "nit":
            TypoDoc = "5"
        elif TipoDoc == "":
            TypoDoc = "1"
        elif TipoDoc == "":
            TypoDoc = "1"
        else:
            print('error')

        Email = str(seleccionId[8])
        Telefono = str(seleccionId[9])
        Nit = str(seleccionId[10])
        NombreFactura = str(seleccionId[11])
        MetodoPago = str(seleccionId[12])
        ImporteDescuentoCarrito = str(seleccionId[13])
        ImporteSubTotalPedido = str(seleccionId[14])
        MedodoEnvio = str(seleccionId[15])
        ImporteEnvioPedido = str(seleccionId[16])
        ImporteTotal = str(seleccionId[17])
        ImporteTotalImpuestPedido = str(seleccionId[18])
        Sku = str(seleccionId[19])
        Item = str(seleccionId[20])
        desProducto = str(seleccionId[21])
        Cantidad = str(seleccionId[22])
        cant = float(Cantidad)
        Precio = float(seleccionId[23])
        Importe = seleccionId[24]
        CodigoCupon = str(seleccionId[25])
        ImporteDescuento = seleccionId[26]
        ImporteImpuestoDescuento = seleccionId[27]
        Uneg = seleccionId[29]
        Cloc = seleccionId[30].strip()
        Stat = seleccionId[31]
        Tipodesc = seleccionId[32]
        Desc1 = float(seleccionId[33])
        Porcdesc = float(seleccionId[34])

        if Cloc == "":
            Uneg = "01"
            Cloc = "01"

        ran = random.randint(10000000000, 80000000000)
        rando = str(ran)
        Token = rando.strip
        skku = Sku.rstrip()

        ConPag = "001"
        ULocal = "01"
        urlMU2 = 'http://rest-ec.union.com.bo:1616/rest/CONDCLIE'
        sessionmu2 = requests.session()
        sessionmu2.auth = ('ecommerce', 'ecommerce')
        custom_header = {'TenantId': '01,0105'}
        parameters = {
            "cLoja": Cloc,
            "cCod": CodClienteUnion
        }
        responsemu = sessionmu2.get(
        urlMU2, headers=custom_header, params=parameters)
        r = responsemu.json()
        responsemu.close()
        condclie = r['data'][0]['Cond']
        if MetodoPago == "Contra entrega":
            if condclie >= '002':
                ConPag = "002"
            else:
                ConPag = condclie
            
        if MetodoPago == "Línea de crédito":
              if condclie >= '026':
                ConPag = condclie
              
                

        CodmPag = "6"
        if ConPag == "001":
            CodmPag = "1"

        IdPedidoNuevo = 0
        seleccionId = cursormu.fetchone()
        
        if seleccionId:
            IdPedidoNuevo = str(seleccionId[1])
        else:
            sss = True
            ss = True
            contador = contador+1
        if ss == False:
            if IdPedido == IdPedidoNuevo:
                sw = True
                contador = contador+1
                ssss = True
            else:
                sw = False

        if sw == False and contador == 0:
            data = {
                'USRCOD': "000297",
                "C5_CLIENTE": CodClienteUnion,
                "C5_LOJACLI": Cloc,
                "C5_MOEDA": 1,
                "C5_UOBSERV": "Observacion",
                "DDATABASE": fecha,
                "C5_ULOCAL": "01",
                "C5_DOCGER": "2",
                "SERIE": "ECO",
                "C5_CODMPAG": CodmPag,
                "C5_XEMAIL": Email,
                "C5_XTIPDOC": TypoDoc,
                "C5_UNOMCLI": NombreFactura,
                "C5_UNITCLI": Nit,
            }
            data['productos'] = []
            data['productos'].append({
                "B1_PRV1": Precio * tipo_cambio,
                "C6_LOCAL": "01",
                "C6_PRODUTO": skku,
                "C6_QTDVEN": cant,
                "C6_UTPLIQ": Tipodesc,
                "C6_VALDESC": Desc1,
                "C6_DESCONT": Porcdesc,                
                "C6_VDOBS": ""
            })

            data['C5_CONDPAG'] = ConPag
            data['TOKEN'] = rando
            data['C5_UTIPOEN'] = 8
            data['online'] = "2"
        if contador >= 1:
            if contador == 1:
                data = {
                    'USRCOD': "000297",
                    "C5_CLIENTE": CodClienteUnion,
                    "C5_LOJACLI": Cloc,
                    "C5_MOEDA": 1,
                    "C5_UOBSERV": "Observacion",
                    "DDATABASE": fecha,
                    "C5_ULOCAL": "01",
                    "C5_DOCGER": "2",
                    "SERIE": "ECO",
                    "C5_CODMPAG": CodmPag,
                    "C5_XEMAIL": Email,
                    "C5_XTIPDOC": TypoDoc,
                    "C5_UNOMCLI": NombreFactura,
                    "C5_UNITCLI": Nit,
                }
                data['productos'] = []
                data['productos'].append({
                    "B1_PRV1": Precio * tipo_cambio,
                    "C6_LOCAL": "01",
                    "C6_PRODUTO": skku,
                    "C6_QTDVEN": cant,
                    "C6_UTPLIQ": Tipodesc,
                    "C6_VALDESC": Desc1, 
                    "C6_DESCONT": Porcdesc, 
                    "C6_VDOBS": ""
                })
            if contador > 1:
                data['productos'].append({
                    "B1_PRV1": Precio * tipo_cambio,
                    "C6_LOCAL": "01",
                    "C6_PRODUTO": skku,
                    "C6_QTDVEN": cant,
                    "C6_UTPLIQ": Tipodesc,
                    "C6_VALDESC": Desc1,
                    "C6_DESCONT": Porcdesc, 
                    "C6_VDOBS": ""
                })
            if contador == 1:
                data['C5_CONDPAG'] = ConPag
                data['TOKEN'] = rando
                data['C5_UTIPOEN'] = 8
                data['online'] = "2"
                contador = contador + 1

            datao = data

        if sss == True:
            sw = False
        if contador > 0 and sw == False:
            contador = 0

        if sw == False:
            responsemu = sessionmu.post(
                urlMU, headers=custom_header, data=json.dumps(data))
            c5num = 0
            c5col = 0

            if responsemu.status_code == 200 or responsemu.status_code == 201:
                r = responsemu.json()
                Stado = r['status']
                if Stado != 'Fail':
                    datpos = r['data']
                    post = datpos[0]
                    c5num = post['C5_NUM']
                    c5col = post['C5_COLOR']
                    c5col1 = str(c5col)
                    print(c5num)

                    if responsemu.status_code == 200 or responsemu.status_code == 201:

                        Curupdate = cn2.cursor()
                        consulup = '''UPDATE PedidosWoo SET Stat = {}'''.format(
                            c5num) + ''' WHERE Idpedido = {}'''.format(IdPedido)
                        Curupdate.execute(consulup)

                        # Pone el pedido y el color en el woocommerce-
                        data = {
                            "id": IdPedido,
                            "totvs_id": c5num,
                            "totvs_color": c5col1
                        }

                        session = requests.session()
                        urlMUWOO = "https://www.union.com.bo/wp-json/totvs/v1/update-order-meta/"
                        responsemu = session.post(urlMUWOO, data=json.dumps(data))
                        print(IdPedido)
                        Curupdate.commit()
                        Curupdate.close()
                        del Curupdate
                        
                        
                else:
                    stado = r['message']
                    hora = datetime.now()
                    
                    print("Se produjo un error en el pedido.")
                    with open(f'logs/{archivo_log}', 'w',encoding="utf-8") as log:
                        log.write(f'{datetime.now()}: {stado}\n')
                    
    except: 
       
        print("Error al Sincronizar Pedido a totvs")
        
print("Sincronizacion finalizada !!!!")
time.sleep(5)
exit()


