import sys
import os
import pandas as pd
import time
import pyodbc
from sqlalchemy import create_engine

server = 'PC-Nico'
database = 'TestDatabase'
username = 'sa'
password = 'sa'

engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')

def leer_boletas_pandas(directorio_base):
    # Lista para almacenar los DataFrames de todas las boletas
    datos_boletas = []
    datos_Tratados = []
    df_tratados = []	
    # Recorrer directorios de años, meses y archivos CSV
    for ano in os.listdir(directorio_base):
        
        directorio_ano = os.path.join(directorio_base, ano)

        if not os.path.isdir(directorio_ano):
            continue

        for mes in os.listdir(directorio_ano):
            
            directorio_mes = os.path.join(directorio_ano, mes)
            if not os.path.isdir(directorio_mes):
                continue

            for nombre_archivo in os.listdir(directorio_mes):
                if nombre_archivo.endswith(".csv"):
                    # Construir la ruta completa al archivo
                    ruta_archivo = os.path.join(directorio_mes, nombre_archivo)
                    print(nombre_archivo)
                    # Leer la boleta y convertirla en un DataFrame de Pandas
                    df_boleta = leer_boleta_individual(ruta_archivo)

                    # Verificar si el archivo cumple con el formato esperado antes de agregarlo a datos_boletas
                    
                    datos_boletas.append(df_boleta)
            df_tratados = crearDataFrameVentas(datos_boletas)
            datos_boletas = []
            datos_Tratados.append(df_tratados)
    return datos_Tratados

def leer_boleta_individual(file_path):
    # Listas para almacenar las columnas
    codes = []
    days = []
    months = []
    years = []
    productos = []
    precios_total = []

    # Leer el archivo CSV línea por línea y procesar las columnas
    with open(file_path, 'r') as file:
        # Leer la primera línea (encabezado)
        header = file.readline().strip().split(',')

        # Leer las líneas restantes y procesar las columnas
        for line in file:
            # Usar coma como delimitador
            columns = line.strip().split(',')

            # Agregar las columnas a las listas
            if columns[-1] != '0':
                #dipshit need you to split the code on column 0 split it by - and aply all the elementes to differents list like code, day, month, year
                boletaCode = columns[0].strip().split('-')
                codes.append(boletaCode[0])
                days.append(boletaCode[1])
                months.append(boletaCode[2])
                years.append(boletaCode[3])
                productos.append(','.join(columns[1:-1]))
                precios_total.append(columns[-1])

    # Crear un DataFrame a partir de las listas y usar el encabezado proporcionado
    df = pd.DataFrame({'BoletaCode': codes, 'Dia': days, 'Mes': months, 'Año': years, 'Productos': productos, 'Precio Total': precios_total})

    return df


def crearDataFrameVentas(lista_df):
    # Inicializar listas para almacenar los datos
    meses = []
    años = []
    productos = []
    cantidades = []

    # Iterar sobre cada DataFrame en la lista
    for df in lista_df:
        # Iterar sobre cada fila del DataFrame
        for index, row in df.iterrows():
            # Separar los productos en la fila y contar cada producto individualmente
            productos_venta = row['Productos'].split(',')
            for producto_venta in productos_venta:
                producto_venta = producto_venta.strip()
                # Agregar los datos a las listas
                meses.append(row['Mes'])
                años.append(row['Año'])
                productos.append(producto_venta)
                cantidades.append(1)

    # Crear un DataFrame a partir de las listas
    df_ventas = pd.DataFrame({'Mes': meses, 'Año': años, 'Producto': productos, 'Cantidad': cantidades})

    # Agrupar por mes, año y producto, sumando las cantidades
    df_ventas = df_ventas.groupby(['Mes', 'Año', 'Producto']).sum().reset_index()

    return df_ventas

def leer_facturas_pandas(directorio_base):
    # Lista para almacenar los datos de todos los archivos
    datos_factura = []

    # Recorrer directorios de años, meses y archivos CSV
    for ano in os.listdir(directorio_base):
        directorio_ano = os.path.join(directorio_base, ano)

        if not os.path.isdir(directorio_ano):
            continue

        for mes in os.listdir(directorio_ano):
            directorio_mes = os.path.join(directorio_ano, mes)

            if not os.path.isdir(directorio_mes):
                continue

            for nombre_archivo in os.listdir(directorio_mes):
                if nombre_archivo.endswith(".csv"):
                    # Construir la ruta completa al archivo
                    file_path = os.path.join(directorio_mes, nombre_archivo)
                    print(nombre_archivo)

                    try:
                        # Leer el archivo CSV con Pandas
                        df = pd.read_csv(file_path)
                        fechas = []
                        fechas = nombre_archivo.replace("facturas", "").replace(".csv", "").split("-")

                        df["Dias"] = fechas[0]
                        df["Mes"] = fechas[1]
                        df["Año"] = fechas[2]
                        
                        
                        #Check de las columnas
                        expected_columns = ['Proveedor', 'Producto', 'Cantidad', 'Precio Unitario', 'Precio Total']
                        if not all(column in df.columns for column in expected_columns):
                            print(f"Advertencia: El archivo {file_path} no tiene las columnas esperadas.")
                            sys.exit(1) # comprobar que la advertencia se imprima en la consola
                            continue
                        
                        #Borrado de erros en caso de que alguna columna numerica en factura sea 0
                        cols_to_check = ['Cantidad', 'Precio Unitario', 'Precio Total']
                        df = df[~df[cols_to_check].eq(0).any(axis=1)]
                        # Verificar si hay algún valor igual a 0 en las columnas específicas
                        if df.empty:
                            print(f"Advertencia: Todas las filas con valores igual a 0 fueron eliminadas en el archivo {file_path}")
                            sys.exit(1) # comprobar que la advertencia se imprima en la consola
                        else:
                            # Agregar el DataFrame a datos_totales si no hay valores igual a 0
                            datos_factura.append(df)

                    except pd.errors.EmptyDataError:
                        print(f"Advertencia: El archivo {file_path} está vacío.")

                    except pd.errors.ParserError:
                        print(f"Error al leer el archivo {file_path}. Asegúrate de que tenga el formato CSV correcto.")
                            
    # Imprimir el DataFrame resultante
    '''                   
    for df in datos_totales:
        print(df)
    '''
    return datos_factura

def crear_inventaro_Df(directorio_base):

    # Lista para almacenar los datos de todos los archivos de inventario
    datos_inventario = []

    # Recorrer directorios de años, meses y archivos CSV de inventario
    
    for nombre_archivo in os.listdir(directorio_base):

        if nombre_archivo.endswith(".xlsx"):
            # Construir la ruta completa al archivo
            file_path = os.path.join(directorio_base, nombre_archivo)
            print(nombre_archivo)

            try:

                # Leer el archivo CSV con Pandas
                df = pd.read_excel(file_path)                
                # Verificar si hay algún valor igual a 0 en las columnas específicas
                if df.empty:

                    print(f"Advertencia: Todas las filas con valores igual a 0 fueron eliminadas en el archivo {file_path}")
                    sys.exit(1) # comprobar que la advertencia se imprima en la consola
                else:
                    # Agregar el DataFrame a datos_totales si no hay valores igual a 0
                    nuevo_df = pd.DataFrame({
                        'Año': '2005',
                        'Mes': 'Enero',
                        'ID': df['ID'],
                        'Valor': 100
                        
                    })
                    
                    datos_inventario.append(nuevo_df)

            except pd.errors.EmptyDataError:
                print(f"Advertencia: El archivo {file_path} está vacío.")

            except pd.errors.ParserError:
                print(f"Error al leer el archivo {file_path}. Asegúrate de que tenga el formato CSV correcto.")
    

            # Verificar si el archivo cumple con el formato esperado antes de agregarlo a datos_inventario
            

    return datos_inventario
   


def leer_precios_pandas(directorio_base):
    datos_precios = []

    for ano in os.listdir(directorio_base):
        directorio_ano = os.path.join(directorio_base, ano)

        if not os.path.isdir(directorio_ano):
            continue

        for mes in os.listdir(directorio_ano):
            directorio_mes = os.path.join(directorio_ano, mes)

            if not os.path.isdir(directorio_mes):
                continue

            for nombre_archivo in os.listdir(directorio_mes):
                if "precios" in nombre_archivo.lower() and nombre_archivo.endswith(".csv"):
                    file_path = os.path.join(directorio_mes, nombre_archivo)
                    #print(nombre_archivo)
                    try:
                        df = pd.read_csv(file_path)
                        df["Mes"] = mes	
                        df["Año"] = ano

                        #Check de las columnas
                        expected_columns = ['Identificador', 'Precio']
                        if not all(column in df.columns for column in expected_columns):
                            print(f"Advertencia: El archivo {file_path} no tiene las columnas esperadas.")
                            sys.exit(1) # comprobar que la advertencia se imprima en la consola
                            continue
                        #Borrado de erros en caso de que alguna columna numerica en Precios sea 0
                        cols_to_check = ['Precio']
                        df = df[~df[cols_to_check].eq(0).any(axis=1)]
                        # Verificar si hay algún valor igual a 0 en las columnas específicas
                        if df.empty:
                            print(f"Advertencia: Todas las filas con valores igual a 0 fueron eliminadas en el archivo {file_path}")
                            sys.exit(1) # comprobar que la advertencia se imprima en la consola
                        else:
                            # Agregar el DataFrame a datos_totales si no hay valores igual a 0
                            datos_precios.append(df)

                    except pd.errors.EmptyDataError:
                        print(f"Advertencia: El archivo {file_path} está vacío.")

                    except pd.errors.ParserError:
                        print(f"Error al leer el archivo {file_path}. Asegúrate de que tenga el formato CSV correcto.")

    return datos_precios

def leer_productps(directorio_base):

    # Lista para almacenar los datos de todos los archivos de inventario
    datos_inventario = []

    # Recorrer directorios de años, meses y archivos CSV de inventario
    
    for nombre_archivo in os.listdir(directorio_base):

        if nombre_archivo.endswith(".xlsx"):
            # Construir la ruta completa al archivo
            file_path = os.path.join(directorio_base, nombre_archivo)
            #print(nombre_archivo)

            try:

                # Leer el archivo CSV con Pandas
                df = pd.read_excel(file_path)
                #Check de las columnas
                #Borrado de erros en caso de que alguna columna numerica en factura sea 0
                
                # Verificar si hay algún valor igual a 0 en las columnas específicas
                if df.empty:

                    print(f"Advertencia: Todas las filas con valores igual a 0 fueron eliminadas en el archivo {file_path}")
                    sys.exit(1) # comprobar que la advertencia se imprima en la consola
                else:
                    # Agregar el DataFrame a datos_totales si no hay valores igual a 0
                    
                    datos_inventario.append(df)

            except pd.errors.EmptyDataError:
                print(f"Advertencia: El archivo {file_path} está vacío.")

            except pd.errors.ParserError:
                print(f"Error al leer el archivo {file_path}. Asegúrate de que tenga el formato CSV correcto.")
    

            # Verificar si el archivo cumple con el formato esperado antes de agregarlo a datos_inventario
            

    return datos_inventario


def leer_proovedores(directorio_base):

    # Lista para almacenar los datos de todos los archivos de inventario
    datos_inventario = []

    # Recorrer directorios de años, meses y archivos CSV de inventario
    
    for nombre_archivo in os.listdir(directorio_base):

        if nombre_archivo.endswith(".xlsx"):
            # Construir la ruta completa al archivo
            file_path = os.path.join(directorio_base, nombre_archivo)
            print(nombre_archivo)

            try:

                # Leer el archivo CSV con Pandas
                df = pd.read_excel(file_path)
                #Check de las columnas
                #Borrado de erros en caso de que alguna columna numerica en factura sea 0
                
                # Verificar si hay algún valor igual a 0 en las columnas específicas
                if df.empty:

                    print(f"Advertencia: Todas las filas con valores igual a 0 fueron eliminadas en el archivo {file_path}")
                    sys.exit(1) # comprobar que la advertencia se imprima en la consola    

            except pd.errors.EmptyDataError:
                print(f"Advertencia: El archivo {file_path} está vacío.")

            except pd.errors.ParserError:
                print(f"Error al leer el archivo {file_path}. Asegúrate de que tenga el formato CSV correcto.")
    

            # Verificar si el archivo cumple con el formato esperado antes de agregarlo a datos_inventario
            

    return df

tiempo_inicial = time.time()

directorio_base = "./Boletas/"
print("Iniciando lectura boletas")
datos_boletas = leer_boletas_pandas(directorio_base)
print("Boletas Listas")
directorio_facturas = "./Facturas/"
datos_facturas_pandas = leer_facturas_pandas(directorio_facturas)
print("Facturas Listas")
directorio_inventario = "./Productos/"
datos_inventario_pandas = crear_inventaro_Df(directorio_inventario)
print("Inventario Creado")
directorio_precios = "./Precios/"
datos_precios_pandas = leer_precios_pandas(directorio_precios)
print("Precios Listas")
directorio_producto = "./Productos/"
datos_productos_pandas = leer_productps(directorio_producto)
print("Productos litos")
directorio_proveedor = "./Proveedores/"
datos_proveedor_pandas = leer_proovedores(directorio_proveedor)
print("Proveedores litos")

tiempo_final = time.time()
tiempo_total = tiempo_final - tiempo_inicial
print(f"Tiempo total de ejecución: {tiempo_total} segundos")



print("\n------------------")
'''
cant = 0
for df in datos_boletas:    
    print(df)
    cant += 1
    if cant == 5:
        break
    print("\n------------------")

cant = 0
for df in datos_facturas_pandas:    
    print(df)
    cant += 1
    if cant == 5:
        break
    print("\n------------------")


cant = 0
for df in datos_inventario_pandas:    
    print(df)
    cant += 1
    if cant == 5:
        break
    print("\n------------------")


cant = 0
for df in datos_precios_pandas:    
    print(df)
    cant += 1
    if cant == 5:
        break
    print("\n------------------")

for df in datos_productos_pandas:    
    print(df)
    print("\n------------------")
'''
#print(datos_proveedor_pandas)
#(print(datos_proveedor_pandas.columns))

datos_proveedor_pandas.to_sql('Proveedores', con=engine, if_exists='replace', index=False)
try:
    check = True
    for df in datos_productos_pandas:
        if check:
            df.to_sql('Productos', con=engine, if_exists='replace', index=False)
            check = False
        else:
            df.to_sql('Productos', con=engine, if_exists='append', index=False)
    check = True
    for df in datos_precios_pandas:
        if check:
            df.to_sql('Precios', con=engine, if_exists='replace', index=False)
            check = False
        else:
            df.to_sql('Precios', con=engine, if_exists='append', index=False)
    check = True
    for df in datos_boletas:
        if check:
            df.to_sql('Boletas', con=engine, if_exists='replace', index=False)
            check = False
        else:
            df.to_sql('Boletas', con=engine, if_exists='append', index=False)
    check = True
    for df in datos_facturas_pandas:
        if check:
            df.to_sql('Facturas', con=engine, if_exists='replace', index=False)
            check = False
        else:
            df.to_sql('Facturas', con=engine, if_exists='append', index=False)
    check = True
    for df in datos_inventario_pandas:
        if check:
            df.to_sql('Inventarios', con=engine, if_exists='replace', index=False)
            check = False
        else:
            df.to_sql('Inventarios', con=engine, if_exists='append', index=False)
            
except Exception as ex:
    print(ex)  
