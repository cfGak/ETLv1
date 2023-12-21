ETLv1: Procesamiento y Carga de Datos
Descripción
ETLv1 es un script de Python avanzado diseñado para automatizar y optimizar las operaciones de Extracción, Transformación y Carga (ETL) de datos. Este proyecto utiliza la potencia de Pandas para el procesamiento de datos y SQL para la interacción eficiente con bases de datos, facilitando así la gestión de grandes volúmenes de información.

Características
Extracción Eficiente: Capacidad para leer y procesar grandes conjuntos de datos desde múltiples fuentes.
Transformación de Datos: Funciones robustas para la limpieza, transformación y preparación de datos.
Carga en Bases de Datos: Integración con SQL para una carga eficiente y segura de los datos procesados.

Requisitos
Python 3.x
Pandas
PyODBC
SQLAlchemy

Instalación
Para instalar las dependencias necesarias, ejecute el siguiente comando en su terminal:
pip install pandas pyodbc sqlalchemy
## Configuración
Antes de ejecutar el script, asegúrese de configurar correctamente los parámetros de conexión a su base de datos SQL en el script.

## Uso
Para ejecutar el script, navegue hasta el directorio del proyecto y ejecute:
bash
python ETLv1.py

Funciones Principales
leer_boletas_pandas(directorio_base): Procesa archivos CSV de boletas desde un directorio base.

leer_boleta_individual(file_path): Analiza y procesa un archivo CSV individual de boleta.

crearDataFrameVentas(lista_df): Genera un DataFrame consolidado de ventas.

leer_facturas_pandas(directorio_base): Extrae y procesa facturas desde archivos CSV.

crear_inventario_Df(directorio_base): Crea un DataFrame detallado del inventario.

leer_precios_pandas(directorio_base): Procesa información de precios desde archivos CSV.

leer_productos(directorio_base): Extrae datos de productos para su análisis y procesamiento.

leer_proveedores(directorio_base): Gestiona y procesa información de proveedores.
