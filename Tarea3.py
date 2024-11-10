#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import date_format
from pyspark.sql.types import DecimalType

df.select(date_format('date_time', 'dd-mm-yyyy').alias('date')).show()
# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)


#imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Eliminar las columnas no relevantes
df = df.drop('Identificador de la Orden','Entidad','Rama de la Entidad','Orden de la Entidad','Sector de la Entidad',
                       'Solicitante','Solicitud','Agregacion','Entidad Obigada','EsPostconflicto','NIT Proveedor',
					   'Proveedor','Actividad Economica Proveedor','NIT Entidad','ID Entidad')


# Verificar el resultado
print("\nPrimeras filas después de eliminar columnas:")
df.show(5)

# Estadisticas básicas
df.summary().show()

# Muestra estadísticas descriptivas para las columnas numéricas
print("\nDescripción estadística del dataset:")
df.describe().show() 

# Análisis de Totales nulos
print("\nCantidad de Totales nulos por columna:")
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Eliminar filas con valores nulos
df = df.dropna()

# Mostrar las primeras filas después de eliminar las filas con nulos
print("\nPrimeras filas después de eliminar filas con nulos:")
df.show(5) 

# Consulta1: Filtrar por Total y seleccionar columnas
print("Ciudades con Total mayor a 10000000\n")
ciudades = df.filter(F.col('Total') > 10000000).select('Total','Ciudad','Año')
ciudades.show()


# Ordenar filas por los Totales en la columna "Total" en orden descendente
print("Totales ordenados de mayor a menor\n")
sorted_df = df.sort(F.col("Total").desc())
sorted_df.show()

# Consulta2:Agrupar por la columna 'items' y contar las ocurrencias de cada valor en la columna
df_items = df.groupBy("items").count()

# Mostrar el resultado
print("\nResultado de agrupar por 'items' y contar las ocurrencias:")
df_items.show()

# Consulta3:Agrupar por la columna 'Estado' y contar las ocurrencias de cada valor en la columna
df_estado = df.groupBy("Estado").count()

# Mostrar el resultado
print("\nResultado de agrupar por 'estado' y contar las ocurrencias:")
df_estado.show()

# Consulta4: Filtrar por Fecha y seleccionar columnas
print("Fechas con estado Cancelada\n")
canceladas = df.filter(F.col('Estado') == 'Cancelada').select('Estado','Fecha','Total')
canceladas.show()

# Consulta5: Filtrar por Fecha y seleccionar columnas
print("Fechas con estado Emitida\n")
emitidas = df.filter(F.col('Estado') == 'Emitida').select('Estado','Fecha','Total')
emitidas.show()

# Consulta6: Filtrar por Fecha y seleccionar columnas
print("Fechas con estado Cerrada\n")
cerradas = df.filter(F.col('Estado') == 'Cerrada').select('Estado','Fecha','Total')
cerradas.show()

# Cerrar la sesión de Spark
spark.stop()
