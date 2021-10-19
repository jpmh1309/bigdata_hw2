import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, arrays_zip, sum, percentile_approx

def spark_session():
    spark = SparkSession.builder.appName("Bigdata: Tarea 2").getOrCreate()
    return spark

def get_parser():
    parser = argparse.ArgumentParser('Tarea2')
    parser.add_argument('--folder', '-f', type=str, help='Folder que contiene todos los archivos *.json ', required=True)
    return parser

def total_productos(data):
    print("Generate file 'total_productos.csv'")

    flattenDF = data.select(explode("compras").alias("c")).selectExpr("c.nombre", "c.cantidad")

    arrayDF = flattenDF.withColumn("tmp", arrays_zip(col("nombre"), col("cantidad"))).withColumn("tmp", explode("tmp")).select(col("tmp.nombre"), col("tmp.cantidad"))
    finalDF = arrayDF.groupBy('nombre').agg(sum("cantidad").alias("total_vendido"))
    
    finalDF.show()
    
    finalDF.coalesce(1).write.mode('overwrite').option("header", True).csv("output/total_productos")

def total_cajas(data):
    print("Generate file 'total_cajas.csv'")

    flattenDF = data.select(col("numero_caja"), explode("compras").alias("c")).selectExpr("numero_caja", "c.cantidad", "c.precio_unitario")

    arrayDF = flattenDF.withColumn("tmp", arrays_zip(col("cantidad"), col("precio_unitario"))).withColumn("tmp", explode("tmp")).select("numero_caja", "tmp.cantidad", "tmp.precio_unitario")
    arrayDF = arrayDF.withColumn("total_vendido", col("cantidad")*col("precio_unitario") )
    
    finalDF = arrayDF.select("numero_caja", "total_vendido")
    finalDF = finalDF.groupBy('numero_caja').agg(sum("total_vendido").alias("total_vendido"))
    
    finalDF.show()
    
    finalDF.coalesce(1).write.mode('overwrite').option("header", True).csv("output/total_cajas")

def metricas(spark, data):
    print("Generate file 'metricas.csv'")

    metricas = {
        'caja_con_mas_ventas': str(0),
        'caja_con_menos_ventas': str(0),
        'percentil_25_por_caja': str(0),
        'percentil_50_por_caja': str(0),
        'percentil_75_por_caja': str(0),
        'producto_mas_vendido_por_unidad': str(0),
        'producto_de_mayor_ingreso': str(0)
    }

    flattenDF = data.select(col("numero_caja"), explode("compras").alias("c")).selectExpr("numero_caja", "c.cantidad", "c.precio_unitario")
    arrayDF = flattenDF.withColumn("tmp", arrays_zip(col("cantidad"), col("precio_unitario"))).withColumn("tmp", explode("tmp")).select("numero_caja", "tmp.cantidad", "tmp.precio_unitario")
    arrayDF = arrayDF.withColumn("total_vendido", col("cantidad")*col("precio_unitario") )
    total_cajasDF = arrayDF.select("numero_caja", "total_vendido")
    total_cajasDF = total_cajasDF.groupBy('numero_caja').agg(sum("total_vendido").alias("total_vendido"))
    total_cajasDF.show()

    # Get caja_con_mas_ventas
    metricas['caja_con_mas_ventas'] = str(total_cajasDF.sort(total_cajasDF.total_vendido.desc()).collect()[0][0])

    # Get caja_con_menos_ventas
    metricas['caja_con_menos_ventas'] = str(total_cajasDF.sort(total_cajasDF.total_vendido.asc()).collect()[0][0])

    total_cajasDF = total_cajasDF.sort(total_cajasDF.total_vendido.asc())
    total_cajasDF.show()

    percentile = total_cajasDF.select(percentile_approx("total_vendido", [0.25, 0.5, 0.75], 1000000).alias("quantiles"))
    
    # Get percentil_25_por_caja
    metricas['percentil_25_por_caja'] = str(percentile.collect()[0][0][0])

    # Get percentil_50_por_caja
    metricas['percentil_50_por_caja'] = str(percentile.collect()[0][0][1])

    # Get percentil_75_por_caja
    metricas['percentil_75_por_caja'] = str(percentile.collect()[0][0][2])

    flattenDF = data.select(explode("compras").alias("c")).selectExpr("c.nombre", "c.cantidad", "c.precio_unitario")
    arrayDF = flattenDF.withColumn("tmp", arrays_zip(col("nombre"), col("cantidad"), col("precio_unitario"))).withColumn("tmp", explode("tmp")).select(col("tmp.nombre"), col("tmp.cantidad"), (col("tmp.cantidad")*col("tmp.precio_unitario")).alias("total_ingreso"))
    total_productosDF = arrayDF.groupBy('nombre').agg(sum("cantidad").alias("total_vendido"), sum("total_ingreso").alias("total_ingreso"))
    total_productosDF.show()

    # Get producto_mas_vendido_por_unidad
    metricas['producto_mas_vendido_por_unidad'] = total_productosDF.sort(total_productosDF.total_vendido.desc()).collect()[0][0]

    # Get producto_de_mayor_ingreso
    metricas['producto_de_mayor_ingreso'] = total_productosDF.sort(total_productosDF.total_ingreso.desc()).collect()[0][0]

    # Write csv file
    lol = list(map(list, metricas.items()))
    metricasDF = spark.createDataFrame(lol, ["metrica", "valor"])
    metricasDF.show()

    metricasDF.coalesce(1).write.mode('overwrite').option("header", True).csv("output/metricas")

def main(args=None):
    parser = get_parser()
    args = parser.parse_args(args)

    # Create spark session
    spark = spark_session()

    # Read cajas_*.json files
    df = spark.read.option("multiline","true").json(args.folder)
    df.printSchema()
    print(df.schema.fieldNames)
    df.show()

    # Generate file 'total_productos.csv'
    # total_productos(df)

    # Generate file 'total_cajas.csv'
    # total_cajas(df)

    # Generate file 'metricas.csv'
    metricas(spark, df)

if __name__ == '__main__':
    main()