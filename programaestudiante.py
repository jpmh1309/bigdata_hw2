import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, arrays_zip, sum

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
    
    finalDF.coalesce(1).write.option("header", True).csv("output/total_productos")

def total_cajas(data):
    print("Generate file 'total_cajas.csv'")


    flattenDF = data.select(col("numero_caja"), explode("compras").alias("c")).selectExpr("numero_caja", "c.cantidad", "c.precio_unitario")

    arrayDF = flattenDF.withColumn("tmp", arrays_zip(col("cantidad"), col("precio_unitario"))).withColumn("tmp", explode("tmp")).select("numero_caja", "tmp.cantidad", "tmp.precio_unitario")
    arrayDF = arrayDF.withColumn("total_vendido", col("cantidad")*col("precio_unitario") )
    
    finalDF = arrayDF.select("numero_caja", "total_vendido")
    finalDF = finalDF.groupBy('numero_caja').agg(sum("total_vendido").alias("total_vendido"))
    
    finalDF.show()
    
    finalDF.coalesce(1).write.option("header", True).csv("output/total_cajas")

def metricas(data):
    print("Generate file 'metricas.csv'")
    data.coalesce(1).write.option("header", True).csv("output/metricas")

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
    total_productos(df)

    # Generate file 'total_cajas.csv'
    total_cajas(df)

    # Generate file 'metricas.csv'
    # metricas(df)

if __name__ == '__main__':
    main()