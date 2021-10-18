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

    # data.write.csv("output/total_productos.csv")
    #data.select("compras").show()
    #data.select("compras").printSchema()
    #print(data.select("compras").schema.fieldNames)
    #explodeDF = data.select(explode("compras").alias("c"))
    #explodeDF.show()
    #print(explodeDF.schema.fieldNames)
    #explodeDF.printSchema()

    #explodeDF2 = explodeDF.select(explode("nombre").alias("n"))
    #explodeDF2.show()
    #flattenDF = explodeDF.select(explode("nombre").alias("n"))
    #flattenDF.show()
    #final = data.withColumn("nombre", data("compras.nombre"))
    #final.show()

    #nombreDF = flattenDF.select(explode(col("nombre")).alias("nombre"))
    #nombreDF.show()

    #cantidadDF = flattenDF.select(explode(col("cantidad")).alias("cantidad"))
    #cantidadDF.show()

    #nombreDF.join(cantidadDF)
    #nombreDF.show()

    # flattenDF.filter(flattenDF.nombre == "banano").show()
    #flattenDF.select("nombre").distinct().show()
    # df.select("columnname").distinct().show()





def total_cajas(data):
    print("Generate file 'total_cajas.csv'")
    data.write.csv("output/total_cajas.csv")

def metricas(data):
    print("Generate file 'metricas.csv'")
    data.write.csv("output/metricas.csv")

def main(args=None):
    parser = get_parser()
    args = parser.parse_args(args)

    # Create spark session
    spark = spark_session()

    # schema = ['numero_caja','compras']
    # Read cajas_*.json files
    df = spark.read.option("multiline","true").json(args.folder)
    df.printSchema()
    print(df.schema.fieldNames)
    df.show()

    # Generate file 'total_productos.csv'
    total_productos(df)

    # Generate file 'total_cajas.csv'
    # total_cajas(df)

    # Generate file 'metricas.csv'
    # metricas(df)

if __name__ == '__main__':
    main()