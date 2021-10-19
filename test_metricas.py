from programaestudiante import metricas

def test_full_metricas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos/")

    actual_ds = metricas(spark_session, df)

    expected_ds = spark_session.createDataFrame(
        [
            ('caja_con_mas_ventas', '3'),
            ('caja_con_menos_ventas', '45'),
            ('percentil_25_por_caja', '65550'),
            ('percentil_50_por_caja', '76700'),
            ('percentil_75_por_caja', '89050'),
            ('producto_mas_vendido_por_unidad', 'mango'),
            ('producto_de_mayor_ingreso', 'sandia'),
        ],
        ['metrica', 'valor'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()