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

def test_one_metricas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut2.json")

    actual_ds = metricas(spark_session, df)

    expected_ds = spark_session.createDataFrame(
        [
            ('caja_con_mas_ventas', '45'),
            ('caja_con_menos_ventas', '45'),
            ('percentil_25_por_caja', '15308'),
            ('percentil_50_por_caja', '15308'),
            ('percentil_75_por_caja', '15308'),
            ('producto_mas_vendido_por_unidad', 'manzana'),
            ('producto_de_mayor_ingreso', 'manzana'),
        ],
        ['metrica', 'valor'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_null_cantidad_metricas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut3.json")

    actual_ds = metricas(spark_session, df)

    expected_ds = spark_session.createDataFrame(
        [
            ('caja_con_mas_ventas', '45'),
            ('caja_con_menos_ventas', '45'),
            ('percentil_25_por_caja', '15198'),
            ('percentil_50_por_caja', '15198'),
            ('percentil_75_por_caja', '15198'),
            ('producto_mas_vendido_por_unidad', 'manzana'),
            ('producto_de_mayor_ingreso', 'aguacate'),
        ],
        ['metrica', 'valor'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_null_precio_metricas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut4.json")

    actual_ds = metricas(spark_session, df)

    expected_ds = spark_session.createDataFrame(
        [
            ('caja_con_mas_ventas', '45'),
            ('caja_con_menos_ventas', '45'),
            ('percentil_25_por_caja', '15198'),
            ('percentil_50_por_caja', '15198'),
            ('percentil_75_por_caja', '15198'),
            ('producto_mas_vendido_por_unidad', 'manzana'),
            ('producto_de_mayor_ingreso', 'aguacate'),
        ],
        ['metrica', 'valor'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_caja_con_mas_ventas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/metricas/")

    actual_ds = metricas(spark_session, df)
    actual_ds.show()
    
    assert actual_ds.collect()[0][1] == '100'

def test_caja_con_menos_ventas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/metricas/")

    actual_ds = metricas(spark_session, df)
    actual_ds.show()
    
    assert actual_ds.collect()[1][1] == '0'

def test_percentil_25_por_caja(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/metricas/")

    actual_ds = metricas(spark_session, df)
    actual_ds.show()
    
    assert actual_ds.collect()[2][1] == '250'

def test_percentil_50_por_caja(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/metricas/")

    actual_ds = metricas(spark_session, df)
    actual_ds.show()
    
    assert actual_ds.collect()[3][1] == '500'

def test_percentil_75_por_caja(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/metricas/")

    actual_ds = metricas(spark_session, df)
    actual_ds.show()
    
    assert actual_ds.collect()[4][1] == '750'

def test_producto_mas_vendido_por_unidad(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/metricas/")

    actual_ds = metricas(spark_session, df)
    actual_ds.show()
    
    assert actual_ds.collect()[5][1] == 'kiwi'

def test_producto_de_mayor_ingreso(spark_session):
    
    df = spark_session.read.option("multiline","true").json("datos_ut/metricas/")

    actual_ds = metricas(spark_session, df)
    actual_ds.show()
    
    assert actual_ds.collect()[6][1] == 'kiwi'