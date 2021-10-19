from programaestudiante import total_cajas

def test_full_total_cajas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos/")

    actual_ds = total_cajas(df)

    expected_ds = spark_session.createDataFrame(
        [
            (0, 86800),
            (1, 76700),
            (3, 92800),
            (2, 65550),
            (4, 89050),
            (45, 5132),
        ],
        ['numero_caja', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_one_total_cajas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut2.json")

    actual_ds = total_cajas(df)

    expected_ds = spark_session.createDataFrame(
        [
            (45, 15308),
        ],
        ['numero_caja', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_order_total_cajas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/metricas/")

    actual_ds = total_cajas(df)

    expected_ds = spark_session.createDataFrame(
        [
            (0, 0),
            (50, 500),
            (25, 250),
            (100, 1000),
            (75, 750),
        ],
        ['numero_caja', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_null_cantidad_total_cajas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut3.json")

    actual_ds = total_cajas(df)

    expected_ds = spark_session.createDataFrame(
        [
            (45, 15198),
        ],
        ['numero_caja', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_null_precio_total_cajas(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut4.json")

    actual_ds = total_cajas(df)

    expected_ds = spark_session.createDataFrame(
        [
            (45, 15198),
        ],
        ['numero_caja', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()