from programaestudiante import total_productos

def test_full_total_productos(spark_session):

    df = spark_session.read.option("multiline","true").json("datos/")

    actual_ds = total_productos(df)
    
    expected_ds = spark_session.createDataFrame(
        [
            ('kiwi', 82),
            ('pera', 63),
            ('papaya', 54),
            ('brocoli', 2),
            ('mango', 97),
            ('sandia', 73),
            ('melocoton', 57),
            ('pina', 79),
            ('manzana', 66),
            ('ciruela', 63),
            ('guayaba', 62),
            ('limon', 58),
            ('higo', 34),
            ('aguacate', 56),
            ('melon', 44),
            ('banano', 74),
            ('naranja', 96),
        ],
        ['nombre', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_draw_total_productos(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut1.json")
    df.show()

    actual_ds = total_productos(df)

    expected_ds = spark_session.createDataFrame(
        [
            ('pera', 2),
            ('brocoli', 2),
            ('manzana', 3),
            ('aguacate', 3),
            ('banano', 3),
        ],
        ['nombre', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_one_total_productos(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut2.json")
    df.show()

    actual_ds = total_productos(df)

    expected_ds = spark_session.createDataFrame(
        [
            ('manzana', 15),
        ],
        ['nombre', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()

def test_null_total_productos(spark_session):

    df = spark_session.read.option("multiline","true").json("datos_ut/caja_ut3.json")
    df.show()

    actual_ds = total_productos(df)

    expected_ds = spark_session.createDataFrame(
        [
            ('pera', None),
            ('brocoli', 2),
            ('manzana', 3),
            ('aguacate', 3),
            ('banano', 3),
        ],
        ['nombre', 'total_vendido'])

    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()