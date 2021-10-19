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