import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

# spark-submit --master yarn --deploy-mode cluster extrator.py

conf = SparkConf().setAppName('extrator').set('spark.executor.memoryOverhead', 1000)
sc = SparkContext.getOrCreate(conf)

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sql = """
SELECT
    din_referenciainicio,
    id_subsistema,
    val_intercambio
FROM zona_consumo.g_intercambiodesubsistema WHERE
    p_ano = '2018'
    AND p_mes = '1'
    AND cod_periodograndeza = 'DI'
    AND tip_classificacao = 'Consistido'
"""
df = spark.sql(sql)
df.repartition(1).write.csv("s3://ons-datalake-carbondata/data/intercambio", sep=';')

sql = """
SELECT
    din_referenciainicio,
    val_geracao,
    id_recursogeracao,
    guid_recursogeracao
FROM zona_consumo.g_geracao
WHERE
    p_ano = '2018'
    and p_mes = '1'
    and cod_periodograndeza = 'DI'
    and tip_classificacao = 'Preliminar'
"""
df = spark.sql(sql)
df.repartition(1).write.csv("s3://ons-datalake-carbondata/data/geracao", sep=';')

sql = """
SELECT
    id_recursogeracao,
    guid_recursogeracao,
    id_subsistema
FROM zona_consumo.e_recursogeracao
WHERE
    id_subsistema IS NOT NULL
"""
df = spark.sql(sql)
df.repartition(1).write.csv("s3://ons-datalake-carbondata/data/recursos.csv", sep=';')
