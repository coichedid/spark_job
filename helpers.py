"""
.. module:: helpers
    :platform: Unix, Windows

.. note::
    Módulo com funções de apoio para mocking de dados

.. moduleauthor:: `Clovis Chedid <clovis.chedid@ons.org.br>`
"""

import json
import pandas as pd
import logging

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pandas.testing import assert_frame_equal

def extractDictAFromB(A,B):
    return dict([(k,B[k]) for k in A.keys() if k in B.keys()])


def load_pandas_dataframe(filename, sep):
    """Carrega um dos arquivos CSV existentes na pasta "tests/data".

    :param string filename: Nome do arquivo CSV.
    :return: Dataframe carregado.
    :rtype: pandas.DataFrame

    """

    df = pd.read_csv(filename, sep=sep)
    return df

def register_spark_table(table_name, original_df, spark, cast = True):
    """Cria uma tabela no spark a partir de um dataframe pandas.

    :param string table_name: Nome da tabela a ser registrada.
    :param pandas.DataFrame original_df: Dataframe que será registrado como tabela.
    :param pyspark.sql.SparkSession spark: Sessão do spark local.
    :return:
    :rtype: void

    """
    df = original_df
    if cast:
        df = spark.createDataFrame(df)
    df.registerTempTable(table_name)

def removeSparkTable(table_name, spark):
    """Remove o registro de uma tabela do spark.

    :param string table_name: Nome da tabela que será removida.
    :return:
    :rtype: void

    """
    spark.dropTempTable(table_name)

def get_logger():
    """Prepara o logger do test.

    :return:
    :rtype: void

    """
    ## Setting up logging service
    MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
    logger = logging.getLogger('spark_job')
    logger.setLevel(logging.INFO)
    return logger

def cast_datetime(df, col):
    """Converte uma coluna string em um datetime.

    :param pandas.DataFrame df: Dataframe contendo a coluna a ser transformada.
    :param string col: Nome da coluna para ser convertida.
    :return: Dataframe atualizado.
    :rtype: pandas.DataFrame

    """
    df[col] = pd.to_datetime(df[col])
    return df

def cast_string(df, col):
    """Converte uma coluna string em um datetime.

    :param pandas.DataFrame df: Dataframe contendo a coluna a ser transformada.
    :param string col: Nome da coluna para ser convertida.
    :return: Dataframe atualizado.
    :rtype: pandas.DataFrame

    """
    df[col] = df[col].apply(str)
    return df

def get_spark_session():
    """ creating a spark context"""
    conf = (SparkConf().setMaster("local[2]").setAppName("spark_job"))
    sc = SparkContext(conf=conf)
    sk = SparkSession.builder.getOrCreate()
    sk.sparkContext.setLogLevel("WARN")

    return (sc, sk)

def deserialize_params(params):
    """Interpreta a string passada como valor;valor e retorna uma lista

    :param string params: Serialização da coleção de valor separado por ponto e virgula.
    :return: Lista de valores.
    :rtype: list(string)

    """

    return params.split(';')
