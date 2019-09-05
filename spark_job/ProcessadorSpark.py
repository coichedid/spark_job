import os
import sys
# sys.path.insert(0, os.path.abspath('../../'))
sys.path.append(os.path.abspath('../'))

import helpers
class ProcessadorSparkClass(object):
    """Classe de processamento de dados com spark"""
    def __init__(self, logger, spark_session):
        """Construtor.

        :param logging logger: instancia de logging.
        :param pyspark.sql.SparkSession spark_session: Sessão spark inicializada.
        :return: Instancia do processador
        :rtype: ProcessadorSpark

        """
        super(ProcessadorSparkClass, self).__init__()
        self.logger = logger
        self.spark = spark_session
        self.logger.info('Processador inicializado com sucesso.')

    def load_pandas_data(self, datafiles):
        """Carrega os arquivos de dados em Dataframes Pandas.

        :param list(string) datafiles: Lista de nomes de arquivos a serem carregados.
        :return: Lista de dataframes pandas.
        :rtype: dict(datafile,pandas.DataFrame)

        """
        pandas_dfs = {}
        for d in datafiles:
            # Os arquivos são depositados em ../data
            filename = './data/{}.csv'.format(d)
            df = helpers.load_pandas_dataframe(filename)
            pandas_dfs[d] = df
        return pandas_dfs

    def load_spark_tables(self, pandas_dfs, pandas_spark_names):
        """Cria tabelas em memória para cada dataframe pandas.

        :param dict(string, pandas.DataFrame) pandas_dfs: Coleção de dataframes pandas indexados pelo nome do arquivo.
        :param list(tuple(string,string)) pandas_spark_names: De Para entre nome do arquivo e tabela spark.
        :return:
        :rtype:

        """

        # Para cada dtaframe carregado instancia uma tabela no spark
        # Usamos o pandas como intermediário para não precisar fazer type cast com spark.
        for (pandas_name, spark_name) in pandas_spark_names:
            df = pandas_dfs[pandas_name]
            helpers.register_spark_table(spark_name, df, self.spark)

    def load_data(self, datafiles, spark_names):
        """Carrega os dados em memória.

        :param list(string) datafiles: Lista de nomes de arquivos, relativos a ./data.
        :param list(string) spark_names: Nomes das tabelas spark.
        :return:
        :rtype:

        """

        # para cada nome de arquivo, encontra o nome da tabela relativa (mesma posição)
        # carrega o arquivo
        # registra a tabela em memória no spark
        df_s = self.load_pandas_data(datafiles)

        # cria a lista de tuplas (nome do arquivo, nome da tabela)
        pandas_spark_names = [(datafile, spark_names[idx]) for (idx, datafile) in enumerate(datafiles)]
        self.load_spark_tables(df_s, pandas_spark_names)
        self.logger.info('Dados dos arquivos {} carregados como as tabelas {}'.format(datafiles, spark_names))

    def convert_spark_df_to_list(self, df):
        """Coleta os dados e converte para uma lista de listas.

        :param pyspark.sql.Dataframe df: Dataframe spark.
        :return: Dados coletados.
        :rtype: list(list)

        """
        to_list = [list(row) for row in df.collect()]
        return to_list

    def simple_select(self, table_name):
        """Seleciona os dados de uma tabela.

        :param string table_name: Nome da tabela.
        :return: Lista com os dados da tabela.
        :rtype: list(tuples)

        """
        sql = "SELECT * FROM {}".format(table_name)
        df = self.spark.sql(sql)
        return self.convert_spark_df_to_list(df)

    def process_data(self):
        """Executa o processamento dos dados.

        :return: Lista de tuplas (subsistema, valor_carga).
        :rtype: type

        """
