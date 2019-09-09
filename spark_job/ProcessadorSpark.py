import os
import sys
import time
import datetime

from dfa_lib_python.dataflow import Dataflow
from dfa_lib_python.transformation import Transformation
from dfa_lib_python.attribute import Attribute
from dfa_lib_python.attribute_type import AttributeType
from dfa_lib_python.set import Set
from dfa_lib_python.set_type import SetType
from dfa_lib_python.task import Task
from dfa_lib_python.dataset import DataSet
from dfa_lib_python.element import Element

# sys.path.insert(0, os.path.abspath('../../'))
sys.path.append(os.path.abspath('../'))

import helpers
class ProcessadorSparkClass(object):
    """Classe de processamento de dados com spark"""
    def __init__(self, logger, spark_session, data_flow, dataflow_tag):
        """Construtor.

        :param logging logger: instancia de logging.
        :param pyspark.sql.SparkSession spark_session: Sessão spark inicializada.
        :return: Instancia do processador
        :rtype: ProcessadorSpark

        """
        super(ProcessadorSparkClass, self).__init__()
        self.logger = logger
        self.spark = spark_session
        self.data_flow = data_flow
        self.dataflow_tag = dataflow_tag
        self.logger.info('Processador inicializado com sucesso.')

    def load_pandas_data(self, datafiles, sep):
        """Carrega os arquivos de dados em Dataframes Pandas.

        :param list(string) datafiles: Lista de nomes de arquivos a serem carregados.
        :return: Lista de dataframes pandas.
        :rtype: dict(datafile,pandas.DataFrame)

        """
        pandas_dfs = {}
        for d in datafiles:
            # Os arquivos são depositados em ../data
            filename = './data/{}.csv'.format(d)
            df = helpers.load_pandas_dataframe(filename, sep)
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
            if spark_name == 'geracao' or spark_name == 'intercambio':
                df = helpers.cast_datetime(df, 'data')
            if spark_name == 'geracao':
                df = helpers.cast_string(df, 'id_usina')
                df = helpers.cast_string(df, 'guid_usina')
            if spark_name == 'recursos':
                df = helpers.cast_string(df, 'id_recurso')
                df = helpers.cast_string(df, 'guid_recurso')
            helpers.register_spark_table(spark_name, df, self.spark)

    def load_data(self, datafiles, spark_names, sep):
        """Carrega os dados em memória.

        :param list(string) datafiles: Lista de nomes de arquivos, relativos a ./data.
        :param list(string) spark_names: Nomes das tabelas spark.
        :return:
        :rtype:

        """

        # para cada nome de arquivo, encontra o nome da tabela relativa (mesma posição)
        # carrega o arquivo
        # registra a tabela em memória no spark
        df_s = self.load_pandas_data(datafiles, sep)

        # cria a lista de tuplas (nome do arquivo, nome da tabela)
        pandas_spark_names = [(datafile, spark_names[idx]) for (idx, datafile) in enumerate(datafiles)]
        self.load_spark_tables(df_s, pandas_spark_names)
        self.logger.info('Dados dos arquivos {} carregados como as tabelas {}'.format(datafiles, spark_names))
        self.datafiles = datafiles
        self.spark_names = spark_names

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

    def aggreg_geracao(self, table_name, aggreg_unit):
        """Agrega os valores de geração para a unidade de agregação.

        :param string table_name: Nome da tabela.
        :param string aggreg_unit: [diario, mensal].
        :return: Dataframe agregado
        :rtype: pyspark.sql.Dataframe

        """
        st_time = time.time()
        tf1 = Transformation('aggreg_geracao') ## Usando o nome da task spark
        tf1_input = Set("i{}1".format('aggreg_geracao'), SetType.INPUT,
            [
                Attribute("tablename", AttributeType.TEXT),
                Attribute("currenttime", AttributeType.TEXT),
                Attribute("aggregationunit", AttributeType.TEXT)
            ])

        tf1_output = Set("o{}1".format('aggreg_intercambio'), SetType.OUTPUT,
          [
                Attribute("currenttime", AttributeType.TEXT),
                Attribute("elapsedtime", AttributeType.NUMERIC),
                Attribute("recordcount", AttributeType.NUMERIC)
          ])

        tf1.set_sets([tf1_input, tf1_output])
        self.data_flow.add_transformation(tf1)
        self.data_flow.save()

        t1 = Task(3, self.dataflow_tag, "aggreg_geracao", "2")
        t1_input = DataSet("i{}1".format('aggreg_geracao'), [Element([table_name,datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), aggreg_unit])])
        t1.add_dataset(t1_input)
        t1.begin()
        sql = ''
        if aggreg_unit == 'diario':
            sql = """
                SELECT r.subsistema, t.data, sum(t.valor) as valor
                FROM {} as t,
                    recursos as r
                WHERE t.guid_usina = r.guid_recurso
                GROUP BY r.subsistema, t.data
            """.format(table_name)
        elif aggreg_unit == 'mensal':
            sql = """
                SELECT r.subsistema, avg(t.valor) as valor
                FROM {} as t,
                    recursos as r
                WHERE t.guid_usina = r.guid_recurso
                GROUP BY r.subsistema
            """.format(table_name)
        df = self.spark.sql(sql)
        c = df.count()
        runtime = time.time() - st_time
        stats = {
            'task': 'aggregate_geracao',
            'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'elapsedtime': runtime,
            'attributes':{
                'aggregationunit': aggreg_unit,
                'tablename': table_name,
                'count': c
            }
        }
        # TODO: Publicar a execução da agregação de geracao com a variável stats
        t1_output = DataSet("o{}1".format('aggreg_geracao'), [Element([stats['currenttime'],stats['elapsedtime'], stats['attributes']['count']])])
        t1.add_dataset(t1_output)
        t1.end()
        self.logger.info(stats)
        return df

    def aggreg_intercambio(self, table_name, aggreg_unit):
        """Agrega os valores de intercambio para a unidade de agregação.

        :param string table_name: Nome da tabela.
        :param string aggreg_unit: [diario, mensal].
        :return: Dataframe agregado
        :rtype: pyspark.sql.Dataframe

        """
        st_time = time.time()
        tf1 = Transformation('aggreg_intercambio') ## Usando o nome da task spark
        tf1_input = Set("i{}1".format('aggreg_intercambio'), SetType.INPUT,
            [
                Attribute("tablename", AttributeType.TEXT),
                Attribute("currenttime", AttributeType.TEXT),
                Attribute("aggregationunit", AttributeType.TEXT)
            ])

        tf1_output = Set("o{}1".format('aggreg_intercambio'), SetType.OUTPUT,
          [
                Attribute("currenttime", AttributeType.TEXT),
                Attribute("elapsedtime", AttributeType.NUMERIC),
                Attribute("recordcount", AttributeType.NUMERIC)
          ])

        tf1.set_sets([tf1_input, tf1_output])
        self.data_flow.add_transformation(tf1)
        self.data_flow.save()

        t1 = Task(3, self.dataflow_tag, "aggreg_intercambio", "3")
        t1_input = DataSet("i{}1".format('aggreg_intercambio'), [Element([table_name,datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), aggreg_unit])])
        t1.add_dataset(t1_input)
        t1.begin()
        sql = ''
        if aggreg_unit == 'diario':
            sql = """
                SELECT t.subsistema, t.data, sum(t.valor) as valor
                FROM {} as t
                GROUP BY t.subsistema, t.data
            """.format(table_name)
        elif aggreg_unit == 'mensal':
            sql = """
                SELECT t.subsistema, avg(t.valor) as valor
                FROM {} as t
                GROUP BY t.subsistema
            """.format(table_name)
        df = self.spark.sql(sql)
        c = df.count()
        runtime = time.time() - st_time
        stats = {
            'task': 'aggregate_intercambio',
            'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'elapsedtime': runtime,
            'attributes':{
                'aggregationunit': aggreg_unit,
                'tablename': table_name,
                'count': c
            }
        }
        # TODO: Publicar a execução da agregação de geracao com a variável stats
        t1_output = DataSet("o{}1".format('aggreg_intercambio'), [Element([stats['currenttime'],stats['elapsedtime'], stats['attributes']['count']])])
        t1.add_dataset(t1_output)
        t1.end()
        self.logger.info(stats)
        return df

    def calculate_carga(self, df_geracao, df_intercambio, aggreg_unit):
        """Calcula a carga global (Geração - Intercambio) unindo os dataframes fornecidos.

        :param pyspark.sql.Dataframe df_geracao: Dados de geracao.
        :param pyspark.sql.Dataframe df_intercambio: Dados de intercambio.
        :param string aggreg_unit: [diario, mensal].
        :return: Dados de carga.
        :rtype: pyspark.sql.Dataframe

        """
        st_time = time.time()
        st_time_total = time.time()
        tf1 = Transformation('calculate_carga') ## Usando o nome da task spark
        tf1_input = Set("i{}1".format('calculate_carga'), SetType.INPUT,
            [
                Attribute("currenttime", AttributeType.TEXT),
                Attribute("aggregationunit", AttributeType.TEXT)
            ])

        tf1_output = Set("o{}1".format('calculate_carga'), SetType.OUTPUT,
          [
                Attribute("currenttime", AttributeType.TEXT),
                Attribute("elapsedtime", AttributeType.NUMERIC),
                Attribute("elapsedtimeloadgeracao", AttributeType.NUMERIC),
                Attribute("elapsedtimeloadintercambio", AttributeType.NUMERIC),
                Attribute("elapsedtimeloadcarga", AttributeType.NUMERIC),
                Attribute("elapsedtimecalccarga", AttributeType.NUMERIC),
                Attribute("elapsedtimecalcstats", AttributeType.NUMERIC),
                Attribute("subsistemamaisdemandante", AttributeType.TEXT),
                Attribute("valormaisalto", AttributeType.NUMERIC)
          ])

        tf1.set_sets([tf1_input, tf1_output])
        self.data_flow.add_transformation(tf1)
        self.data_flow.save()

        t1 = Task(3, self.dataflow_tag, "calculate_carga", "4")
        t1_input = DataSet("i{}1".format('calculate_carga'), [Element([datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), aggreg_unit])])
        t1.add_dataset(t1_input)
        t1.begin()
        helpers.register_spark_table('geracao_aggr', df_geracao, self.spark, False)
        runtime_load_geracao = time.time() - st_time

        st_time = time.time()
        helpers.register_spark_table('intercambio_aggr', df_intercambio, self.spark, False)
        runtime_load_intercambio = time.time() - st_time

        sql = ""
        if aggreg_unit == 'diario':
            sql = """
                SELECT g.subsistema, g.data, (g.valor - i.valor) as valor
                FROM geracao_aggr as g,
                     intercambio_aggr as i
                WHERE g.subsistema = i.subsistema
                  AND g.data = i.data
            """
        elif aggreg_unit == 'mensal':
            sql = """
                SELECT g.subsistema, (g.valor - i.valor) as valor
                FROM geracao_aggr as g,
                     intercambio_aggr as i
                WHERE g.subsistema = i.subsistema
            """
        st_time = time.time()
        df_carga = self.spark.sql(sql)
        runtime_calc = time.time() - st_time
        st_time = time.time()
        helpers.register_spark_table('carga', df_carga, self.spark, False)
        runtime_load_carga = time.time() - st_time

        self.logger.info(df_carga.show())

        # sql = """
        #     SELECT subsistema, valor
        #     FROM
        #         (SELECT subsistema,
        #                valor,
        #                RANK() OVER (PARTITION BY valor ORDER BY valor DESC) as rnk
        #         FROM
        #             carga) as a
        #     WHERE rnk = 1
        #
        # """

        sql = """
            SELECT subsistema, valor
            FROM carga
            ORDER BY valor DESC
        """

        st_time = time.time()
        df_stats = self.spark.sql(sql)
        self.logger.info(df_stats.show())
        subsistema_mais_demandante = df_stats.select('subsistema').collect()[0].subsistema
        valor_mais_alto = df_stats.select('valor').collect()[0].valor
        runtime_stats = time.time() - st_time

        runtime_total = time.time() - st_time_total
        stats = {
            'task': 'calculate_carga',
            'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'elapsedtime': runtime_total,
            'attributes':{
                'aggregation_unit': aggreg_unit,
                'elapsed_time_load_geracao': runtime_load_geracao,
                'elapsed_time_load_intercambio': runtime_load_intercambio,
                'elapsed_time_load_carga': runtime_load_carga,
                'elapsed_time_calc_carga': runtime_calc,
                'elapsed_time_stats': runtime_stats,
                'subsistema_mais_demandante': subsistema_mais_demandante,
                'valor_mais_alto': valor_mais_alto
            }
        }
        # Attribute("currenttime", AttributeType.TEXT),
        # Attribute("elapsedtime", AttributeType.NUMERIC),
        # Attribute("elapsedtimeloadgeracao", AttributeType.NUMERIC),
        # Attribute("elapsedtimeloadintercambio", AttributeType.NUMERIC),
        # Attribute("elapsedtimeloadcarga", AttributeType.NUMERIC),
        # Attribute("elapsedtimecalccarga", AttributeType.NUMERIC),
        # Attribute("elapsedtimecalcstats", AttributeType.NUMERIC),
        # Attribute("subsistemamaisdemandante", AttributeType.TEXT),
        # Attribute("valormaisalto", AttributeType.NUMERIC)
        t1_output = DataSet("o{}1".format('calculate_carga'),
            [
                Element([
                    stats['currenttime'],
                    stats['elapsedtime'],
                    stats['attributes']['elapsed_time_load_geracao'],
                    stats['attributes']['elapsed_time_load_intercambio'],
                    stats['attributes']['elapsed_time_load_carga'],
                    stats['attributes']['elapsed_time_calc_carga'],
                    stats['attributes']['elapsed_time_stats'],
                    stats['attributes']['subsistema_mais_demandante'],
                    stats['attributes']['valor_mais_alto']
                    ])])
        t1.add_dataset(t1_output)
        t1.end()
        # TODO: Publicar calculo da carga com a variável df_stats
        self.logger.info(stats)
        return df_carga

    def process_data(self, aggreg_unit):
        """Executa o processamento dos dados.
        :param string aggreg_unit: [diario, mensal]
        :return: Lista de tuplas (subsistema, valor_carga).
        :rtype: type

        """
        self.logger.info('Iniciando processamento.')
        st_time = time.time()
        df_geracao = self.aggreg_geracao('geracao', aggreg_unit)
        self.logger.info('Geração agregada.')
        df_intercambio = self.aggreg_intercambio('intercambio', aggreg_unit)
        self.logger.info('Intercambio agregado.')
        df_carga = self.calculate_carga(df_geracao, df_intercambio, aggreg_unit)
        self.logger.info('Carga calculada.')
        runtime = time.time() - st_time

        stats = {
            'task': 'process_data',
            'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'elapsed_time': runtime,
            'attributes':{
                'aggregation_unit': aggreg_unit
            }
        }
        # TODO: Publicar process_data com a variável stats
        self.logger.info(stats)

        return df_carga

    def get_initial_data_stats(self):
        """Obtem estatísticas dos dados carregados.

        :return: Dicionário com algumas propriedades dos dados.
        :rtype: dict

        """
        stats = {
            'num_tabelas': len(self.datafiles),
            'tabelas':{}
        }

        ## para cada spark table obtém a quantidade de registros
        ## obtem também o schema
        for t in self.spark_names:
            sql_base = 'select {} from {}'
            sql = sql_base.format('count(0) as c', t)
            df = self.spark.sql(sql)
            c = df.select('c').collect()[0].c
            stats['tabelas'][t] = {
                'count':c
            }
            sql = sql_base.format('*', t)
            df = self.spark.sql(sql)
            stats['tabelas'][t]['schema'] = df.schema.jsonValue()
        return stats
