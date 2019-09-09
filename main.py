elapsedtimetotal
from dfa_lib_python.dataflow import Dataflow
from dfa_lib_python.transformation import Transformation
from dfa_lib_python.attribute import Attribute
from dfa_lib_python.attribute_type import AttributeType
from dfa_lib_python.set import Set
from dfa_lib_python.set_type import SetType
from dfa_lib_python.task import Task
from dfa_lib_python.dataset import DataSet
from dfa_lib_python.element import Element

##################
import os
import sys
import time
import datetime

import helpers
from spark_job.ProcessadorSpark import ProcessadorSparkClass

if __name__ == "__main__":
    st_time_total = time.time()
    logger = helpers.get_logger()
    (sc,spark) = helpers.get_spark_session()
    args = sys.argv

    if '--datafiles' not in args or '--table_names' not in args or '--aggreg_unit' not in args or '--sep' not in args:
        error = """Argumentos obrigatórios:
            - datafiles: Arquivos de dados para considerar
            - table_name: nomes das tabelas do spark
            - aggreg_unit: [diario, mensal]
        """
        raise Exception(error)

    # lista_datafiles = ['tb1', 'tb2', 'tb3', 'tb4', 'tb5', 'tb6', ]
    # lista_tabelas = ['sp_tb1', 'sp_tb2', 'sp_tb3', 'sp_tb4', 'sp_tb5', 'sp_tb6', ]
    lista_datafiles = helpers.deserialize_params(args[2])
    lista_tabelas = helpers.deserialize_params(args[4])
    aggreg_unit = args[6]
    sep = args[8]

    stats = {
        'task': 'start_job',
        'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'attributes':{
            'datafiles':lista_datafiles,
            'tables': lista_tabelas,
            'aggregationunit': aggreg_unit,
            'csvseparator': sep
        }

    }

    #PROVENIÊNCIA
    ############################

    dataflow_tag = "prov-df"
    df = Dataflow(dataflow_tag)

    logger.info('Inicializando o processador Spark')
    processador = ProcessadorSparkClass(logger, spark, df)

    ##PROVENIÊNCIA PROSPECTIVA
    #Transformação para extrair o primeiro stats: ExtrairStats1
    tf1 = Transformation('load_data') ## Usando o nome da task spark
    tf1_input = Set("i{}1".format('load_data'), SetType.INPUT,
        [
            Attribute("datafiles", AttributeType.TEXT),
            Attribute("tables", AttributeType.TEXT),
            Attribute("currenttime", AttributeType.TEXT),
            Attribute("aggregationunit", AttributeType.TEXT),
            Attribute("csvseparator", AttributeType.TEXT)
        ])

    tf1_output = Set("o{}1".format('load_data'), SetType.OUTPUT,
      [
            Attribute("currenttime", AttributeType.TEXT),
            Attribute("elapsedtime", AttributeType.NUMERIC)
      ])

    tf1.set_sets([tf1_input, tf1_output])
    df.add_transformation(tf1)
    df.save()

    t1 = Task(1, dataflow_tag, "load_data")
    t1_input = DataSet("i{}1".format('load_data'), [Element([','.join(stats['attributes']['datafiles']),','.join(stats['attributes']['tables'], stats['currenttime'], stats['attributes']['aggregationunit'], stats['attributes']['csvseparator'])])])
    t1.add_dataset(t1_input)
    t1.begin()

    st_time = time.time()
    processador.load_data(lista_datafiles, lista_tabelas, sep)
    runtime = time.time() - st_time

    stats = {
        'task': 'load_data',
        'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'elapsedtime': runtime
    }

    # TODO: Publicar o início do fluxo com a variavel stats
    #######################
    # Finalizando extracao de proveniencia
    ############

    t1_output = DataSet("o{}1".format('load_data'), [Element([stats['currenttime'],stats['elapsedtime']])])
    t1.add_dataset(t1_output)
    t1.end()
    #################

    # TODO: Publicar o tempo para carregar as tabelas com a variavel stats

    ##PROVENIÊNCIA PROSPECTIVA
    #Transformação para extrair o primeiro stats: ExtrairStats1
    # tf2 = Transformation('initial_data_stats') ## Usando o nome da task spark
    # tf2_input = Set("i{}1".format('initial_data_stats'), SetType.INPUT,
    #     [
    #         Attribute("currenttime", AttributeType.TEXT)
    #     ])
    #
    # tf2_output = Set("o{}1".format('load_data'), SetType.OUTPUT,
    #   [
    #         Attribute("currenttime", AttributeType.TEXT),
    #         Attribute("elapsedtime", AttributeType.NUMERIC)
    #   ])
    #
    # tf2.set_sets([tf2_input, tf2_output])
    # df.add_transformation(tf2)
    # df.save()
    # t2 = Task(2, dataflow_tag, "initial_data_stats")
    # t2_input = DataSet("i{}1".format('initial_data_stats'), [Element([datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")])])
    # t2.add_dataset(t2_input)
    # t2.begin()
    #
    # st_time = time.time()
    # stats = processador.get_initial_data_stats()
    # runtime = time.time() - st_time
    # stats = {
    #     'task': 'initial_data_stats',
    #     'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    #     'elapsedtime': runtime,
    #     'attributes':stats
    # }
    # t2_output = DataSet("o{}1".format('initial_data_stats'), [Element([stats['currenttime'],stats['elapsedtime']])])
    # t2.add_dataset(t2_output)
    # t2.end()
    # # TODO: Publicar as estatisticas do dados carregados
    # # Estrutura da variavel stats:
    # # {
    # #   'task': 'initial_data_stats',
    # #   'elapsedtime': tempo em segundos
    # #   'attributes': {
    # #       num_tabelas: numero de tabelas carregadas
    # #       tabelas: {
    # #           nome_da_tabela: Schema da Tabela
    # #       }
    # #   }
    # # }
    # logger.info('Estatisticas dos dados:')
    # logger.info(stats)
    #
    # ##PROVENIÊNCIA PROSPECTIVA
    # #Transformação para extrair o primeiro stats: ExtrairStats1
    # tf3 = Transformation('process_data') ## Usando o nome da task spark
    # tf3_input = Set("i{}1".format('process_data'), SetType.INPUT,
    #     [
    #         Attribute("currenttime", AttributeType.TEXT),
    #         Attribute("aggregationunit", AttributeType.TEXT)
    #     ])
    #
    # tf3_output = Set("o{}1".format('process_data'), SetType.OUTPUT,
    #   [
    #         Attribute("currenttime", AttributeType.TEXT),
    #         Attribute("elapsedtime", AttributeType.NUMERIC)
    #   ])
    #
    # tf3.set_sets([tf3_input, tf3_output])
    # df.add_transformation(tf3)
    # df.save()
    #
    # t3 = Task(3, dataflow_tag, "process_data")
    # t3_input = DataSet("i{}1".format('process_data'), [Element([aggreg_unit, datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")])])
    # t3.add_dataset(t2_input)
    # t3.begin()
    #
    # st_time = time.time()
    # df = processador.process_data(aggreg_unit)
    # runtime = time.time() - st_time
    # stats = {
    #     'task': 'process_data',
    #     'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    #     'elapsedtime': runtime,
    #     'attributes':{
    #         'aggreg_unit': aggreg_unit
    #     }
    # }
    #
    # t3_output = DataSet("o{}1".format('process_data'), [Element([stats['currenttime'],stats['elapsedtime']])])
    # t3.add_dataset(t3_output)
    # t3.end()
    #
    # ##PROVENIÊNCIA PROSPECTIVA
    # #Transformação para extrair o primeiro stats: ExtrairStats1
    # tf4 = Transformation('convert_data_to_list') ## Usando o nome da task spark
    # tf4_input = Set("i{}1".format('convert_data_to_list'), SetType.INPUT,
    #     [
    #         Attribute("currenttime", AttributeType.TEXT),
    #         Attribute("aggregationunit", AttributeType.TEXT)
    #     ])
    #
    # tf4_output = Set("o{}1".format('convert_data_to_list'), SetType.OUTPUT,
    #   [
    #         Attribute("currenttime", AttributeType.TEXT),
    #         Attribute("elapsedtime", AttributeType.NUMERIC),
    #         Attribute("num_records", AttributeType.NUMERIC)
    #   ])
    #
    # tf4.set_sets([tf4_input, tf4_output])
    # df.add_transformation(tf4)
    # df.save()
    #
    # t4 = Task(4, dataflow_tag, "convert_data_to_list")
    # t4_input = DataSet("i{}1".format('convert_data_to_list'), [Element([aggreg_unit, datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")])])
    # t4.add_dataset(t2_input)
    # t4.begin()
    #
    # # TODO: Publicar execução do process_data com a variável stats
    # st_time = time.time()
    # lista_carga = processador.convert_spark_df_to_list(df)
    # runtime_conv = time.time() - st_time
    # stats = {
    #     'task': 'convert_data_to_list',
    #     'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    #     'elapsedtime': runtime_conv,
    #     'attributes':{
    #         'carga':lista_carga
    #     }
    # }
    #
    # t4_output = DataSet("o{}1".format('convert_data_to_list'), [Element([stats['currenttime'],stats['elapsedtime'], len(lista_carga)])])
    # t4.add_dataset(t4_output)
    # t4.end()
    #
    # # TODO: Publicar a conversão de dados da carga com a variável stats
    # logger.info(stats)
    # logger.info(lista_carga)
    # runtime_total = time.time() - st_time_total
    # stats = {
    #     'task': 'end_job',
    #     'currenttime': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    #     'elapsedtimetotal': runtime_total,
    #     'attributes':{
    #         'datafiles':lista_datafiles,
    #         'tables': lista_tabelas,
    #         'aggregationunit': aggreg_unit,
    #         'csvseparator': sep
    #     }
    # }
    # # TODO: Publicar o fim do job com a variável stats
    #
    # logger.info(stats)
    sc.stop()
