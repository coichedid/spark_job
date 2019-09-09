
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

#PROVENIÊNCIA
############################

dataflow_tag = "prov-df"
df = Dataflow(dataflow_tag)

##PROVENIÊNCIA PROSPECTIVA
#Transformação para extrair o primeiro stats: ExtrairStats1
tf1 = Transformation("ExtrairStats1")
tf1_input = Set("iExtrairStats1", SetType.INPUT,
    [Attribute("STATS1", AttributeType.TEXT)])

tf1_output = Set("oExtrairStats1", SetType.OUTPUT,
  [Attribute("task", AttributeType.TEXT),
  Attribute("current_time", AttributeType.TEXT),
  Attribute("datafiles", AttributeType.TEXT),
  Attribute("tables", AttributeType.TEXT),
  Attribute("aggregation_unit", AttributeType.TEXT),
  Attribute("csv_separator", AttributeType.TEXT)])

tf1.set_sets([tf1_input, tf1_output])
df.add_transformation(tf1)
df.save()

t1 = Task(1, dataflow_tag, "ExtrairStats1")
t1_input = DataSet("iExtrairStats1", [Element([task, current_time, datafiles, tables, aggregation_unit, csv_separator])])
t1.add_dataset(t1_input)
t1.begin()

if __name__ == "__main__":
    st_time_total = time.time()
    logger = helpers.get_logger()
    (sc,spark) = helpers.get_spark_session()
    logger.info('Inicializando o processador Spark')
    processador = ProcessadorSparkClass(logger, spark)
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
        'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'attributes':{
            'datafiles':lista_datafiles,
            'tables': lista_tabelas,
            'aggregation_unit': aggreg_unit,
            'csv_separator': sep
        }

    }
    # TODO: Publicar o início do fluxo com a variavel stats
    #######################
    # Finalizando extracao de proveniencia
    '''
    t1_output = DataSet("oExtrairStats1", [Element([["task"], ["current_time"], ["datafiles"], ["tables"], ["aggregation_unit"], ["csv_separator"]])])
    t1.add_dataset(t1_output)
    t1.end()
    '''
    ############
    
    t1_output = DataSet("oExtrairStats1", [Element([task, current_time, datafiles, tables, aggregation_unit, csv_separator])])
    t1.add_dataset(t1_output)
    t1.end()
    ################# 
    print(stats)
    
    st_time = time.time()
    processador.load_data(lista_datafiles, lista_tabelas, sep)
    runtime = time.time() - st_time

    stats = {
        'task': 'load_data',
        'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'elapsed_time': runtime
    }

    # TODO: Publicar o tempo para carregar as tabelas com a variavel stats

    st_time = time.time()
    stats = processador.get_initial_data_stats()
    runtime = time.time() - st_time
    stats = {
        'task': 'initial_data_stats',
        'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'elapsed_time': runtime,
        'attributes':stats
    }
    # TODO: Publicar as estatisticas do dados carregados
    # Estrutura da variavel stats:
    # {
    #   'task': 'initial_data_stats',
    #   'elapsed_time': tempo em segundos
    #   'attributes': {
    #       num_tabelas: numero de tabelas carregadas
    #       tabelas: {
    #           nome_da_tabela: Schema da Tabela
    #       }
    #   }
    # }
    logger.info('Estatisticas dos dados:')
    logger.info(stats)

    st_time = time.time()
    df = processador.process_data(aggreg_unit)
    runtime = time.time() - st_time
    stats = {
        'task': 'process_data',
        'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'elapsed_time': runtime,
        'attributes':{
            'aggreg_unit': aggreg_unit
        }
    }
    # TODO: Publicar execução do process_data com a variável stats
    st_time = time.time()
    lista_carga = processador.convert_spark_df_to_list(df)
    runtime_conv = time.time() - st_time
    stats = {
        'task': 'convert_data_to_list',
        'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'elapsed_time': runtime_conv,
        'attributes':{
            'carga':lista_carga
        }
    }
    # TODO: Publicar a conversão de dados da carga com a variável stats
    logger.info(stats)
    logger.info(lista_carga)
    runtime_total = time.time() - st_time_total
    stats = {
        'task': 'end_job',
        'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'elapsed_time_total': runtime_total,
        'attributes':{
            'datafiles':lista_datafiles,
            'tables': lista_tabelas,
            'aggregation_unit': aggreg_unit,
            'csv_separator': sep
        }
    }
    # TODO: Publicar o fim do job com a variável stats

    logger.info(stats)
    sc.stop()


