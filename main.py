
import helpers
from spark_job.ProcessadorSpark import ProcessadorSparkClass



if __name__ == "__main__":
    logger = helpers.get_logger()
    (sc,spark) = helpers.get_spark_session()
    logger.info('Inicializando o processador Spark')
    processador = ProcessadorSparkClass(logger, spark)
    lista_datafiles = ['tb1', 'tb2', 'tb3', 'tb4', 'tb5', 'tb6', ]
    lista_tabelas = ['sp_tb1', 'sp_tb2', 'sp_tb3', 'sp_tb4', 'sp_tb5', 'sp_tb6', ]
    processador.load_data(lista_datafiles, lista_tabelas)
    data = processador.simple_select('sp_tb1')
    logger.info('Dados da tabela sp_tb1 coletados')
    logger.info(data)
    sc.stop()
