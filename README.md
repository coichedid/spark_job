# Script de cálculo de Carga

Este script carrega dados de geração e de intercâmbio e executa o cálculo da carga diária ou mensal.

## Dependências

Este projeto é executado com **python 3.7**
É necessário primeiro atualizar o python.

## Instruções de Instalação


1. Clonar o projeto https://github.com/coichedid/spark_job.git
```bash
cd <pasta do projeto>
git clone https://github.com/coichedid/spark_job
cd spark_job
```

2. Criar um ambiente virtual python
```bash
python3.7 -m venv ./venv
source venv/bin/activate
```

3. Instalar as dependências do projeto
```bash
pip3.7 install -r requirements.txt
```

## Instruções de desenvolvimento

O projeto possui os seguintes módulos e pacotes:
1. main.py - módulo onde tem a lógica de execução do job
2. helpers.py - módulo com os métodos auxiliares do projeto
3. spark_job - pacote com a classe com a lógica de cálculo da carga
  3.1. ProcessadorSpark.py - classe de cálculo da carga

Para incluir as extrações de proveniência, foi criado em vários pontos do código
uma váriável chamada stats, que tem informações a respeito do ponto de execução.
Esta variável tem o formato
```javascript
  {
    task: 'NOME DA TAREFA EXECUTADA',
    current_time: 'MOMENTO DA EXECUÇÃO DA TAREFA',
    elapsed_time: 'TEMPO GASTO NA EXECUÇÃO DA TAREFA',
    attributes: {
      ATRIBUTOS ESPECÍFICOS DE CADA TAREFA...
    }
  }
```

Os pontos relevantes para extração de proveniência foram marcados com a expressão:

```javascript
# TODO: Publicar a...
```

Analisar o código e ver se existem mais pontos interessantes.

Os arquivos a serem interceptados são:

* [main.py](https://github.com/coichedid/spark_job/blob/master/main.py)
* [spark_job/ProcessadorSpark.py](https://github.com/coichedid/spark_job/blob/master/spark_job/ProcessadorSpark.py)

Pesquise pelo `# TODO: ...` no corpo dos dois arquivos e utilize a variável `stats` para pegar os dados relevantes.

A variável `stats` é um dict. Você acessa seus atributos como um hashmap: `stats['task']` ou `stats['attributes']['aggregation_unit']`, por exemplo.

## Instruções de execução

Na pasta do projeto, executar os comandos:

1. para cálculo da carga diária:
```bash
python3.7 main.py --datafiles "geracao;intercambio;recursos" --table_names "geracao;intercambio;recursos" --aggreg_unit diario --sep ";"
```

2. para cálculo da carga mensal:
```bash
python3.7 main.py --datafiles "geracao;intercambio;recursos" --table_names "geracao;intercambio;recursos" --aggreg_unit mensal --sep ";"
```

Os dados estão na pasta "data".

Executar os dois comandos para que seja capturado dois tipos diferentes de proveniência.
Executar os dois comandos diferentes vezes para variar o tempo de processamento.
