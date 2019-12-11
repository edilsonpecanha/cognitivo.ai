# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import json

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as funcs

# Iniciando o contexto do Spark
sc = SparkContext()
sqlContext = SQLContext(sc)
sc.setLogLevel("WARN")
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

try:

    '''
    Criando um schema para acertar o nome das colunas email e name que estam trocados.
    Por conta da modificação acima, uma mensagem WARN será exiba avisando que o schema não está em conformidade com o head 
    Dessa forma o nome da coluna faz mais sentido com o conteúdo.    
    Modificando o tipo do campo ID. Os outros campos serão alterados mais a frente conforme requisito.      
    '''
    schema_load = StructType([StructField("id", IntegerType(), True),
                              StructField("email", StringType(), True),
                              StructField("name", StringType(), True),
                              StructField("phone", StringType(), True),
                              StructField("address", StringType(), True),
                              StructField("age", StringType(), True),
                              StructField("create_date", StringType(), True),
                              StructField("update_date", StringType(), True)])

    # Carregando os dados do arquivo csv para um dataframe
    df = spark.read.csv('data/input/users/load.csv', header=True, inferSchema=False, schema=schema_load)

    '''
    Requisito 2
    Faz a remoção dos dados duplicados utilizando Window.partitionBy e a função rank
    '''
    window = Window.partitionBy(df['id']).orderBy(df['update_date'].desc())
    df_rnk = df.select('*', funcs.rank().over(window).alias('rank')).filter(
        funcs.col('rank') == 1).drop('rank')

    '''
    Requisito 3
    Carregando json com o nome da coluna e o tipo a ser modificado
    '''
    types_mapping = json.loads(open('config/types_mapping.json').read())
    for tm in types_mapping:
        # Alterando o tipo da coluna
        df_rnk = df_rnk.withColumn(tm, funcs.col(tm).cast(types_mapping[tm]))

    # Mostrando schema no final do processo
    df_rnk.printSchema()
    df_rnk.show()

    '''
    Requisito 1
    Exportando para o formato ORC.
    O formato ORC tem uma melhor compressão dos dados comparado com o formato também colunar Parquet. 
    O tempo de resposta na leitura no formato ORC também leva vantagem com relação ao Parquet
    O ORC suporta melhor a evolução do esquema
    '''
    df_rnk.repartition(1).write.orc('data/output/users/orc/')

    '''
    Lendo o arquivo ORC exportado 
    '''
    df_orc = spark.read.orc('data/output/users/orc')
    df_orc.printSchema()
    df_orc.show()

except Exception as e:
    logging.error(e)
    raise
