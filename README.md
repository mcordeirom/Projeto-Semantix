# projeto_semantix
## Campanha Nacional de vacinação contra COVID

O projeto foi dividido em dois níveis, básico e avançado. Esse projeto engloba parcialmente o nivel basico.

Os exercícios foram feitos utilizando python notebooks e alguns comandos direto no terminal.


---
## Nível Básico

Dados:  https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar

Referência das visualizações:
  - Site: https://covid.saude.gov.br
  - Guia do site: Painel Geral

---

*O projeto foi realizado utilizando Docker e Docker-Compose através do WSL2*

Primeiramente, entrar na pasta onde estão instalados os arquivos docker-compose e inicializar os serviços através do ```docker-compose up -d```

Os arquivos .yml estão disponíveis em : XXXXX

### 1. Enviar os dados para o hdfs

#### 1.1 - Primeiramente é feito download dos dados e posterior extração dos arquivos em pasta local.
   
#### 1.2 - Utilizar ```docker cp``` para fazer a cópia dos arquivos para o namenome, através do segunite script:
   
   ```
   docker cp HIST_PAINEL_COVIDBR_2020_Parte1_06jul2021.csv namenode:/
   ```
   ```
   docker cp HIST_PAINEL_COVIDBR_2020_Parte2_06jul2021.csv namenode:/
   ```
   ```
   docker cp HIST_PAINEL_COVIDBR_2021_Parte1_06jul2021.csv namenode:/
   ```
   ```
   docker cp HIST_PAINEL_COVIDBR_2021_Parte2_06jul2021.csv namenode:/
   ```
   
#### 1.3 - Entrar no container namenode ```docker exec -it namenode bash``` e listar os arquivos através do ```ls```

![foto03_projeto_dev](https://user-images.githubusercontent.com/62483710/125337812-ceef0c80-e325-11eb-9aea-ef1feb4877b4.PNG)

#### 1.4 -De dentro do namenode, criar a seguinte estrutura de pasta **hdfs:/user/projeto/semantix** através do comando ```hdfs dfs -mkdir user/projeto/semantix ``` e enviar os arquivos para o HDFS através dos comandos:
   
   ```
   hdfs dfs -put HIST_PAINEL_COVIDBR_2020_Parte1_06jul2021.csv /user/projeto/semantix
   ```
   ```
   hdfs dfs -put HIST_PAINEL_COVIDBR_2020_Parte2_06jul2021.csv /user/projeto/semantix
   ```
   ```
   hdfs dfs -put HIST_PAINEL_COVIDBR_2021_Parte1_06jul2021.csv /user/projeto/semantix
   ```
   ```
   hdfs dfs -put HIST_PAINEL_COVIDBR_2021_Parte2_06jul2021.csv /user/projeto/semantix
   ```
   
#### 1.5 - Conferir se arquivos foram devidamente salvos no HDFS através de ``` hdfs dfs -ls /user/projeto/semantix```
   
   ![foto04_projeto_dev](https://user-images.githubusercontent.com/62483710/125338881-1e820800-e327-11eb-9d54-ade20e33fa21.PNG)

---       
### 2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município.

Ainda no container do namenode, dar um ```hdfs dfs -cat <file> | head ``` para visualizar o formato do arquivo e seu cabeçalho.

```
hdfs dfs -cat /user/projeto/semantix/HIST_PAINEL_COVIDBR_2021_Parte1_06jul2021.csv | head
```

![foto07_projeto](https://user-images.githubusercontent.com/62483710/125356531-b6d6b780-e33c-11eb-8bfd-50af6c0c6954.PNG)


#### 2.1 - Sair do container namenode através do comando *Ctrl+d* e entrar no container Hiver-Server através do comando:

```
docker exec -it hive-server bash
```

#### 2.2 - Conectar-se ao Hive e acessar ao cliente beeline para se conectar ao HiveServer2 através da porta localhost:10000 através do script:

```
beeline -u jdbc:hive2://localhost:10000
```

![foto05_projeto](https://user-images.githubusercontent.com/62483710/125340327-bb917080-e328-11eb-9aa9-26a9b4864d26.PNG)

#### 2.3 - Criar banco de dados *projeto_semantix*

```
create database projeto_semantix;
```

mostrar banco de dados criado: script ```show databases;```

![foto06_projeto_hive](https://user-images.githubusercontent.com/62483710/125344000-204eca00-e32d-11eb-99f4-fd51e799e650.PNG)

#### 2.4 - Criar tabela dados_covid para receber dados do HDFS

```
CREATE TABLE dados_covid(regiao string,
                         estado string,
                         municipio string,
                         cod_uf int,
                         cod_mun int,
                         cod_regiao_saude int,
                         nome_regiao_saude string,
                         data date,
                         semana_epi int,
                         populacaoTCU2019 int,
                         casos_acumulados int,
                         casos_novos int,
                         obitos_acumulado int,
                         obitos_novos int,
                         recuperados_novos int,
                         em_acompanhamento_novos int,
                         interior_metropolitana string)
                         row format delimited
                         fields terminated by ';'
                         lines terminated by '\n'
                         tblproperties("skip.header.line.count"="1");
```

#### 2.5 - Carregar dados do HDFS na tabela dados_covid:

```
load data inpath '/user/projeto/semantix' into table dados_covid;
```
---
### 3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS:

Utilizando o Jupyter Notebook através da porta localhost: 8889.

#### 3.1 - Listar arquivos no hive/warehouse

```
! hdfs dfs -ls /user/hive/warehouse/
```

![foto08_spark](https://user-images.githubusercontent.com/62483710/125826064-3a052972-b69a-490e-8d61-2f3ee61d2d7e.PNG)


#### 3.2 - Ler a tabela Hive 

```pyspark
tabela = spark.read.table("dados_covid")
```

#### 3.3 - Visualizar schema

```pyspark
tabela.printSchema()
```

![SCHEMA](https://user-images.githubusercontent.com/62483710/126519678-cc8c8413-8358-4222-8db6-d82738111139.PNG)


##### 3.4.1 - Utilizando *spark.sql* criar variáveis que salvam os indicadores:

```pyspark
### Criação das querys

total_casos_recuperados = spark.sql("select MAX (recuperados_novos) as Casos_Recuperados from dados_covid").show()
casos_em_acompanhamento = spark.sql("select LAST (em_acompanhamento_novos) as Em_Acompanhamento from dados_covid WHERE em_acompanhamento_novos IS NOT NULL").show()

total_casos_acumulados = spark.sql("select MAX (casos_acumulados) as Acumulado from dados_covid ").show()
casos_novos = spark.sql("select MAX (casos_novos) as Casos_novos from dados_covid where data = ('2021-07-06')").show()
incidencia = spark.sql("SELECT ROUND(((MAX(casos_acumulados) / MAX(populacaotcu2019))*100000),1) as incidencia from dados_covid where data = ('2021-07-06')").show()

###Calculo incidencia (casos confirmados * 1.000.000) / população.
###Calculo letalidade (mortes totais/casos totais)
###Calculo mortalidade (mortes totais/população)

total_obitos_acumulados = spark.sql("select MAX (obitos_acumulado) as Obito_acumulado from dados_covid ").show()
obitos_novos = spark.sql("select MAX (obitos_novos) as Obitos_novos from dados_covid where data = ('2021-07-06')").show()
letalidade = spark.sql("SELECT ROUND(((MAX(obitos_acumulado) / MAX(casos_acumulados))*100),1) as letalidade from dados_covid").show()
mortalidade = spark.sql("SELECT ROUND(((MAX(obitos_acumulado) / MAX(populacaotcu2019))*100000),1) as mortalidade from dados_covid").show()


#populacao = spark.sql("select MAX (populacaotcu2019) as populacao from dados_covid where data = ('2021-07-06')").show()

```
![indic_03](https://user-images.githubusercontent.com/62483710/126370889-bb5e36bf-e27c-4db4-bd89-7642cd12a3d8.PNG)
![indic_01](https://user-images.githubusercontent.com/62483710/126370891-0f6f5b34-caf1-469e-9e91-97c98ab4db7d.PNG)
![indic_02](https://user-images.githubusercontent.com/62483710/126370895-45b5970f-977f-47bf-aa7f-56a54c73f1d2.PNG)

##### 3.4.2 - Criação das views:

Criação das views

```pyspark
spark.sql("use projeto_semantix")

### Criação das views


total_casos_recuperados = spark.sql("ALTER VIEW Casos_Recuperados AS select 'Casos_Recuperados', MAX (recuperados_novos) as total_casos_recuperados from dados_covid UNION select 'Em_acompanhamento', MAX (em_acompanhamento_novos) as Em_Acompanhamento from dados_covid WHERE data = ('2021-07-06')").show()

casos_confirmados = spark.sql("ALTER VIEW Casos_confirmados AS select 'Acumulados', (MAX (casos_acumulados)) as casos_confirmados from dados_covid UNION select 'Casos_novos', MAX (casos_novos) as Casos_novos from dados_covid where data = ('2021-07-06') UNION SELECT 'Incidencia', ROUND(((MAX(casos_acumulados) / MAX(populacaotcu2019))*100000),1) as incidencia from dados_covid where data = ('2021-07-06')").show()

Obitos_confirmados = spark.sql("ALTER VIEW Obitos_confirmados AS select 'Total_obitos', MAX (obitos_acumulado) as Obitos_confirmados from dados_covid UNION select 'obitos_novos', MAX (obitos_novos) as Obitos_novos from dados_covid where data = ('2021-07-06') UNION SELECT 'obitos_acumulado', ROUND(((MAX(obitos_acumulado) / MAX(casos_acumulados))*100),1) as letalidade from dados_covid UNION SELECT 'mortalidade', ROUND(((MAX(obitos_acumulado) / MAX(populacaotcu2019))*100000),1) as mortalidade from dados_covid").show()


```
##### 3.4.3 Visualização das views:

```pyspark
spark.sql("SELECT * from Casos_Recuperados").show()
spark.sql("SELECT * from Casos_confirmados").show()
spark.sql("SELECT * from Obitos_confirmados").show()
```

![vies_03](https://user-images.githubusercontent.com/62483710/128094569-1069b432-ad6e-4e80-aed6-da3c6df00e20.PNG)
![vies_01](https://user-images.githubusercontent.com/62483710/128094570-19b51dfc-a6a7-47c3-aa90-617e2ef695c7.PNG)
![vies_02](https://user-images.githubusercontent.com/62483710/128094571-d1b3d11e-1a0b-4f71-865c-83db9fb89d8c.PNG)



![foto01_projeto](https://user-images.githubusercontent.com/62483710/125175523-287afe00-e1a3-11eb-8aea-4c59b9d79272.PNG)

---
### 4. Salvar a primeira visualização como tabela Hive

```pyspark
### Criar a View1 e salvá-la como tabela Hive

View1 = spark.sql("SELECT * from Casos_Recuperados")
View1.write.saveAsTable("View1")
```

```pyspark
### Verificar warehouse 

!hdfs dfs -ls /user/hive/warehouse/view1
```

output:

![saveAsTable](https://user-images.githubusercontent.com/62483710/128095391-8bc42dbe-1ed9-4b9f-9a1f-b8c893722ffe.PNG)


---
### 5. Salvar a segunda visualização com formato parquet e compressão snappy


```pyspark
### Criar a view 2 com formato parquet e compressão snappy

View2 = spark.sql("SELECT * from Casos_confirmados")
View2.write.option("compression","snappy").parquet('/user/hive/warehouse/view2')

```

```pyspark
### Verificar warehouse

!hdfs dfs -ls /user/hive/warehouse/view2
```
output:

![parquet_file](https://user-images.githubusercontent.com/62483710/128095871-4eadc670-69b5-4499-b0ce-bd06beac1c04.PNG)

---
### 6. Salvar a terceira visualização em um tópico no Kafka

```
### Salvando view 3 em um topico no kafka

## No terminal kafka ##

#Criando topico kafka

kafka-topics.sh --bootstrap-server kafka:9092 --topic view3 --create --partitions 1 --replication-factor 1

#Verificar criação do topico

kafka-topics.sh --bootstrap-server kafka:9092 --list

#instrui o topico criado a ser um comsumer dos dados

kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic view3
```

```
###Criar o envio dos dados para o kafka

from pyspark.sql.functions import *
from pyspark.sql.types import *

View3 = spark.sql("SELECT * from obitos_confirmados")




View3_converted = View3.withColumn("value", struct(col("total_obitos"),col("obitos_confirmados")))
View3_converted = View3_converted.drop("total_obitos","obitos_confirmados")
View3_converted = View3_converted.withColumn("value",col("value").cast("string"))
#View3_converted.toPandas()


View3_converted.write.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("topic","view3").save()
```

```
### Print abaixo do resultado de saida no console consumer do kafka 

from IPython.display import Image
Image('Kafka.JPG')
```

output:

![kafka01](https://user-images.githubusercontent.com/62483710/128096657-fe7248e2-df59-43e1-a032-054e807a0873.PNG)

---
### 7. Criar a visualização pelo Spark com os dados enviados para o HDFS:

![foto02_projeto](https://user-images.githubusercontent.com/62483710/125175529-36308380-e1a3-11eb-9306-aafa753b9b54.PNG)

```
### Criação da view do exercicio 7 com spark Dataframe


from pyspark.sql.functions import col
from pyspark.sql.functions import lit

temp = tabela.select('regiao','populacaotcu2019','obitos_acumulado','casos_acumulados','estado').where(tabela.regiao != 'regiao')


temp = temp.groupBy('estado', 'regiao')
temp = temp.max('casos_acumulados','obitos_acumulado','populacaotcu2019')


temp = temp.groupBy('regiao')
temp = temp.sum('max(casos_acumulados)','max(obitos_acumulado)','max(populacaotcu2019)')
temp = temp.withColumn('mult', lit(100000))

temp = temp.withColumn('Incidencia', ((col('sum(max(casos_acumulados))') * col('mult'))/col('sum(max(populacaotcu2019))')))
temp = temp.withColumn('Mortalidade', ((col('sum(max(obitos_acumulado))')/col('sum(max(populacaotcu2019))'))) * col('mult'))

temp = temp.drop('mult','sum(max(populacaotcu2019))')
temp = temp.withColumnRenamed('sum(max(casos_acumulados))', 'Casos').withColumnRenamed('sum(max(obitos_acumulado))', 'Óbitos')
view7 = temp

view7.printSchema()
view7.toPandas()
```

output:

![pic_07](https://user-images.githubusercontent.com/62483710/128096742-4320c4f3-d4b0-4020-bc85-5832ae67e8c2.PNG)

---
### 8. Salvar a visualização do exercício 6 em um tópico no Elastic

```
### Conversão da view 3 em CSV para upload no elastic
view3_el = view7.write.format('csv').option("inferSchema", "true").option("header","true").save("/user/hive/warehouse/elastic_search/view3_elastic.csv")

```

```
###checagem de gravação
!hdfs dfs -ls /user/hive/warehouse/elastic_search/view3_elastic.csv
```

output:

![08_01](https://user-images.githubusercontent.com/62483710/128097734-d68d8d94-fd08-4a50-acba-1ecfa28cb704.PNG)

```
### leitura do arquivo
spark.read.csv('/user/hive/warehouse/elastic_search/view3_elastic.csv', inferSchema = True, header = True).toPandas()
```

output:

![08_02](https://user-images.githubusercontent.com/62483710/128097937-3e42149f-67f1-47ca-9e2f-ab40dcc9edbc.PNG)

---
### 9. Criar um dashboard no Elastic para visualização dos novos dados enviados

```
### Comandos Scala no terminal 

## Iniciar sessão com atributos abaixo

spark-shell --jars /opt/spark/jars/elasticsearch-spark_2.10-2.4.1.jar \
        --conf spark.es.nodes="YOUR-IP-ELASTICSEARCH" \
        --conf spark.es.port="9200" \
        --conf spark.es.index.auto.create=true \
        --conf spark.es.nodes.discovery=false

##Salvar view no Elastic

import org.elasticsearch.spark.sql._
val view3 = spark.read.option("inferSchema",true).option("header",true).csv("/user/hive/warehouse/elastic_search/view3_elastic.csv")
view3.show()
view3.saveToEs("view3_elastic")
```

---


