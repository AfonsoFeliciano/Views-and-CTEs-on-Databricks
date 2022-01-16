-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC Essa demonstração faz parte do módulo *Databases, Tables and Views on Databricks* do curso Data Analyst Learning Plan, e respeitando os direitos autorais da plataforma, o meu objetivo é apenas resumir e fixar o conteúdo ministrado durante todo o curso. 

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Objetivos
-- MAGIC Os objetivos do curso são:
-- MAGIC * Utilizar comandos DDL em Spark SQL para criação de views.
-- MAGIC * Executar queries utilizando o CTE (common table expressions).
-- MAGIC 
-- MAGIC 
-- MAGIC **Recursos adicionais**
-- MAGIC * [Create View - Databricks Docs](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html)
-- MAGIC * [Common Table Expressions - Databricks Docs](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-cte.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuração do ambiente
-- MAGIC Nas células abaixo foi utilizado um script python para geração de variáveis e configuração de um diretório temporário para importar o dataset utilizado neste curso.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import sys, subprocess, os
-- MAGIC subprocess.check_call([sys.executable, "-m", "pip", "install", "git+https://github.com/databricks-academy/user-setup"])
-- MAGIC 
-- MAGIC from dbacademy import LessonConfig
-- MAGIC LessonConfig.configure(course_name="Databases Tables and Views on Databricks", use_db=False)
-- MAGIC LessonConfig.install_datasets(silent=True)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.widgets.text("username", LessonConfig.clean_username)
-- MAGIC dbutils.widgets.text("working_directory", LessonConfig.working_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Observação
-- MAGIC Vale destacar que o ambiente foi configurado utilizando widgets nativos do ambiente Databricks para armazenar variáveis. Um exemplo é `${username}`. Não é necessário alterar os valores, visto que são detectados automaticamente em tempo de execução. 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC O primeiro passo será realizar a criação de um database, juntamente com uma view e uma tabela. Os parâmetros utilizados são concatenados de maneira dinâmica como é o caso do database, que será composto por nome do usuário + _training_database.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${username}_training_database;
USE ${username}_training_database;


CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path '${working_directory}/datasets/flights/departuredelays.csv',
  header "true",
  mode "FAILFAST"
);
CREATE OR REPLACE TABLE external_table LOCATION '${working_directory}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Observação**
-- MAGIC 
-- MAGIC Ao especificar o modo FAILFAST, se alguma anomalia for encontrada, a operação será abortada. 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Views
-- MAGIC Agora que os dados foram criados na external tabela, as criações das views são iniciadas, sendo a primeira uma view que contém apenas os dados de origem igual a ABQ e destino LAX.

-- COMMAND ----------

CREATE OR REPLACE VIEW view_delays_ABQ_LAX AS
SELECT * FROM external_table WHERE origin = 'ABQ' AND destination = 'LAX';
SELECT * FROM view_delays_ABQ_LAX;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Para visualizar uma lista de tabelas e views, pode-se utilizar o comando `SHOW TABLES`.  
-- MAGIC 
-- MAGIC Aqui tem um ponto muito importante, que são as views por contexto. Poderemos observar que ao executar o comando `SHOW TABLES` a view `view_delays_abq_lax` estará visiível na lista.
-- MAGIC 
-- MAGIC Se o cluster for desconectado e reconectado e reutilizado o comando `SHOW TABLES` a view continuará presente na lista. Isso ocorre devido esse tipo de view está armazenada diretamente no metastore do databricks. 
-- MAGIC 
-- MAGIC **Observação**
-- MAGIC 
-- MAGIC O comando `USE ${username}_training_database;` é utilizado após a reconexão do cluster ser efetuada devido visto que a sessão do spark ser é excluída durante esse processo de desconexão e reconexão.

-- COMMAND ----------

USE ${username}_training_database;
SHOW tables;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Agora será criada uma view temporária. A sintaxe é muito similar, porém é necessário adicionar a palavra-chave `TEMPORARY` no comando.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW temp_view AS
SELECT * FROM external_table WHERE delay > 120 ORDER BY delay ASC;
SELECT * FROM temp_view;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Novamente, será possível visualizar a lista de tabelas e view. 
-- MAGIC 
-- MAGIC É possível observar que a view temporária está setada com o valor `True` na coluna `isTemporary` 
-- MAGIC 
-- MAGIC Ao desconectar e reconectar o cluster e novamente recarregar a lista de tabelas e views, será possível observar que a `temp_view` foi deletada. Esse comportamento pode ser explicado pelo fato que as views temporárias não são armazenadas no metastore do Databricks. Quando uma desconexão e posteriormente conexão é realizada, a sessão do Spark é excluída juntamente com a view temporária. 

-- COMMAND ----------

USE ${username}_training_database;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC Por fim, será criada uma view temporária global. Para isso bastar adicionar a palavra-chave `GLOBAL` ao comando. Essa view é muito similar a view temporária, porém sua principal diferença é que ela é adicionada ao banco de dados `global_temp` que é persistido no cluster. 
-- MAGIC 
-- MAGIC Desse modo, enquanto o cluster estiver em execução, esse banco de dados estará online e qualquer notebook conectado a esse cluster poderá se conectar ao banco e realizar consultas nas views temporárias globais.

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW global_temp_view_distance AS
SELECT * FROM external_table WHERE distance > 1000;
SELECT * FROM global_temp.global_temp_view_distance;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Por fim, mesmo que o cluster seja desconectado e reconectado, as views temporárias globais estarão disponíveis para o notebook de origem e para qualquer outro notebook vinculado ao cluster. 

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_distance;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Vale ressaltar que ao utilizar o comando `SHOW TABLES` não é possível visualizar as views temporárias globais.

-- COMMAND ----------

USE ${username}_training_database;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Common Table Expressions (CTEs)
-- MAGIC 
-- MAGIC As consultas utilizando CTEs podem ser utilizadas em vários contextos diferentes. Abaixo tentarei explicar da minha forma, tudo que foi abordado no curso. O primeiro exemplo se trata da aplicação dessa consulta, criando vários aliases (apelidos) para algumas colunas.

-- COMMAND ----------

WITH flight_delays(
  total_delay_time,
  origin_airport,
  destination_airport
) AS (
  SELECT
    delay,
    origin,
    destination
  FROM
    external_table
)
SELECT
  *
FROM
  flight_delays
WHERE 
  1=1
  AND total_delay_time > 120
  AND origin_airport = "ATL"
  AND destination_airport = "DEN";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Basicamente na consulta acima as colunas `delay, origin e destination` foram apelidadas como `total_delay_time, origin_airport e destination_airport` e após isso, utilizadas na cláusula where. 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC A seguir será observado um CTE dentro de outro CTE. 

-- COMMAND ----------

WITH lax_bos AS (
  WITH origin_destination (origin_airport, destination_airport) AS (
    SELECT
      origin,
      destination
    from
      external_table
  )
  SELECT
    *
  FROM
    origin_destination
  WHERE
    origin_airport = 'LAX'
    AND destination_airport = 'BOS'
)
SELECT
  count(origin_airport) AS `Total Flights from LAX to BOS`
FROM
  lax_bos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Similar ao primeiro exemplo o CTE interno foi utilizado para conceder um alias as colunas. Após isso, essas colunas foram utilizadas na cláusula where e posteriormente o CTE externo foi utilizado para realizar uma contagem de Voos do aeroporto LAX para o aeroporto BOS.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC A seguir pode-se observar um exemplo CTE em uma subquery (subconsulta).

-- COMMAND ----------

SELECT
  max(total_delay) AS `Longest Delay (in minutes)`
FROM
  (
    WITH delayed_flights(total_delay) AS (
      SELECT
        delay
      from
        external_table
    )
    SELECT
      *
    FROM
      delayed_flights
  );

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Basicamente, na parte interna da consulta, foi utilizada a expressão CTE com alias e após isso, na parte externa, uma subconsulta foi utilizada para retornar o valor máximo dentro do conjunto de dados.
-- MAGIC 
-- MAGIC 
-- MAGIC Também é possível utilizar um CTE em uma expressão de subquery conforme abaixo.

-- COMMAND ----------

SELECT
  (
    WITH distinct_origins AS (
      SELECT DISTINCT origin FROM external_table
    )
    SELECT
      count(origin)
    FROM
      distinct_origins
  ) AS `Number of Different Origin Airports`;
  
  
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Com exemplo acima, foi utilizado um CTE com Distinct para visualizar todas as origens de maneira única. Após isso, foi realizada uma contagem dessas origens de maneira distinta e selecionada através do SELECT externo da consulta. 
-- MAGIC 
-- MAGIC Por fim, conforme abaixo, é possível visualizar um exemplo do CTE na criação de uma view. 

-- COMMAND ----------

CREATE OR REPLACE VIEW BOS_LAX AS WITH origin_destination(origin_airport, destination_airport) AS
  (SELECT origin,
          destination
   FROM external_table)
SELECT *
FROM origin_destination
WHERE origin_airport = 'BOS'
  AND destination_airport = 'LAX';


SELECT count(origin_airport) AS `Number of Delayed Flights from BOS to LAX`
FROM BOS_LAX;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Desse modo, modo, foi possível observar as mais variadas aplicações de Views, Views Temporárias e Views Temporárias Globais, tornando possível a um analista de dados, analista de bi e engenheiro de dados a maior facilidade na escolha de qual view utilizar no dia a dia. Além, disso de maneira muito útil, foi possível verificar o funcionamento das CTEs. 
-- MAGIC 
-- MAGIC No começo de minha carreira, sempre sentia dúvidas sobre como, quando e por que utilizar uma expressão CTE, desse modo, gostaria de sugerir a leitura deste artigo para quem está iniciando no mundo de dados. 
-- MAGIC 
-- MAGIC https://www.dirceuresende.com/blog/sql-server-como-criar-consultas-recursivas-com-a-cte-common-table-expressions/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limpando os diretórios
-- MAGIC Agora, será dropada a base de dados utilizada no treinamento, para isso, é necessário especificar o comando `CASCADE` para que todos os objetos contidos nessa base sejam deletados também.

-- COMMAND ----------

DROP DATABASE ${username}_training_database CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Por fim, foi utilizado um comando em python para remover o diretório com os dados utilizados. 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC path = dbutils.widgets.get("working_directory")
-- MAGIC dbutils.fs.rm(path, True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Gostaria de deixar claro que todos os direitos são reservados a Databricks, e o meu objetivo com este notebook foi apenas enriquecer os meus conhecimentos e demonstrar para a comunidade que estou estudando os mais variados tópicos de Databricks. 
-- MAGIC 
-- MAGIC Para quem possuir interesse, basta acessar https://databricks.com/learn/training/home e conferir o portfólio de cursos presente na plataforma que sempre é atualizado de forma a acompanhar as tendências do momento.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
