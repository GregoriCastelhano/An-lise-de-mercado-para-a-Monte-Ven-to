-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Como a Monte Venêto pode estar bem posicionada para o lançamento do seu primeiro vinho?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Levamos em consideração 3 dimensões principais (Região, tipo da uva e preço)
-- MAGIC 
-- MAGIC * Quais as regiões apresentam maior produção de vinhos avaliados pela revista?
-- MAGIC 
-- MAGIC * Quais os tipos de uvas são mais bem avaliadas?
-- MAGIC 
-- MAGIC * Qual a média de preço dos vinhos?

-- COMMAND ----------

-- MAGIC %md ##Exploração dos dados (EDA)

-- COMMAND ----------

select * from winemag_data

-- COMMAND ----------

-- MAGIC %md ###Quais as países e regiões apresentam maior produção de vinhos avaliados pela revista?

-- COMMAND ----------

select country as pais, count(country) as soma_pais
from winemag_data
group by  country
order by soma_pais desc

-- COMMAND ----------

-- MAGIC %md O país que apresenta maior produção de vinhos é os Estados Unidos

-- COMMAND ----------

select province as regiao, count(province) as soma_regiao
from winemag_data
group by province
order by soma_regiao desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A região que apresenta a maior produção de vinhos é a Califórnia

-- COMMAND ----------

select country as pais, count(country) as soma_pais
from winemag_data
where country = 'Brazil'
group by country

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O Brasil possui um total de 25 vinhos avaliados pela revista durante o período analisado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * quais regiões brasileiras possuem vinhos avaliados?

-- COMMAND ----------

select country as pais, province as regiao, count(province) as soma_regiao
from winemag_data
where country = 'Brazil'
group by country, province, region_1, region_2
order by soma_regiao desc

-- COMMAND ----------

-- MAGIC %md Os vinhos produzidos no Brasil apareceram em 25 avaliações durante o período analisado.
-- MAGIC 
-- MAGIC A região brasileira que teve mais avaliações foi o Vale dos Vinhedos localizado na Serra Gaúcha no estado do Rio Grande do Sul com um total de 11 vinhos, porém há a presença de outros vinhos avaliados também localizados na Serra Gaúcha. Apesar de não haver total certeza de que o outros vinhos também não sejam especificamente da região do Vale dos Vinhedos, é evidenciado que a maior concentração de vinhos brasileiros avaliados pela Wine Enthusiast está na Serra Gaúcha. 
-- MAGIC 
-- MAGIC Os achados nos levam a pensar que a Monte Venêto se localiza em uma região estratégica do ponto de vista de produção vinícula, visto que ela também se encontra na região da Serra Gaúcha. Isso chama a atenção para dois pontos:  O primeiro se refere a competitividade que o seu novo produto deve apresentar em relação ao preço e aos vinhos que ja são produzidos na região. O segundo ponto está no fato de que a região já apresenta uma certa relevância internacional em relação a crítica especializada.

-- COMMAND ----------

-- MAGIC %md ###Quais os tipos uvas são mais bem avaliadas?

-- COMMAND ----------

select country as pais, province as regiao, variety as tipo_uva, count(variety) as soma_vinhos
from winemag_data
group by country, province, variety
order by soma_vinhos desc

-- COMMAND ----------

select variety as tipo_uva, 
       count(variety) as contagem_vinhos, 
       round(avg(points), 0) as media_pontuacao, 
       max(points) as max_pontuacao, 
       min(points) as min_pontuacao
from winemag_data
group by variety
order by media_pontuacao desc

-- COMMAND ----------

fazer um boxplot para as avaliações

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Os vinhos fabricados a partir da uva Pinot Noir foram os mais avaliados, porém a maior média de avaliação foi para os vinhos que possuem o blend Cabernet-Shiraz 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * quais uvas são mais bem avaliadas no Brasil?

-- COMMAND ----------

select province as regiao, variety as tipo_uva,
       count(variety) as contagem_vinhos, 
       round(avg(points), 0) as media_pontuacao, 
       max(points) as max_pontuacao, 
       min(points) as min_pontuacao
from winemag_data
where country = 'Brazil'
group by province, variety
order by media_pontuacao desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No Brasil os vinhos fabricados a partir da uva Merlot da Serra Gaúcha possuem uma maior média de avaliação com 88 pontos, seguido pelo Bordeaux-style Red Blend de Santa Catarina com 86 pontos. Devido às características de avaliação e os produtos da Monte Venêto a uva Bordeaux-style Red Blend merece destaque.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Qual a média de preço dos vinhos?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Média de preço geral (Dólares)

-- COMMAND ----------

select distinct variety as tipo_uva, 
                count(variety) as contagem_vinhos, 
                round(avg(price), 0) as media_preco,
                round(max(price), 0) as max_preco,
                round(min(price), 0) as min_preco 
from winemag_data
group by tipo_uva
order by media_preco desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Média de preços no Brasil

-- COMMAND ----------

select distinct variety as tipo_uva,
       province as regiao,
       count(variety) as contagem_vinhos,  
       round(avg(price), 0) as media_preco_dolar,
       round(avg(price * 5.14), 0) as media_preco_reais,
       round(min(price), 0) as min_preco_dolar,
       round(min(price * 5.14), 0) as min_preco_reais,
       round(max(price), 0) as max_preco_dolar,
       round(max(price * 5.14), 0) as max_preco_reais
from winemag_data
where country = 'Brazil'
group by province, tipo_uva
order by media_preco_dolar desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O preço dos vinho fabricado a partir da uva Bordeaux-style Red Blend alaviado pela revista é de R$ 180

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##Relacionando o tipo da uva usada e o preço do vinho

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - A região onde se localiza a Monte Venêto é estratégica do ponto de vista geográfico em termos de relevância no cenário de produção vinícula. Isso chama a atenção também para a necessidade de poricionar o seu produto em relação ao preço dos outros vinhos vendidos na região.
-- MAGIC 
-- MAGIC - As uvas mais bem avaliadas na região é a Merlot e a Bordeaux-style Red Blend. Essa última uva se apresenta como ideal para o lançamento do vinho, uma vez que já e uma uva cultivada pela empresa.
-- MAGIC 
-- MAGIC - Agora iremos comparar os preços dos vinhos na região da Serra Gaúcha e vale dos vinhedos para entender melhor qual seria um valor interessante para vender o vinho.

-- COMMAND ----------

select province as regiao, variety as tipo_uva,
       count(variety) as qtd_vinhos,
       winery as adega, designation as vinha,
       points as pontuacao, price as preco, 
       round((price * 5.14), 0) as preco_reais
from winemag_data
where province = 'Serra Gaúcha' or province = 'Vale dos Vinhedos'
group by province, variety, winery, designation, points, price
order by preco desc;




-- COMMAND ----------

select province, count(variety) as qtd_vinhos
from winemag_data
where province = 'Serra Gaúcha' or province = 'Vale dos Vinhedos'
group by province
order by qtd_vinhos desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - A região do Vale dos Vinhedos apresentam uma maior quantidade de vinhos avaliados pela revista.

-- COMMAND ----------

select province as regiao, variety as tipo_uva,
       count(variety) as qtd_vinhos,
       winery as adega, designation as vinha,
       points as pontuacao, price as preco, 
       round((price * 5.14), 0) as preco_reais
from winemag_data
where province = 'Serra Gaúcha' 
group by province, variety, winery, designation, points, price
order by preco desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - No total foram avaliados 15 vinhos originados da Serra Gaúcha. 
-- MAGIC 
-- MAGIC - Os vinhos são produzidos a partir de uvas Merlot, Cabernet Sauvignon ou ainda um blend entre as duas uvas. 
-- MAGIC 
-- MAGIC - A faixa de preço que compreende os vinhos varia entre R$ 57 e R$ 180, enquanto que a maioria dos vinhos avaliados (60%) estão na faixa entre R$ 57 e R$ 67
-- MAGIC 
-- MAGIC 
-- MAGIC A Monte Venêto poderia investir em um vinho que fosse fabricado a partir da uva Bordeaux-style Red Blend uma vez que essa variedade já é cultivada pela empresa. A faixa de preço competitiva para o vinho seria entre R$ 57 e R$ 67 reais. Um vinho pensado inicialmente a partir dessas informações pode torna-lo competitivo tanto em termos de preço como também em relação ao seu tipo uma vez que vinhos da variedade Bordeaux-style Red Blend não foram avaliados e isso pode apontar para uma lacuna no mercado em relação a essa variedade de vinho e que tenha qualidade.
