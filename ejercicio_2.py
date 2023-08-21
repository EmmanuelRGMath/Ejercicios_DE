# -*- coding: utf-8 -*-
"""
@author: emmanuel rivera
"""

######################## problema 2 ###############################
#importar librerias
import pyspark
import pyspark.pandas as ps
from itertools import zip_longest
import os
import csv

# Setup the Configuration
conf = pyspark.SparkConf()
spark_context = SparkSession.builder.getOrCreate()

## leer arcticles csv como un spark dataframe e inferimos el schema
path_file="/Users/emman/Downloads"
articles=spark.read.csv(os.path.join(path_file, "articles.csv"), inferSchema=True, header=True)
##1. Generar un diccionario a partir de la columna product_type_name (product_type_no,product_type_name)
#preservar las dos columnas que queremos convertir en diccionario
product_type_name_columns=['product_type_no', 'product_type_name']
columns_to_covert_df=articles.select(product_type_name_columns).drop_duplicates(product_type_name_columns)
#
dict_product_type = {'product_type_no':[row['product_type_no'] for row in columns_to_covert_df.collect()], 
                  'product_type_name': [row['product_type_name'] for row in columns_to_covert_df.collect()]}

##2. write this dictionary as CSV
with open(f"{path_file}/product_type.csv", 'w') as f:
  full_listing = [['' if not b else b for b in i] for i in zip_longest(*[dict_product_type[c] for c in product_type_name_columns])]
  write = csv.writer(f)
  write.writerows([product_type_name_columns]+full_listing)
  
## Leer el CSV generado y hacer inner join con articles.csv
#lemos csv como pandas dataframe y ajustamos layout y damos nombre a las columnas
product_type_pandas=ps.read_csv(f"{path_file}/product_type.csv", index_col=None)
# from pandas to spark
product_type_spark=product_type_pandas.to_spark()
"""
>>> product_type_spark.printSchema()
root
 |-- product_type_no: integer (nullable = true)
 |-- product_type_name: string (nullable = true)
"""
#hacer inner join 
join_df=articles.join(product_type_spark, on=product_type_name_columns, how='inner')

"""
>>> join_df.count()
105542
>>> articles.count()
105542
>>> join_df.printSchema()
root
 |-- product_type_no: integer (nullable = true)
 |-- product_type_name: string (nullable = true)
 |-- article_id: integer (nullable = true)
 |-- product_code: integer (nullable = true)
 |-- prod_name: string (nullable = true)
 |-- product_group_name: string (nullable = true)
 |-- graphical_appearance_no: integer (nullable = true)
 |-- graphical_appearance_name: string (nullable = true)
 |-- colour_group_code: integer (nullable = true)
 |-- colour_group_name: string (nullable = true)
 |-- perceived_colour_value_id: integer (nullable = true)
 |-- perceived_colour_value_name: string (nullable = true)
 |-- perceived_colour_master_id: integer (nullable = true)
 |-- perceived_colour_master_name: string (nullable = true)
 |-- department_no: integer (nullable = true)
 |-- department_name: string (nullable = true)
 |-- index_code: string (nullable = true)
 |-- index_name: string (nullable = true)
 |-- index_group_no: integer (nullable = true)
 |-- index_group_name: string (nullable = true)
 |-- section_no: integer (nullable = true)
 |-- section_name: string (nullable = true)
 |-- garment_group_no: integer (nullable = true)
 |-- garment_group_name: string (nullable = true)
 |-- detail_desc: string (nullable = true)

"""