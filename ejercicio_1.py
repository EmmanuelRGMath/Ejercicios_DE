# -*- coding: utf-8 -*-
"""
@author: emmanuel rivera
"""

########################### Ejercicio 1 #####################################
#importar pandas
import pandas as pd

## unificar en un solo dataframe la data de 2023. 
path_file="/Users/emman/Downloads"  
list_csv_names=['07-2023_02', '07-2023_01', '06-2023_02', '06-2023_01', '05-2023_02', '05-2023_01', '04-2023_01', '04-2023_02', 
                 '03-2023_01', '02-2023_02',  '02-2023_01',  '01-2023_01', '01-2023_02']
list_dfs=[]
for csv_name in list_csv_names:
    df=pd.read_csv(f"c:/{path_file}/{csv_name}.csv")
    # mover nombre de columnas a primera fila y asiganando nuevo nombre a las columnas
    new_df = (df.T.reset_index().T.reset_index(drop=True)
            .set_axis([f'col_{k}' for k in range(df.shape[1])], axis=1)) 
    #resetear indices (para evitar conflictos al concatenar)
    new_df.reset_index(drop=True, inplace=True)
    #concatenar pandas dfs para obtener el df con la informacion historica
    list_dfs.append(new_df)
#
df_hist = pd.concat(list_dfs, axis=0)
old_names=["col_0", "col_2", "col_3", "col_7", "col_8", "col_11"]
new_name=["producto", "marca", "categoria_prod", "cadena_comercial", \
          "giro_comercia", "estado"]
rename_dict=dict(zip(old_names, new_name))
df_hist.rename(columns=rename_dict, inplace=True)
df_hist=df_hist[new_name]

## 1.contamos los registros totales del historico
len(df_hist.index)
# 8098297
# En 2023 se tiene un total de registros de 8098297

## 2. Cuantas categorias hay. 
df_hist.shape
# Out[5]: (8098297, 15) 
# En este caso se tienen 15 columnas distintas

"""
contamos las distintas categorias de las columnas relevantes
"""
# productos 
# 775 productos distintos
len(df_hist.producto.unique())

# categorias de productos distintos
# 41 categorias de productos
len(df_hist.categoria_prod.unique())

# marcas distintas de productos
# 1109 marcas distintas
len(df_hist.marca.unique())

## 3.Cuantas cadenas comerciales

#diferentes cadenas comerciales
# 254 cadenas distintas
len(df_hist.cadena_comercial.unique())

# categorias de giros
# 16 categorias distintas de giros
len(df_hist.giro_comercia.unique())

#4.
#ahora veremos cuales son los productos mas monitoreados por estado de la rep.
df_1=df_hist[["estado", "categoria_prod"]].groupby(["estado", "categoria_prod"]).size() \
    .reset_index() 
df_1.rename(columns={0: "count"}, inplace=True) 
df_1=df_1.sort_values(by=["estado", "count"], ascending=[True, False])
#generamos dataframe con productos mas monitoreados por estados
df_2=df_1.groupby("estado").first().reset_index() 

"""
                  estado      categoria_prod   count
0         AGUASCALIENTES        MEDICAMENTOS   39840
1        BAJA CALIFORNIA        MEDICAMENTOS   34556
2    BAJA CALIFORNIA SUR        MEDICAMENTOS   31563
3               CAMPECHE        MEDICAMENTOS   34209
4                CHIAPAS        MEDICAMENTOS   10289
5              CHIHUAHUA        MEDICAMENTOS   43751
6       CIUDAD DE MÉXICO        MEDICAMENTOS  336650
7   COAHUILA DE ZARAGOZA        MEDICAMENTOS   36408
8                DURANGO        MEDICAMENTOS   38209
9       ESTADO DE MÉXICO        MEDICAMENTOS  220782
10            GUANAJUATO        MEDICAMENTOS   69953
11              GUERRERO  HORTALIZAS FRESCAS    4738
12               HIDALGO        MEDICAMENTOS   16979
13               JALISCO        MEDICAMENTOS   64093
14   MICHOACÁN DE OCAMPO        MEDICAMENTOS   40488
15               MORELOS        MEDICAMENTOS   27706
16            NUEVO LEÓN        MEDICAMENTOS   73181
17                OAXACA        MEDICAMENTOS   29839
18                PUEBLA        MEDICAMENTOS   47499
19             QUERÉTARO        MEDICAMENTOS   64918
20          QUINTANA ROO        MEDICAMENTOS   52150
21       SAN LUIS POTOSÍ        MEDICAMENTOS   44509
22               SINALOA  HORTALIZAS FRESCAS    4019
23                SONORA        MEDICAMENTOS   34223
24               TABASCO        MEDICAMENTOS   55213
25            TAMAULIPAS        MEDICAMENTOS   27341
26              TLAXCALA        MEDICAMENTOS   29138
27              VERACRUZ        MEDICAMENTOS   48006
28               YUCATÁN        MEDICAMENTOS   54943
29             ZACATECAS        MEDICAMENTOS   61315
"""
# de acuerdo con lo anterior los medicamentos son los productos mas monitoreados
# monitoreados con execpcion de  Sinaloa y Guerrero

## 5.
#ahora veremos cual es la cadena comercial con mas productos monitoreados
df_3=df_hist.groupby("cadena_comercial")["producto"].agg(['nunique']) \
    .sort_values("nunique", ascending=False)

"""
                       nunique
cadena_comercial              
HIPERMERCADO SORIANA       751
MEGA SORIANA               746
CHEDRAUI                   744
SORIANA                    732
WAL-MART                   718
                       ...
ZAPATERIA LA LUNA            1
ZAPATERIA LA PERLA           1
ZAPATERIA PAKAR              1
ZAPATERIAS                   1
ZAPATERIAS 3 HERMANOS        1
"""    
# de acuerdo a lo anterior la cadena comercial con mas variedad de 
#productos monitoreados es HIPERMERCADO SORIANA

# 6. Dato curioso
    
df_4=df_hist[["estado", "giro_comercia"]].groupby(["estado", "giro_comercia"]).size() \
    .reset_index() 
df_4.rename(columns={0: "count"}, inplace=True) 
df_4=df_4.sort_values(by=["estado", "count"], ascending=[True, False])
#
df_5=df_4.groupby("estado").first().reset_index() 

"""
                  estado                          giro_comercia    count
0         AGUASCALIENTES  SUPERMERCADO / TIENDA DE AUTOSERVICIO   102589
1        BAJA CALIFORNIA  SUPERMERCADO / TIENDA DE AUTOSERVICIO    77183
2    BAJA CALIFORNIA SUR  SUPERMERCADO / TIENDA DE AUTOSERVICIO   104813
3               CAMPECHE  SUPERMERCADO / TIENDA DE AUTOSERVICIO   106513
4                CHIAPAS  SUPERMERCADO / TIENDA DE AUTOSERVICIO    54046
5              CHIHUAHUA  SUPERMERCADO / TIENDA DE AUTOSERVICIO   164630
6       CIUDAD DE MÉXICO  SUPERMERCADO / TIENDA DE AUTOSERVICIO  1696881
7   COAHUILA DE ZARAGOZA  SUPERMERCADO / TIENDA DE AUTOSERVICIO   104575
8                DURANGO  SUPERMERCADO / TIENDA DE AUTOSERVICIO    91505
9       ESTADO DE MÉXICO  SUPERMERCADO / TIENDA DE AUTOSERVICIO  1176367
10            GUANAJUATO  SUPERMERCADO / TIENDA DE AUTOSERVICIO   255650
11              GUERRERO  SUPERMERCADO / TIENDA DE AUTOSERVICIO    41389
12               HIDALGO  SUPERMERCADO / TIENDA DE AUTOSERVICIO    59299
13               JALISCO  SUPERMERCADO / TIENDA DE AUTOSERVICIO   273729
14   MICHOACÁN DE OCAMPO  SUPERMERCADO / TIENDA DE AUTOSERVICIO   106322
15               MORELOS  SUPERMERCADO / TIENDA DE AUTOSERVICIO   116097
16            NUEVO LEÓN  SUPERMERCADO / TIENDA DE AUTOSERVICIO   250016
17                OAXACA  SUPERMERCADO / TIENDA DE AUTOSERVICIO   113959
18                PUEBLA  SUPERMERCADO / TIENDA DE AUTOSERVICIO   148714
19             QUERÉTARO  SUPERMERCADO / TIENDA DE AUTOSERVICIO   179038
20          QUINTANA ROO  SUPERMERCADO / TIENDA DE AUTOSERVICIO   197751
21       SAN LUIS POTOSÍ  SUPERMERCADO / TIENDA DE AUTOSERVICIO   106464
22               SINALOA  SUPERMERCADO / TIENDA DE AUTOSERVICIO    17081
23                SONORA  SUPERMERCADO / TIENDA DE AUTOSERVICIO    89623
24               TABASCO  SUPERMERCADO / TIENDA DE AUTOSERVICIO   210903
25            TAMAULIPAS  SUPERMERCADO / TIENDA DE AUTOSERVICIO    80886
26              TLAXCALA  SUPERMERCADO / TIENDA DE AUTOSERVICIO    91183
27              VERACRUZ  SUPERMERCADO / TIENDA DE AUTOSERVICIO   121757
28               YUCATÁN  SUPERMERCADO / TIENDA DE AUTOSERVICIO   149119
29             ZACATECAS  SUPERMERCADO / TIENDA DE AUTOSERVICIO   182786
"""
# como podemos observar, se tiene que las cadenas de supermercado grandes son
# las mas monitoreadas

df_6=df_hist.groupby("marca")["producto"].agg(['nunique']) \
    .sort_values("nunique", ascending=False)
    
"""
                    count
marca                    
S/M               2607297
LA COSTEÑA         120167
FUD                 98175
BIMBO               84494
MABE                83246
                  ...
BENOTTO. GISELLE        1
MONTE XANIC             1
GATO NEGRO              1
CAMPOAMOR               1
KIRKLAND                1
"""
# La mayoria de los productos que se monitorean aparecen sin marca, o de lineas
# mas economicas. 
