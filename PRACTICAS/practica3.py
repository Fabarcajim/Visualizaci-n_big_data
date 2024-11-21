from pyspark.sql import functions as f
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import numpy as np 
from pyspark.sql.functions import lit
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder\
        .master("local[*]")\
        .appName('PySpark_Tutorial')\
        .getOrCreate()

data_schema = [
               StructField('homename', StringType(), True),
               StructField('homecontinent', StringType(), True),
               StructField('homecountry', StringType(), True),
               StructField('homelat', DoubleType(), True),
               StructField('homelon', DoubleType(), True),
               StructField('travelcontinent', StringType(), True),
               StructField('travelcountry', StringType(), True),
               StructField('travellat', DoubleType(), True),
               StructField('travellon', DoubleType(), True)
            ]
final_struc = StructType(fields=data_schema)
b_data = spark.read.csv(
    'data/viajes_surfistas.csv',
    sep = ',',
    header = True,
    schema = final_struc
    )

b_data.printSchema()
print('Mostrando estructura de la tabla')
b_data.show()
unique_homecountry = b_data.select("homecountry").distinct()
print('Mostrando paises unicos')
unique_homecountry.show()
unique_count = b_data.select("homecountry").distinct().count()
print(unique_count)
agrupado = b_data.groupBy('homecountry').agg(
    f.count('homename').alias('homename')
)
print('Mostrando agrupación por pais')
agrupado.show()
agrupado_ordenado = agrupado.orderBy('homename', ascending=False)
agrupado_ordenado.show()
df_top_4=agrupado_ordenado.limit(4)
print('Mostrando top 4')
df_top_4.show()
df_otros = agrupado_ordenado.exceptAll(df_top_4)
df_otros.show()
df_otros = df_otros.withColumn('pais', lit('Otros'))
df_otros.show()
df_otros_agrupado = df_otros.groupBy('pais').agg(
    _sum('homename').alias('homename')
)
print('Mostrando final con la agrupación de paises y otros')
df_otros_agrupado.show()
df_otros_agrupado_pandas = df_otros_agrupado.toPandas()
df_top_4_pandas=df_top_4.toPandas()
df_top_4_pandas.columns = ['pais', 'homename']
df_final = pd.concat([df_top_4_pandas, df_otros_agrupado_pandas])
df_final = df_final.set_index('pais')

def plot_pie_chart(df_final, column_name='homename', title='Porcentaje de Surfistas por País de Procedencia'):

    ax = df_final.plot.pie(
        y=column_name, 
        startangle=90,
        autopct='%1.1f%%',
        colormap='Pastel1' 
    )

    ax.set_ylabel('')
    ax.legend(
        title='Pais de Surfistas',
        bbox_to_anchor=(1, 1),
        fontsize=8
    )

    ax.set_title(title)  
    plt.show()

def main():
    print("Graficando pie final")
    plot_pie_chart(df_final)

if __name__=="__main__":
    main()
