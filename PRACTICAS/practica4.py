from pyspark.sql import functions as f
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import numpy as np 
from pyspark.sql.functions import lit
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import substring
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col
from pyspark.sql.functions import substring, coalesce, col, countDistinct, sum, lit

spark = SparkSession.builder\
        .master("local[*]")\
        .appName('PySpark_Tutorial')\
        .getOrCreate()

data_schema = [
               StructField('id', IntegerType(), True),
               StructField('date', StringType(), True),
               StructField('store_nbr', DoubleType(), True),
               StructField('family', StringType(), True),
               StructField('sales', DoubleType(), True),
               StructField('onpromotion', IntegerType(), True)
            ]
final_struc = StructType(fields=data_schema)
b_data = spark.read.csv(
    'data/tienda_ventas.csv',
    sep = ',',
    header = True,
    schema = final_struc
    )


b_data.printSchema()
b_data.show()

print("Crear columna 'mes' a partir de la fecha")
b_data = b_data.withColumn('mes', substring('date', 1, 7))
b_data.show()

print('Contar tiendas únicas')
unique_count = b_data.select(countDistinct("store_nbr")).collect()[0][0]
print("Número de valores únicos en 'store_nbr':", unique_count)

print('Agrupar ventas por tienda')
b_data = b_data.withColumn("sales", coalesce(col("sales").cast("double"), lit(0.0)))
agrupado_tiendas = b_data.groupBy("store_nbr").agg(sum("sales").alias("total_sales"))
agrupado_tiendas.orderBy("total_sales", ascending=False).show()

def analizar_ventas_top_5(b_data):

    print('Filtrar las 5 tiendas con mayores ventas')
    top_5_tiendas = [44, 45, 47, 3, 46]
    df_top_5 = b_data.filter(col('store_nbr').isin(top_5_tiendas))
    df_top_5.show()

    print('Agrupar ventas por tienda y mes')
    top_5_por_mes = df_top_5.groupBy('store_nbr', 'mes').agg(f.sum('sales').alias('total_sales'))
    top_5_por_mes.orderBy("store_nbr", ascending=True).show()

    print('Convertir a Pandas y ordenar por mes')
    top_5_por_mes_pandas = top_5_por_mes.toPandas()
    top_5_por_mes_pandas = top_5_por_mes_pandas.sort_values('mes', ascending=True)
    top_5_por_mes_pandas = top_5_por_mes_pandas.reset_index()

    print('Graficar ventas históricas')
    ax = sns.lineplot(
        data=top_5_por_mes_pandas,
        x='mes',
        y='total_sales',
        hue='store_nbr',
        style="store_nbr",
        markers=True,
    )

    ax.tick_params(axis='x', labelrotation=90, labelsize=8)
    ax.tick_params(axis='y', labelsize=8)
    ax.grid(visible=True, axis='x', alpha=0.2)
    ax.grid(visible=True, axis='y', alpha=0.2)
    ax.legend(title='Top 5 tiendas', bbox_to_anchor=(0, 1), fontsize=8, loc='upper left')
    ax.set_ylabel('Ventas')
    ax.set_xlabel('Mes')

    fig = ax.get_figure()
    fig.set_size_inches(11, 3)
    plt.show()


    print('Graficar cada tienda por separado')
    estilos_por_tienda = {
        44: ['grey', 0.4, '+', '--', 1],
        45: ['grey', 0.4, 's', '--', 1],
        47: ['grey', 0.4, 'x', '--', 1],
        3: ['red', 1, 'o', '-', 1.5],
        46: ['grey', 0.4, 'D', '--', 1]
    }

    fig2, ax2 = plt.subplots()

    for tienda in top_5_tiendas:
        estilos = estilos_por_tienda[tienda]
        df_tmp = top_5_por_mes_pandas[top_5_por_mes_pandas['store_nbr'] == tienda]
        ax2.plot(
            df_tmp['mes'],
            df_tmp['total_sales'],
            label=tienda,
            markersize=4,
            color=estilos[0],
            alpha=estilos[1],
            marker=estilos[2],
            linestyle=estilos[3],
            linewidth=estilos[4],
        )

    ax2.tick_params(axis='x', labelrotation=90, labelsize=8)
    ax2.tick_params(axis='y', labelsize=8)
    ax2.grid(visible=True, axis='x', alpha=0.2)
    ax2.grid(visible=True, axis='y', alpha=0.2)
    ax2.legend(title='Top 5 tiendas', labels=top_5_tiendas, bbox_to_anchor=(0, 1), fontsize=8, loc='upper left')
    ax2.set_ylabel('Ventas')
    ax2.set_xlabel('Mes')
    ax2.set_title('Ventas Históricas por mes del TOP 5 Tiendas')
    fig2.set_size_inches(11, 3)
    plt.show()

def main():
    print("Graficando ventas")
    analizar_ventas_top_5(b_data)

if __name__=="__main__":
    main()

