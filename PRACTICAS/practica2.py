from pyspark.sql import functions as f
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import numpy as np 

spark = SparkSession.builder\
        .master("local[*]")\
        .appName('PySpark_Tutorial')\
        .getOrCreate()

data_schema = [
               StructField('dataset', StringType(), True),
               StructField('cat1', DoubleType(), True),
               StructField('cat2', DoubleType(), True),
               StructField('cat3', DoubleType(), True),
               StructField('otros', DoubleType(), True),
            ]
final_struc = StructType(fields=data_schema)

b_data = spark.read.csv(
    'data/datasets_top_4.csv',
    sep = ',',
    header = True,
    schema = final_struc
    )
b_data.printSchema()
b_data.show()
filas = 14
df1 = b_data.limit(filas)
df2 = b_data.subtract(df1)
df1.show()
df2.show()

def plot_datos(df_spark1, df_spark2):


    df1 = df_spark1.toPandas()
    df2 = df_spark2.toPandas()

    fig, (ax1, ax2) = plt.subplots(1, 2)
    fig.set_size_inches(7, 4)

    df1.plot.barh(
        stacked=True,
        ax=ax1,
        width=0.4,
        color={
            'cat1': '#000086',
            'cat2': '#5396f4',
            'cat3': '#a1dbe8',
            'otros': '#DADADA'
        }
    )
    df2.plot.barh(
        stacked=True,
        ax=ax2,
        width=0.4,
        color={
            'cat1': '#000086',
            'cat2': '#5396f4',
            'cat3': '#a1dbe8',
            'otros': '#DADADA'
        }
    )

    ax2.set_ylabel('')
    ax1.set_ylabel('Dataset')
    ax2.set_xlabel('Porcentaje')
    ax1.set_xlabel('Porcentaje')
    ax1.legend().remove()
    ax2.legend(
        ['Categoria 1', 'Categoria 2', 'Categoria 3', 'Otros'],
        ncol=4,
        loc="upper left",
        bbox_to_anchor=(-1.3, -0.15)
    )
    ax2.yaxis.tick_right()
    ax1.text(
        75, -5,
        "Algunos datasets han sido ignorados del análisis",
        fontsize=6,
        bbox={"facecolor": "orange", "alpha": 0.5}
    )
    fig.suptitle('¿Cómo se distribuyen las categorías\ndentro de los datasets?')
    plt.show()

def main():
    print("Graficando distribución de categorias")
    plot_datos(df1, df2)

if __name__=="__main__":
    main()
    