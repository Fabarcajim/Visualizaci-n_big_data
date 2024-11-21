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
               StructField('algoritmo1', DoubleType(), True),
               StructField('algoritmo2', DoubleType(), True),
            ]

final_struc = StructType(fields=data_schema)

b_data = spark.read.csv(
    'data/comparasion_algoritmos.csv',
    sep = ',',
    header = True,
    schema = final_struc
    )

b_data.printSchema()
b_data.show()

def plot_grafica(spark_df):
    """
    Convierte un DataFrame de Spark a pandas y grafica la comparación entre dos algoritmos.
    
    Parameters:
    - spark_df: DataFrame de Spark que contiene las columnas 'dataset', 'algoritmo1', y 'algoritmo2'.
    """
    df_pandas = spark_df.toPandas()
    ax = df_pandas.plot(
        kind='bar',
        x='dataset',
        y=['algoritmo1', 'algoritmo2'],
        figsize=(12, 5)
    )
    ax.grid(visible=True)
    ax.set_xlabel("Dataset")
    ax.set_ylabel("Rendimiento")
    ax.set_title("Evaluación de Algoritmo1 vs Algoritmo2")
    ax.legend(labels=['Algoritmo 1', 'Algoritmo 2'], loc='upper left', bbox_to_anchor=(0, 1))
    ax.set_axisbelow(True)
    plt.show()

def plot_otros_ejemplos1(groups, values_girls, values_boys, xlabel="Groups", ylabel="Number of Students"):
   
    X_axis = np.arange(len(groups))
    plt.bar(X_axis - 0.2, values_girls, 0.4, label = 'Girls')
    plt.bar(X_axis + 0.2, values_boys, 0.4, label = 'Boys')
    plt.xticks(X_axis, groups)
    plt.xlabel("Groups")
    plt.ylabel("Number of Students")
    plt.legend()
    plt.show()

def plot_otros_ejemplos2(df):
  
    sns.barplot(
        data=df, 
        x='Groups', 
        y='Number of Students', 
        hue='Sex'
    )
    plt.show()

df = pd.DataFrame({
    'Groups': ['Group A', 'Group A', 'Group B', 'Group B', 'Group C', 'Group C', 'Group D', 'Group D'],
    'Sex': ['Girl', 'Boy', 'Girl', 'Boy', 'Girl', 'Boy', 'Girl', 'Boy'],
    'Number of Students': [10, 20, 20, 30, 20, 25, 40, 30]    })   


def main():
    print("Graficando datos de comparación de algoritmos de spark")
    plot_grafica(b_data)
    print("Graficando ejemplo 1")
    plot_otros_ejemplos1(['Group A', 'Group B', 'Group C', 'Group D'], [10, 20, 20, 40], [20, 30, 25, 30])
    print("Graficando ejemplo 2")
    plot_otros_ejemplos2(df)

if __name__=="__main__":
    main()