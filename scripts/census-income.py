from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def census_income_tr(df):
    df = df.withColumn('workclass',
                        F.when(F.col('workclass').rlike('\?'), None)
                         .otherwise(F.col('workclass')))

    df = df.withColumn('occupation',
                        F.when(F.col('occupation').rlike('\?'), None)
                         .otherwise(F.col('occupation')))

    df = df.withColumn('native-country',
                        F.when(F.col('native-country').rlike('\?'), None)
                         .otherwise(F.col('native-country')))
    
    return df


def pergunta_1(df):
    (df.where(F.col('income').rlike('>50K'))
       .groupBy('workclass', 'income')
       .count()
       .orderBy(F.col('count').desc())
       .show())


def pergunta_2(df):
    (df.groupBy('race')
       .agg(F.round(F.avg('hours-per-week'), 2).alias('hora_de_trabalho_semanal_media'))
       .show())


if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

    schema_census_income = StructType([
                    StructField('age', IntegerType(),            True),
                    StructField('workclass', StringType(),       True),
                    StructField('fnlwgt', IntegerType(),         True),
                    StructField('education', StringType(),       True),
                    StructField('education-num', IntegerType(),  True),
                    StructField('marital-status', StringType(),  True),
                    StructField('occupation', StringType(),      True),
                    StructField('relationship', StringType(),    True),
                    StructField('race', StringType(),            True),
                    StructField('sex', StringType(),             True),
                    StructField('capital-gain', IntegerType(),   True),
                    StructField('capital-loss', IntegerType(),   True),
                    StructField('hours-per-week', IntegerType(), True),
                    StructField('native-country', StringType(),  True),
                    StructField('income', StringType(),          True)
                ])

    df = (spark.getOrCreate().read
               .format("csv")
               .option("header", "true")
               .schema(schema_census_income)
               .load("/home/spark/capgemini-aceleracao-pyspark/data/census-income/census-income.csv"))

    df_tr = census_income_tr(df)
    
    # pergunta_1(df_tr)
    pergunta_2(df_tr)
