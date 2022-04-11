from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def is_null(coluna):
	return (F.col(coluna).isNull()) | (F.col(coluna) == 'NA') | (F.col(coluna) == 'NaN')

def online_retail_qa(df):
	# Qualidade InvoiceNo
	df = df.withColumn(
			'InvoiceNo_qa',
			F.when(is_null('InvoiceNo'), 									'M')
		 	 .when(~F.col('InvoiceNo').rlike('(^[0-9]{6}$)|(^C[0-9]{6}$)'), 'F')
		)
	
	# Qualidade StockCode
	df = df.withColumn(
			'StockCode_qa',
			F.when(is_null('StockCode'),                    'M')
			 .when(F.col('StockCode').rlike('[a-zA-Z]'),    'A')
			 .when(~F.col('StockCode').rlike('^[0-9]{5}$'), 'F')
		)
	
	# Qualidade Description
	df = df.withColumn(
			'Description_qa',
			F.when(is_null('Description'), 'M')
		)

	# Qualidade Quantity
	df = df.withColumn(
			'Quantity_qa'
			F.when(is_null(df),           'M')
			 .when(F.col('Quantity') < 0, 'I')
		)

	# Qualidade InvoiceDate
	df = df.withColumn(
			'InvoiceDate_qa',
			F.when(is_null('InvoiceDate'), 'M')
		)
	
	# Qaulidade UnitPrice
	df = df.withColumn(
			'UnitPrice_qa',
			F.when(is_null('UnitPrice'),   			 'M')
			 .when(F.col('UnitPrice') < 0, 			 'I')
			 .when(F.col('UnitPrice').rlike[a-zA-Z], 'A')
		)

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType([
					  StructField('InvoiceNo', StringType(),      True),
					  StructField('StockCode', IntegerType(),     True),
					  StructField('Description', StringType(),    True),
					  StructField('Quantity', IntegerType(),      True),
					  StructField('InvoiceDate', StringType(),    True),
					  StructField('UnitPrice', StringType(),      True),
					  StructField('CustomerID', IntegerType(),    True),
					  StructField('Country', StringType(),        True)
				  ])

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	#print(df.show())
	online_retail_qa(df)
