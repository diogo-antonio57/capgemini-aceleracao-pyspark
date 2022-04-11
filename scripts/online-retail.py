from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Função que verifica os nulos
def is_null(coluna):
	return (F.col(coluna).isNull()) | (F.col(coluna) == 'NA') | (F.col(coluna) == 'NaN')

# Função de apontamento de qualidade
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
			'Quantity_qa',
			F.when(is_null('Quantity'),   'M')
			 .when(F.col('Quantity') < 0, 'I')
		)

	# Qualidade InvoiceDate
	df = df.withColumn(
			'InvoiceDate_qa',
			F.when(is_null('InvoiceDate'), 'M')
		)
	
	# Qualidade UnitPrice
	df = df.withColumn(
			'UnitPrice_qa',
			F.when(is_null('UnitPrice'),   				 'M')
			 .when(F.col('UnitPrice') < 0, 				 'I')
			 .when(F.col('UnitPrice').rlike('[a-zA-Z]'), 'A')
		)

	# Qualidade CustomerID
	df = df.withColumn(
		   'CustomerID_qa',
			F.when(is_null('CustomerID'),                    'M')
			 .when(F.col('CustomerID').rlike('[a-zA-Z]'),    'A')
			 .when(~F.col('CustomerID').rlike('^[0-9]{5}$'), 'F')
		)
	
	# Qualidade Country
	df = df.withColumn(
			'Country_qa',
			F.when(is_null('Country'), 'M')
		)
	
	return df

# Função de transformação
def online_retail_proc(df):

	# Tratamento InvoiceNo
	df = df.withColumn(
			'InvoiceNo_status',
			F.when(F.col('InvoiceNo').rlike('C'), 'Cancelled')
			 .when(F.col('InvoiceNo').rlike('^[0-9]{6}$'), 'Effective')
		)
	
	# Tratamento Quantity
	df = df.withColumn(
			'Quantity',
			F.when(is_null('Quantity'), 0)
			 .when(F.col('Quantity') < 0, 0)
			 .otherwise(F.col('Quantity'))
		)
	
	# Tratamento InvoiceDate
	df = (df.withColumn('InvoiceDate', F.lpad(F.col('InvoiceDate'), 16, '0'))
		    .withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'dd/MM/yyyy HH:mm')))
	
	# Transformação UnitPrice
	df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
	 	    .withColumn('UnitPrice',
				F.when((is_null('UnitPrice')) | (F.col('UnitPrice') < 0), 0)))

	return df

# Main
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

	print(df.show(3))
	df_quality = online_retail_qa(df)
	df_proc    = online_retail_proc(df) 
	#print(df.show())