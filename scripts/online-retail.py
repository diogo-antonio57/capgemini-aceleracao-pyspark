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
			F.when(is_null('InvoiceNo'),									'M')
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
	df = (df.withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m')))
	
	# Transformação UnitPrice
	df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
	 	    .withColumn('UnitPrice',
				F.when((is_null('UnitPrice')) | (F.col('UnitPrice') < 0), 0)
				 .otherwise(F.col('UnitPrice'))))

	return df

# Função report
def online_retail_report(df):
	
	# Pergunta 1
	print('Pergunta 1')

	(df.where(F.col('StockCode').rlike('^gift_0001'))
	   .groupBy(F.col('Description'))
	   .agg(F.sum(F.col('UnitPrice')))
	   .agg( F.round(F.sum(F.col('sum(UnitPrice)')), 2).alias('Sum gift cards') )
	   .show())

	print('---------------------------------------------------------------------------')
	
	# Pergunta 2
	print('Pergunta 2')
	
	(df.where( F.col('StockCode').rlike('^gift_0001') )
	   .groupBy( F.month(F.col('InvoiceDate')).alias('InvoiceDate') )
	   .agg( F.round(F.sum(F.col('UnitPrice')), 2).alias('sales') )
	   .orderBy(F.col('InvoiceDate').asc())
	   .show())

	print('---------------------------------------------------------------------------')

	# Pergunta 3
	print('Pergunta 3')

	(df.where( F.col('StockCode') == 'S' )
	   .groupBy( F.col('StockCode') )
	   .agg( F.round(F.sum(F.col('UnitPrice')), 2).alias('total value') )
	   .show())

	print('---------------------------------------------------------------------------')

	# Pergunta 4
	print('Pergunta 4')

	(df.groupBy(F.col('Description'))
	   .count()
	   .orderBy(F.col('count').desc())
	   .show(1))
	   
	print('---------------------------------------------------------------------------')

	# Pergunta 5
	print('Pergunta 5')

	(df.groupBy( F.month(F.col('InvoiceDate')), F.col('Description') )
	   .count()
	   .orderBy(F.col('count').desc())
	   .show(1))
	
	# Encontrando o maior valor de cada mes
	df_join  = df.groupBy( F.col('Description').alias('d'), F.month(F.col('InvoiceDate')).alias('i') ).count()

	df_join = df.join(df_join,
					 (df['Description'] == df_join['d']) &
					 (F.month(df['InvoiceDate']) == df_join['i']),
					 'left')

	(df_join.groupBy( F.month(F.col('InvoiceDate')) )
	        .agg( F.max(F.struct('count', 'Description')).alias('struct') )
		    .select( 'struct.Description', 'month(InvoiceDate)', 'struct.count' )
		    .orderBy( 'month(InvoiceDate)' )
		    .show())

	del df_join
	print('---------------------------------------------------------------------------')

	# Pergunta 6
	print('Pergunta 6\n')

	df_join = df.groupBy( F.hour('InvoiceDate').alias('h') ).count()

	df_join = df.join(df_join,
					 (F.hour(df['InvoiceDate']) == df_join['h']),
					 'left')
	
	(df_join.groupBy( F.col('h') )
			.agg( F.max('count') )
			.orderBy( F.col('max(count)').desc() )
			.select( F.col('h').alias('hour'), F.col('max(count)').alias('count') )
			.show(1))

	del df_join
	print('---------------------------------------------------------------------------')
	
# Main
if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType([
					  StructField('InvoiceNo', StringType(),      True),
					  StructField('StockCode', StringType(),      True),
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

	df_quality = online_retail_qa(df)
	df_proc    = online_retail_proc(df)
	online_retail_report(df_proc) 

	# ---------------------------------------------------------------------------------------------------
	# testes
	# print(df_proc.filter( F.hour('InvoiceDate') == 7 ).count())