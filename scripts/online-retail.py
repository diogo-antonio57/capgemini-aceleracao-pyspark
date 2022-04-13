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
			F.when(is_null('InvoiceNo'),                                    'M')
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
	df = df.withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))
	
	# Transformação UnitPrice
	df = (df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', '.').cast('float'))
	 	    .withColumn('UnitPrice',
				F.when((is_null('UnitPrice')) | (F.col('UnitPrice') < 0), 0)
				 .otherwise(F.col('UnitPrice'))))

	# Coluna valor total
	df = df.withColumn('total_value', F.round(F.col('UnitPrice') * F.col('Quantity'), 2).cast('float'))

	return df


# Função report
def online_retail_report(df):
	
	# Pergunta 1
	print('Pergunta 1')

	(df.where(F.col('StockCode').rlike('^gift_0001'))
	   .agg( F.round(F.sum(F.col('total_value')), 2).alias('Sum gift cards') )
	   .show())

	print('---------------------------------------------------------------------------')
	
	# Pergunta 2
	print('Pergunta 2')
	
	(df.where( F.col('StockCode').rlike('^gift_0001') )
	   .groupBy( F.month(F.col('InvoiceDate')).alias('month') )
	   .agg( F.round(F.sum(F.col('total_value')), 2).alias('sales') )
	   .orderBy(F.col('month').asc())
	   .show())

	print('---------------------------------------------------------------------------')

	# Pergunta 3
	print('Pergunta 3')

	(df.where( F.col('StockCode') == 'S' )
	   .groupBy( F.col('StockCode') )
	   .agg( F.round(F.sum(F.col('total_value')), 2).alias('total value') )
	   .show())

	print('---------------------------------------------------------------------------')

	# Pergunta 4
	print('Pergunta 4')

	(df.where(~F.col('StockCode').rlike('C'))
	   .groupBy(F.col('Description'))
	   .agg(F.sum('Quantity').alias('Quantity'))
	   .orderBy(F.col('Quantity').desc())
	   .limit(1)
	   .show())

	print('---------------------------------------------------------------------------')

	# Pergunta 5
	print('Pergunta 5')

	# Encontrando a quantidade de vendas de cada produto em cada mes
	df_filter  = (df.where((~F.col('Description').rlike('\?')) &
						   (~F.col('StockCode').rlike('C')))
					.groupBy( F.col('Description'), F.month(F.col('InvoiceDate')).alias('month') )
					.agg(F.sum('Quantity').alias('count')))

	(df_filter.groupBy('month')
	          .agg( F.max(F.struct('count', 'Description')).alias('struct') )
		      .select( 'struct.Description', 'month', 'struct.count' )
		      .orderBy( 'month' )
		      .show())

	del df_filter
	print('---------------------------------------------------------------------------')

	# Pergunta 6
	print('Pergunta 6\n')

	(df.where(~F.col('StockCode').rlike('C'))
	   .groupBy(F.hour('InvoiceDate'))
	   .agg( F.round(F.sum('total_value'), 2).alias('value') )
	   .orderBy(F.col('value').desc())
	   .limit(1)
	   .show())
	print('---------------------------------------------------------------------------')

	# Pergunta 7
	print('Pergunta 7')

	(df.groupBy( F.month('InvoiceDate') )
	   .agg( F.round(F.sum('total_value'), 2).alias('value') )
	   .orderBy( F.col('value').desc() )
	   .limit(1)
	   .show())
	print('---------------------------------------------------------------------------')

	# Pergunta 8
	print('Pergunta 8')

	# Encontra o Ano com maior valor em vendas
	df_best_year = (df.groupBy( F.year(F.col('InvoiceDate')).alias('year') )
	   	    	 	  .agg( F.round(F.sum('total_value'), 2).alias('value') )
	   			 	  .orderBy(F.col('value').desc())
				 	  .select('year')
				 	  .limit(1) )
	
	# Junta com a coluna principal
	df_best_year = df.join(df_best_year,
					  (F.year(df['InvoiceDate']) == df_best_year['year']),
					  'left')
	
	# Faz a soma das vendas dos produtos em cada mês
	df_prod_month = (df_best_year.where(F.col('year').isNotNull())
					   			 .groupBy('year', 'Description', F.month('InvoiceDate'))
					   			 .agg(F.round(F.sum('total_value'), 2).alias('value')) )

	# Encontra o maior valor de vendas entre cada mês do ano
	(df_prod_month.groupBy('month(InvoiceDate)')
				  .agg( F.max(F.struct('value', 'Description', 'year')).alias('struct') )
				  .select('struct.Description', 'struct.year', 'month(InvoiceDate)', 'struct.value')
				  .show())
	print('---------------------------------------------------------------------------')

	# Pergunta 9
	print('Pergunta 9')

	(df.groupBy('Country')
	   .agg(F.round(F.sum('total_value'), 2).alias('value'))
	   .orderBy(F.col('value').desc())
	   .limit(1)
	   .show())
	print('---------------------------------------------------------------------------')

	# Pergunta 10
	print('Pergunta 10')

	(df.where(F.col('StockCode') == 'M')
	   .groupBy('Country')
	   .agg(F.round(F.sum('total_value'), 2).alias('value'))
	   .orderBy(F.col('value').desc())
	   .limit(1)
	   .show())

	print('---------------------------------------------------------------------------')

	# Pergunta 11
	print('Pergunta 11')

	(df.where(~F.col('InvoiceNo').rlike('C'))
	   .groupBy('InvoiceNo')
	   .agg(F.round(F.sum('total_value'), 2).alias('value'))
	   .orderBy(F.col('value').desc())
	   .limit(1)
	   .show())
	print('---------------------------------------------------------------------------')

	# Pergunta 12
	print('Pergunta 12')

	(df.where(~F.col('InvoiceNo').rlike('C'))
	   .select('InvoiceNo', 'Quantity')
	   .orderBy(F.col('Quantity').desc())
	   .limit(1)
	   .show())


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
	df_proc.show(5)
	online_retail_report(df_proc) 

	# ---------------------------------------------------------------------------------------------------
	# testes
	# print( df_proc.where((F.month('InvoiceDate') == 1) &
	# 					 (F.year('InvoiceDate') == 2011))
	# 			  .groupBy('Description')
	# 			  .agg(F.sum('total_value'))
	# 			  .orderBy(F.col('sum(total_value)').desc())
	# 			  .show() )