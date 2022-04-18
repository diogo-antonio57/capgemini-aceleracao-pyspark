from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 

def df_schema():
	schema_df = StructType([
		StructField('state', IntegerType(),               True), 
		StructField('county', IntegerType(),              True), 
		StructField('community', IntegerType(),           True), 
		StructField('communityname', StringType(),        True), 
		StructField('fold', IntegerType(),                True), 
		StructField('population', FloatType(),            True), 
		StructField('householdsize', FloatType(),         True), 
		StructField('racepctblack', FloatType(),          True), 
		StructField('racePctWhite',FloatType(),           True), 
		StructField('racePctAsian', FloatType(),          True), 
		StructField('racePctHisp', FloatType(),           True), 
		StructField('agePct12t21', FloatType(),           True),
		StructField('agePct12t29', FloatType(),           True),
		StructField('agePct16t24', FloatType(),           True),
		StructField('agePct65up', FloatType(),            True),
		StructField('numbUrban', FloatType(),             True),
		StructField('pctUrban', FloatType(),              True),
		StructField('medIncome', FloatType(),             True),
		StructField('pctWWage', FloatType(),              True),
		StructField('pctWFarmSelf', FloatType(),          True),
		StructField('pctWInvInc', FloatType(),            True),
		StructField('pctWSocSec', FloatType(),            True),
		StructField('pctWPubAsst', FloatType(),           True),
		StructField('pctWRetire', FloatType(),            True),
		StructField('medFamInc', FloatType(),             True),
		StructField('perCapInc', FloatType(),             True),
		StructField('whitePerCap', FloatType(),           True),
		StructField('blackPerCap', FloatType(),           True),
		StructField('indianPerCap', FloatType(),          True),
		StructField('AsianPerCap', FloatType(),           True),
		StructField('OtherPerCap', FloatType(),           True),
		StructField('HispPerCap', FloatType(),            True),
		StructField('NumUnderPov', FloatType(),           True),
		StructField('PctPopUnderPov', FloatType(),        True),
		StructField('PctLess9thGrade', FloatType(),       True),
		StructField('PctNotHSGrad', FloatType(),          True),
		StructField('PctBSorMore', FloatType(),           True),
		StructField('PctUnemployed', FloatType(),         True),
		StructField('PctEmploy', FloatType(),             True),
		StructField('PctEmplManu', FloatType(),           True),
		StructField('PctEmplProfServ', FloatType(),       True),
		StructField('PctOccupManu', FloatType(),          True),
		StructField('PctOccupMgmtProf', FloatType(),      True),
		StructField('MalePctDivorce', FloatType(),        True),
		StructField('MalePctNevMarr', FloatType(),        True),
		StructField('FemalePctDiv', FloatType(),          True),
		StructField('TotalPctDiv', FloatType(),           True),
		StructField('PersPerFam', FloatType(),            True),
		StructField('PctFam2Par', FloatType(),            True),
		StructField('PctKids2Par', FloatType(),           True),
		StructField('PctYoungKids2Par', FloatType(),      True),
		StructField('PctTeen2Par', FloatType(),           True),
		StructField('PctWorkMomYoungKids', FloatType(),   True),
		StructField('PctWorkMom', FloatType(),            True),
		StructField('NumIlleg', FloatType(),              True),
		StructField('PctIlleg', FloatType(),              True),
		StructField('NumImmig', FloatType(),              True),
		StructField('PctImmigRecent', FloatType(),        True),
		StructField('PctImmigRec5', FloatType(),          True),
		StructField('PctImmigRec8', FloatType(),          True),
		StructField('PctImmigRec10', FloatType(),         True),
		StructField('PctRecentImmig', FloatType(),        True),
		StructField('PctRecImmig5', FloatType(),          True),
		StructField('PctRecImmig8', FloatType(),          True),
		StructField('PctRecImmig10', FloatType(),         True),
		StructField('PctSpeakEnglOnly', FloatType(),      True),
		StructField('PctNotSpeakEnglWell', FloatType(),   True),
		StructField('PctLargHouseFam', FloatType(),       True),
		StructField('PctLargHouseOccup', FloatType(),     True),
		StructField('PersPerOccupHous', FloatType(),      True),
		StructField('PersPerOwnOccHous', FloatType(),     True),
		StructField('PersPerRentOccHous', FloatType(),    True),
		StructField('PctPersOwnOccup', FloatType(),       True),
		StructField('PctPersDenseHous', FloatType(),      True),
		StructField('PctHousLess3BR', FloatType(),        True),
		StructField('MedNumBR', FloatType(),              True),
		StructField('HousVacant', FloatType(),            True),
		StructField('PctHousOccup', FloatType(),          True),
		StructField('PctHousOwnOcc', FloatType(),         True),
		StructField('PctVacantBoarded', FloatType(),      True),
		StructField('PctVacMore6Mos', FloatType(),        True),
		StructField('MedYrHousBuilt', FloatType(),        True),
		StructField('PctHousNoPhone', FloatType(),        True),
		StructField('PctWOFullPlumb', FloatType(),        True),
		StructField('OwnOccLowQuart', FloatType(),        True),
		StructField('OwnOccMedVal', FloatType(),          True),
		StructField('OwnOccHiQuart', FloatType(),         True),
		StructField('RentLowQ', FloatType(),              True),
		StructField('RentMedian', FloatType(),            True),
		StructField('RentHighQ', FloatType(),             True),
		StructField('MedRent', FloatType(),               True),
		StructField('MedRentPctHousInc', FloatType(),     True),
		StructField('MedOwnCostPctInc', FloatType(),      True),
		StructField('MedOwnCostPctIncNoMtg', FloatType(), True),
		StructField('NumInShelters', FloatType(),         True),
		StructField('NumStreet', FloatType(),             True),
		StructField('PctForeignBorn', FloatType(),        True),
		StructField('PctBornSameState', FloatType(),      True),
		StructField('PctSameHouse85', FloatType(),        True),
		StructField('PctSameCity85', FloatType(),         True),
		StructField('PctSameState85', FloatType(),        True),
		StructField('LemasSwornFT', FloatType(),          True),
		StructField('LemasSwFTPerPop', FloatType(),       True),
		StructField('LemasSwFTFieldOps', FloatType(),     True),
		StructField('LemasSwFTFieldPerPop', FloatType(),  True),
		StructField('LemasTotalReq', FloatType(),         True),
		StructField('LemasTotReqPerPop', FloatType(),     True),
		StructField('PolicReqPerOffic', FloatType(),      True),
		StructField('PolicPerPop', FloatType(),           True),
		StructField('RacialMatchCommPol', FloatType(),    True),
		StructField('PctPolicWhite', FloatType(),         True),
		StructField('PctPolicBlack', FloatType(),         True),
		StructField('PctPolicHisp', FloatType(),          True),
		StructField('PctPolicAsian', FloatType(),         True),
		StructField('PctPolicMinor', FloatType(),         True),
		StructField('OfficAssgnDrugUnits', FloatType(),   True),
		StructField('NumKindsDrugsSeiz', FloatType(),     True),
		StructField('PolicAveOTWorked', FloatType(),      True),
		StructField('LandArea', FloatType(),              True),
		StructField('PopDens', FloatType(),               True),
		StructField('PctUsePubTrans', FloatType(),        True),
		StructField('PolicCars', FloatType(),             True),
		StructField('PolicOperBudg', FloatType(),         True),
		StructField('LemasPctPolicOnPatr', FloatType(),   True),
		StructField('LemasGangUnitDeploy', FloatType(),   True),
		StructField('LemasPctOfficDrugUn', FloatType(),   True),
		StructField('PolicBudgPerPop', FloatType(),       True),
		StructField('ViolentCrimesPerPop', FloatType(),   True),
	 ])

	return schema_df


def communities_crime_tr(df):
    df = df.withColumn('PolicOperBudg',
                        F.when(F.col('PolicOperBudg').isNull(), 0)
                         .otherwise(F.col('PolicOperBudg')))
    
    df = df.withColumn('PctPolicWhite',
                        F.when(F.col('PctPolicWhite').isNull(), 0)
                         .otherwise(F.col('PctPolicWhite')))

    df = df.withColumn('ViolentCrimesPerPop',
                        F.when(F.col('ViolentCrimesPerPop').isNull(), 0)
                         .otherwise(F.col('ViolentCrimesPerPop')))
    
    df = df.withColumn('population',
                        F.when(F.col('population').isNull(), 0)
                         .otherwise(F.col('population')))

    df = df.withColumn('racepctblack',
                        F.when(F.col('racepctblack').isNull(), 0)
                         .otherwise(F.col('racepctblack')))

    df = df.withColumn('pctWWage',
                        F.when(F.col('pctWWage').isNull(), 0)
                         .otherwise(F.col('pctWWage')))
    
    df = df.withColumn('agePct12t29',
                        F.when(F.col('agePct12t29').isNull(), 0)
                         .otherwise(F.col('agePct12t29')))
    
    return df


def pergunta_1(df):
	(df.where(F.col('PolicOperBudg').isNotNull())
	   .select('communityname', 'PolicOperBudg')
	   .orderBy(F.col('PolicOperBudg').desc())
	   .limit(1)
	   .show())


def pergunta_2(df):
	(df.groupBy('communityname')
	   .agg(F.round(F.sum('ViolentCrimesPerPop'), 2).alias('violent_crimes'))
	   .orderBy(F.col('violent_crimes').desc())
	   .limit(1)
	   .show())


def pergunta_3(df):
	(df.groupBy('communityname')
	   .agg(F.round(F.sum('population'), 2).alias('population'))
	   .orderBy(F.col('population').desc())
	   .limit(1)
	   .show())


def pergunta_4(df):
	(df.groupBy('Communityname')
	   .agg(F.round(F.sum('racepctblack'), 2).alias('populacao_negra'))
	   .orderBy(F.col('populacao_negra').desc())
	   .limit(1)
	   .show())


def pergunta_5(df):
	(df.groupBy('communityname')
	   .agg(F.round(F.sum('pctWWage'), 2).alias('perc_salariados'))
	   .orderBy(F.col('perc_salariados').desc())
	   .limit(1)
	   .show())


def pergunta_6(df):
	(df.groupBy('communityname')
	   .agg(F.round(F.sum('agePct12t29'), 2).alias('jovens'))
	   .orderBy(F.col('jovens').desc())
	   .limit(1)
	   .show())

def pergunta_7(df):
	df.agg(F.round(F.corr('PolicOperBudg', 'ViolentCrimesPerPop'), 2).alias('correlacao')).show()


def pergunta_8(df):
    df.agg(F.round(F.corr('PctPolicWhite', 'PolicOperBudg'), 2).alias('correlacao')).show()


def pergunta_9(df):
    df.agg(F.round(F.corr('population', 'PolicOperBudg'), 2).alias('correlacao')).show()


def pergunta_10(df):
    df.agg(F.round(F.corr('population', 'ViolentCrimesPerPop'), 2).alias('correlacao')).show()


if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

    schema_communities_crime = df_schema()

    df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))

    # Transformação
    df_tr = communities_crime_tr(df) 

	pergunta_1(df_tr)
	pergunta_2(df_tr)
	pergunta_3(df_tr)
	pergunta_4(df_tr)
	pergunta_5(df_tr)
	pergunta_6(df_tr)
    pergunta_7(df_tr)
    pergunta_8(df_tr)
    pergunta_9(df_tr)
    pergunta_10(df_tr)
