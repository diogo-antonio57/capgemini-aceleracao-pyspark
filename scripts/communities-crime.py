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
    df = (df.withColumn('raca_predominante',
                        F.when(F.col('racepctblack') == F.greatest('racepctblack', 'racePctWhite', 'racePctAsian', 'racePctHisp'), 'african american')
                         .when(F.col('racePctWhite') == F.greatest('racepctblack', 'racePctWhite', 'racePctAsian', 'racePctHisp'), 'caucasian')
                         .when(F.col('racePctAsian') == F.greatest('racepctblack', 'racePctWhite', 'racePctAsian', 'racePctHisp'), 'asian heritage')
                         .when(F.col('racePctHisp')  == F.greatest('racepctblack', 'racePctWhite', 'racePctAsian', 'racePctHisp'), 'hispanic heritage')))
    
    return df


def pergunta_1(df):
    print('Pergunta 1')
    (df.where((F.col('PolicOperBudg').isNotNull()) &
              (F.col('state').isNotNull()) &
              (F.col('communityname').isNotNull()))
       .groupBy('state', 'communityname')
       .agg(F.round(F.sum('PolicOperBudg'), 2).alias('PolicOperBudg'))
       .orderBy(F.col('PolicOperBudg').desc())
       .limit(1)
       .show())


def pergunta_2(df):
    print('Pergunta 2')
    (df.where((F.col('ViolentCrimesPerPop').isNotNull()) &
              (F.col('state').isNotNull()) &
              (F.col('communityname').isNotNull()))
       .groupBy('state', 'communityname')
       .agg(F.round(F.sum('ViolentCrimesPerPop'), 2).alias('violent_crimes'))
       .orderBy(F.col('violent_crimes').desc())
       .limit(1)
       .show())


def pergunta_3(df):
    print('Pergunta 3')
    (df.where((F.col('population').isNotNull()) &
              (F.col('state').isNotNull()) &
              (F.col('communityname').isNotNull()))
       .groupBy('state', 'communityname')
       .agg(F.round(F.sum('population'), 2).alias('population'))
       .orderBy(F.col('population').desc())
       .limit(1)
       .show())


def pergunta_4(df):
    print('Pergunta 4')
    (df.where((F.col('racepctblack').isNotNull()) &
              (F.col('state').isNotNull()) &
              (F.col('communityname').isNotNull()))
       .groupBy('state', 'Communityname')
       .agg(F.round(F.sum('racepctblack'), 2).alias('populacao_negra'))
       .orderBy(F.col('populacao_negra').desc())
       .limit(1)
       .show())


def pergunta_5(df):
    print('Pergunta 5')
    (df.where((F.col('pctWWage').isNotNull()) &
              (F.col('state').isNotNull()) &
              (F.col('communityname').isNotNull()))
       .groupBy('state', 'communityname')
       .agg(F.round(F.sum('pctWWage'), 2).alias('perc_salariados'))
       .orderBy(F.col('perc_salariados').desc())
       .limit(1)
       .show())


def pergunta_6(df):
    print('Pergunta 6')
    (df.where((F.col('agePct12t29').isNotNull()) &
              (F.col('state').isNotNull()) &
              (F.col('communityname').isNotNull()))
       .groupBy('state', 'communityname')
       .agg(F.round(F.sum('agePct12t29'), 2).alias('jovens'))
       .orderBy(F.col('jovens').desc())
       .limit(1)
       .show())

def pergunta_7(df):
    print('Pergunta 7')
    df.agg(F.round(F.corr('PolicOperBudg', 'ViolentCrimesPerPop'), 2).alias('correlacao')).show()


def pergunta_8(df):
    print('Pergunta 8')
    df.agg(F.round(F.corr('PctPolicWhite', 'PolicOperBudg'), 2).alias('correlacao')).show()


def pergunta_9(df):
    print('Pergunta 9')
    df.agg(F.round(F.corr('population', 'PolicOperBudg'), 2).alias('correlacao')).show()


def pergunta_10(df):
    print('Pergunta 10')
    df.agg(F.round(F.corr('population', 'ViolentCrimesPerPop'), 2).alias('correlacao')).show()

def pergunta_11(df):
    print('Pergunta 11')
    df.agg(F.round(F.corr('medIncome', 'ViolentCrimesPerPop'), 2).alias('correlacao')).show()


def pergunta_12(df):
    print('Pergunta 12')
    (df.select('state', 'communityname', 'racepctblack', 'racePctWhite', 'racePctAsian', 'racePctHisp', 'ViolentCrimesPerPop', 'raca_predominante')
       .orderBy(F.col('ViolentCrimesPerPop').desc())
       .limit(10)
       .show())


if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

    schema_communities_crime = df_schema()

    df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))

    # df.printSchema()
    # df.show()

    # Transformação
    df_tr = communities_crime_tr(df) 

    pergunta_1(df)
    pergunta_2(df)
    pergunta_3(df)
    pergunta_4(df)
    pergunta_5(df)
    pergunta_6(df)
    pergunta_7(df)
    pergunta_8(df)
    pergunta_9(df)
    pergunta_10(df)
    pergunta_11(df)
    pergunta_12(df_tr)
