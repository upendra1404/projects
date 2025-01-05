###############################################################################
# Script Name:amtf_cspa_rc_onetime_load.py                                    #
# Purpose: To build a one tine view of cspa details for RC clients            #
# Created by: Upendra Kumar                                                   #
# Create Date: 05/15/2021                                                     #
# Modify date:                                                                #
###############################################################################

from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from ConfigParser import ConfigParser
from pyspark.sql.functions import mean as _mean, stddev_pop as _stddev, col
from pyspark.sql.window import Window
import pyspark
import os
import sys
import getopt
import datetime
import commands



############################################ Function definition section #####################################################################
## Context Build and Variable Setting
def validateArg():
    try:
        argv = sys.argv[1:]
        opts, args = getopt.getopt(argv, 'f', ['config_file='])
        return args[0]
    except (getopt.GetoptError, IndexError):
        print("Something went wrong during validating the config arg file/path")
        print("Please check the format for the spark command")
        sys.exit("""spark-submit script_name.py -f <config_file_path>/<config_file_name>""")

###################################################################################################################
##Validate config file
def validateConfigFile(configFileName):
    if(not(os.path.exists(configFileName))):
        sys.exit("ERROR: The Configuration file " + configFileName + " doesn't exist")
    else:
        config = ConfigParser()
        config.optionxform = str
        config.read(configFileName)
        ## Checking the Config File Sections
        if('SCHEMA' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: SCHEMA")
        elif('HIVE' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: HIVE")
        elif('HDFSPATH' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: HDFSPATH")
        else:
                # check the options in each section
                if('readModeRawFile' not in config.options('FUNCTIONAL')):
                    sys.exit("ERROR: Section FUNCTIONAL doesn't contain readModeRawFile")
                else:
                    readMode = config.get('FUNCTIONAL', 'readModeRawFile')
                    readOptions = ['DROPMALFORMED', 'PERMISSIVE']
                if (readMode in readOptions):
                    readMode = readMode
                if ('writeModeTokenFile' not in config.options('FUNCTIONAL')):
                   sys.exit("ERROR: Section FUNCTIONAL doesn't contain readModeRawFile")
                else:
                     writeMode = config.get('FUNCTIONAL','writeModeTokenFile')
                     writeOptions = ['overwrite','append','ignore','error']
                     if writeMode in writeOptions:
                        writeMode = writeMode
                     else:
                       sys.exit("ERROR: input write mode seems invalid, please check back the write mode in config file")
                if ('edl_amf_lexid_syfacc' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain edl_amf_lexid_syfacc")
                else:
                    edl_amf_lexid_syfacc = config.get('HIVE', 'edl_amf_lexid_syfacc')
                if ('inputFileDemiliter' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section HIVE doesn't contain inputFileDemiliter")
                else:
                    inputFileDemiliter = config.get('SCHEMA', 'inputFileDemiliter')
                if ('adColumnsP3' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section SCHEMA doesn't contain adColumnsP3")
                else:
                    adColumnsP3 = config.get('SCHEMA','adColumnsP3')
                if ('adColumnsP4' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain adColumnsP4")
                else:
                    adColumnsP4 = config.get('SCHEMA','adColumnsP4')
                if ('afColumnsP3' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain afColumnsP3")
                else:
                    afColumnsP3 = config.get('SCHEMA','afColumnsP3')
                if ('afColumnsP4' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain afColumnsP4")
                else:
                    afColumnsP4 = config.get('SCHEMA','afColumnsP4')
                if ('adExtPathP3' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain adExtPathP3")
                else:
                    adExtPathP3 = config.get('HDFSPATH','adExtPathP3')
                if ('adExtPathP4' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain adExtPathP4")
                else:
                    adExtPathP4 = config.get('HDFSPATH','adExtPathP4')
                if ('afExtPathP3' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain afExtPathP3")
                else:
                    afExtPathP3 = config.get('HDFSPATH','afExtPathP3')
                if ('afExtPathP4' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain afExtPathP4")
                else:
                    afExtPathP4 = config.get('HDFSPATH','afExtPathP4')
                if ('cspaRCExtPath' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain cspaRCExtPath")
                else:
                    cspaRCExtPath = config.get('HDFSPATH','cspaRCExtPath')
                                
        return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'adColumnsP3': adColumnsP3
                            ,'adColumnsP4': adColumnsP4
                            ,'adExtPathP3': adExtPathP3
                            ,'adExtPathP4': adExtPathP4
                            ,'afColumnsP3': afColumnsP3
                            ,'afColumnsP4': afColumnsP4
                            ,'afExtPathP3': afExtPathP3
                            ,'afExtPathP4': afExtPathP4
                            ,'cspaRCExtPath': cspaRCExtPath
                            
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('c360AttributeFeature').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.sql("SET spark.hadoop.hive.mapred.supports.subdirectories=true")
 spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
 status,protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
 spark.sql("add jar " + protegrityFile)
 spark.sql("add jar /opt/protegrity/pepspark/lib/pepspark-2.3.2.jar")
 spark.sql("add jar /usr/share/java/mysql-connector-java-5.1.17.jar")
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'")
 spark.sql('add jar /data1/EDL/domains/cdl/synapps/edlservices-crypto-sparkudf-extensions/crypto-sparkudf-extensions-jar-with-dependencies.jar')
 return spark

####################################################################################################################
##Create sql context
def createSQLContext(spark):
 sc = spark.sparkContext
 sqlContext = SQLContext(sc)
 return sqlContext

######################################################################################################################
##Create hive context
def readHiveContext(spark):
 hive = HiveContext(spark)
 return hive

def createSchema(columnNames):
    if (not columnNames):
        sys.exit("The Column Names string is blank. Please provide valid column names")
    else:
        columnStructFields = []
        for column in columnNames.split(","):
           if (column in ('MERGER_DATE','ACQ_SCORE_STRATEGY_FLIP_DATE','ACQ_LASER_STRATEGY_FLIP_DATE','ACM_SCORE_FLIP_DATE')):
              columnStructFields.append(StructField(column, LongType(), True))
           elif (column in ('EDL_LOAD_TS')):
              columnStructFields.append(StructField(column, TimestampType(), True))
           else:
              columnStructFields.append(StructField(column, StringType(), True))
    schema = StructType(columnStructFields)
    return schema
########################################################################################################################

## Function definition to replace blank with nulls
def replaceBlankAsNull(x):
    return when(trim(col(x)) != '', col(x)).otherwise(None)

def readAccDimP3P4(adExtPathP3,adExtPathP4,adSchemaP3,adSchemaP4,spark):
 try:
  adP3Df = spark.read.schema(adSchemaP3).csv(adExtPathP3)\
                                       .filter("logical_application_key is not null or trim(logical_application_key) != ''")\
                                       .filter("(sys is not null or trim(sys) != '') and (prin is not null or trim(prin) != '') and (agent is not null or trim(agent) != '')")\
                                       .select(col("sys")
                                              ,col("prin")
                                              ,col("agent")
                                              ,col("bin")
                                              ,col("dealer")
                                              ,trim(col("branch")).alias("branch")
                                              ,col("client_id").alias("dw_client_id")
                                              ,trim(col("logical_application_key")).alias("logical_application_key"))
  adP4Df = spark.read.schema(adSchemaP4).csv(adExtPathP4)\
                                       .filter("logical_application_key is not null or trim(logical_application_key) != ''")\
                                       .filter("(sys is not null or trim(sys) != '') and (prin is not null or trim(prin) != '') and (agent is not null or trim(agent) != '')")\
                                       .select(col("sys")
                                              ,col("prin")
                                              ,col("agent")
                                              ,col("bin")
                                              ,col("dealer")
                                         ,trim(col("branch")).alias("branch")
                                              ,col("client_id").alias("dw_client_id")
                                         ,trim(col("logical_application_key")).alias("logical_application_key"))
  adDf = adP3Df.union(adP4Df)
  ## Function call to replace spaces with null
  nullCols = [replaceBlankAsNull(x).alias(x) for x in adDf.columns]
  adDf = adDf.select(*nullCols).dropDuplicates()
  adDf.cache()
  return adDf
 except Exception as e:
  sys.exit("ERROR: while reading account dim P3,P4 tables - " + str(e))
  

def readAFP3P4(afExtPathP3,afExtPathP4,afSchemaP3,afSchemaP4,spark):
 try:
  windowSpec = Window.partitionBy("logical_application_key").orderBy(desc("edl_load_ts"))
  afP3Df = spark.read.schema(afSchemaP3).csv(afExtPathP3)\
                                        .where(trim(col("final_decision_code")) == 'A')\
                                        .drop(col("final_decision_code"))\
                                        .select(col("client_id")
                                               ,col("dealer_name")
                                               ,col("prod_code")
                                               ,col("country_code").alias("country")
                                               ,trim(col("logical_application_key")).alias("logical_application_key")
                                               ,col("edl_load_ts"))
  afP4Df = spark.read.schema(afSchemaP4).csv(afExtPathP4)\
                                        .where(trim(col("final_decision_code")) == 'A')\
                                        .drop(col("final_decision_code"))\
                                        .select(col("client_id")
                                               ,col("dealer_name")
                                               ,col("prod_code")
                                               ,col("country_code").alias("country")
                                               ,trim(col("logical_application_key")).alias("logical_application_key")
                                               ,col("edl_load_ts"))
  afDf = afP3Df.union(afP4Df)
  afDf = afDf.withColumn("rk",dense_rank().over(windowSpec)).where(col("rk") == 1).select("*")
  afDf.cache()
  return afDf
 except Exception as e:
  sys.exit("ERROR: while reading application fact P3,P4 tables - " + str(e))
  
 
 ## Combining AD & AF tables to fetch genasys_id
def fetchGenasysId(adDf,afDf,spark):
 try:
  wdSpecRGID = Window.partitionBy("sys","prin","agent","bin","dealer","branch").orderBy(desc("edl_load_ts"))
  wdSpecRdup = Window.partitionBy("sys","prin","agent","bin","dealer","branch","client_id","dealer_name","dw_client_id").orderBy(lit("X"))
  wdSpecRPC = Window.partitionBy("sys","prin","agent","bin","dealer","branch","client_id","dealer_name","dw_client_id","prod_code").orderBy(lit("X"))
  combinedDf = adDf.join(afDf, on =(adDf['logical_application_key'] == afDf['logical_application_key']), how ="left")
  combinedDf = combinedDf.drop(*['logical_application_key']).withColumn("rkForGID",dense_rank().over(wdSpecRGID)).where(col("rkForGID") == 1).drop(*['rkForGID','edl_load_ts']).dropDuplicates().select("*")
 ## logic to remove duplicates for null or blank prod_code or country
  combinedDf = combinedDf.withColumn("count",count("client_id").over(wdSpecRdup))\
                        .withColumn("rmFlag",expr("case when count > 1 and (country is null or trim(country) = '') and (prod_code is null or trim(prod_code) = '') then 'Y' when count > 1 and (country is not null or trim(country) != '') and (prod_code is null or trim(prod_code) = '') then 'Y' end"))\
                        .where(col("rmFlag").isNull())\
                        .drop(*["rmFlag","count"])\
                        .withColumn("count_001",count("client_id").over(wdSpecRPC))\
                        .withColumn("rmFlag_001",expr("case when count_001 > 1 and (country is null or trim(country) = '') then 'Y' end"))\
                        .where(col("rmFlag_001").isNull())\
                        .drop(*["rmFlag_001","count_001"])
  combinedDf.cache()
  return combinedDf
 except Exception as e:
  sys.exit("ERROR: while fetching most recent genasys id from appilication fact table - " + str(e))
  
 
def createLoadReadyDf(combinedDf):
 ##Forming final load ready df for cspa pscc
 try:
  cspaRCLRDf = combinedDf.select(col("sys").cast("string").alias("sys")
                               ,col("prin").cast("string").alias("prin")
                               ,col("agent").cast("string").alias("agent")
                               ,col("bin").cast("string").alias("iso")
                               ,col("dealer").cast("string").alias("dealer")
                               ,col("branch").cast("string").alias("cost_center")
                               ,col("dealer_name").cast("string").alias("dealer_name")
                               ,col("client_id").cast("string").alias("genasys_id")
                               ,col("dw_client_id").cast("string").alias("dw_client_id")
                               ,col("country").cast("string").alias("country")
                               ,col("prod_code").cast("string").alias("prod_code"))\
                        .dropDuplicates().withColumn('edl_load_ts',lit(current_timestamp()))
  cspaRCLRDf.cache()
  return cspaRCLRDf
 except Exception as e:
  sys.exit("ERROR: while forming the load ready data frame for hive load - " + str(e))
  

#######################################################################################################################
# Function for loading the C360 attributes hive table                                                                 #
#######################################################################################################################
def loadCSPARCHiveTable(cspaRCLRDf,cspaRCExtPath,writeMode,spark):
 try:
  print("######################## Starting loading cspa rc hive table ######################################")
  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  cspaRCLRDf.write.mode(writeMode).parquet(cspaRCExtPath)
  print("Total record loaded into hive table is: "+ str(cspaRCLRDf.count()))
  print("######################## CSPA RC hive table load complete ###########################################")
 except Exception as e:
  sys.exit("ERROR: while data load into hive table - " + str(e))
  

########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 configFileName = validateArg()
 inputParams = validateConfigFile(configFileName)
 adSchemaP3 = createSchema(inputParams['adColumnsP3'])
 adSchemaP4 = createSchema(inputParams['adColumnsP4'])
 afSchemaP3 = createSchema(inputParams['afColumnsP3'])
 afSchemaP4 = createSchema(inputParams['afColumnsP4'])
 adDf = readAccDimP3P4(inputParams['adExtPathP3'],inputParams['adExtPathP4'],adSchemaP3,adSchemaP4,spark)
 afDf = readAFP3P4(inputParams['afExtPathP3'],inputParams['afExtPathP4'],afSchemaP3,afSchemaP4,spark)
 combinedDf = fetchGenasysId(adDf,afDf,spark)
 cspaRCLRDf = createLoadReadyDf(combinedDf)
 loadCSPARCHiveTable(cspaRCLRDf,inputParams['cspaRCExtPath'],inputParams['writeMode'],spark)

 
if __name__ == "__main__":
    main()