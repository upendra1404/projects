############################################################################################
# Script Name:amf_tc360_fct_attributes.py                                                  #
# Purpose: To calculate new tc360 fraud claim tran attributes and load those into EDL      #
# Created by: Upendra Kumar                                                                #
# Create Date: 10/28/2020                                                                  #
# Modify date: 12/28/2020                                                                  #
############################################################################################

from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from ConfigParser import ConfigParser
from time import strftime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import mean as _mean, stddev_pop as _stddev, col
import datetime
import pyspark
import os
import sys
import getopt
import datetime
import commands

############################################ Function definition section #####################################################################
## Context Build and Variable Setting
def validateArg():
    runDay = None
    printError = "spark-submit script_name.py -f <config_file_path>/<config_file_name> -r runDay"
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:r:")
    except getopt.error as msg:
        print("Something went wrong!")
        print("Example for entering argument to the script is:")
        sys.exit(printError)

    runDay = opts[1:]
    for opt, arg in opts:
        if(opt == "-f"):
            configFileName = arg
        elif(opt == "-r"):
            runDay = arg
    if(configFileName is None or configFileName == ''):
        print(printError)
        sys.exit("ERROR: Config File Name not provided with argument -f")
    elif(runDay is None or runDay == ''):
        print(printError)
        sys.exit("ERROR: runDay not provided with argument -r")
    return configFileName, runDay

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
                if ('ftCaseTranPath' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain ftCaseTranPath")
                else:
                    ftCaseTranPath = config.get('HIVE', 'ftCaseTranPath')
                if ('ftccPath' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain ftccPath")
                else:
                    ftccPath = config.get('HIVE', 'ftccPath')
                if ('tc360_fct_hive' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain tc360_fct_hive")
                else:
                    tc360_fct_hive = config.get('HIVE','tc360_fct_hive')
                if ('accXrefColumnNames' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain accXrefColumnNames")
                else:
                    accXrefColumnNames = config.get('SCHEMA','accXrefColumnNames')
        return {

                            'readMode': readMode
                           ,'writeMode': writeMode
                           ,'tc360_fct_hive': tc360_fct_hive
                           ,'ftCaseTranPath': ftCaseTranPath
                           ,'ftccPath': ftccPath
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

#######################################################################################################################
## Create Data Masking UDFs from Masking Jar
def registerJavaFunction(sqlContext):
 sqlContext.registerJavaFunction("encrypt", "com.syf.edl.services.creditservice.crypto.udf.EncrypUDF")
 sqlContext.registerJavaFunction("decrypt", "com.syf.edl.services.creditservice.crypto.udf.DecryptUDF")

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
########################################################################################################################
## Reading ft case tran table
def readFCTran(ftCaseTranPath,spark):
 ftCaseTranDf = spark.read.parquet(ftCaseTranPath).filter("trim(fraud_type_cd)='F'")
 ftCaseTranDf.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return ftCaseTranDf
## Reading fraud cliam case hive table  
def readFCC(ftccPath,spark):
 spark.read.parquet(ftccPath).createOrReplaceTempView("fccDf")
 fccDf = spark.sql("""select * from fccDf where as_of_date=(select max(as_of_date) from fccDf)""")
 fccDf.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return fccDf

##########################################################################################################################
# join fraud claim case and fraud claim tran table                                                                       #
# Fetch transaction details for each claim case                                                                          #
##########################################################################################################################
def calculate_fct_attributes(fccDf,ftCaseTranDf,runDay,spark):
 fccDf.createOrReplaceTempView("fccDf")
 ftCaseTranDf.createOrReplaceTempView("fctDf")
 spark.sql("""select                 
                                     fccDf.cust_lexid
                                    ,fccDf.orig_account_nbr
                                    ,fccDf.current_account_nbr
                                    ,fccDf.current_account_nbr_pty
                                    ,fccDf.case_id as fcc_case_id
                                    ,fccDf.loss_type_cd
                                    ,fctDf.*                                     
                                     from fccDf inner join fctDf
                                     on trim(fccDf.case_id) = trim(fctDf.case_id)
                                     """).createOrReplaceTempView("finalDf")
 fctFinalDf = spark.sql("""select 
                              cast(cust_lexid as bigint) as cust_lexid
                             ,cast(orig_account_nbr as string) as orig_account_nbr
                             ,cast(current_account_nbr as string) as current_account_nbr
                             ,cast(current_account_nbr_pty as string) as current_account_nbr_pty
                             ,cast(fcc_case_id as string) as case_id
                             ,cast(loss_type_cd as char(2)) as loss_type_cd
                             ,cast(fdr_auth_trac_nr as string) as fdr_auth_trac_nr
                             ,cast(currency_cd as string) as currency_cd
                             ,cast(transaction_dt as timestamp) as transaction_dt
                             ,cast(round(transaction_am,2) as decimal(19,2)) as transaction_am
                             ,cast(merchant_nm as string) as merchant_nm
                             ,cast(merchant_account_id as string) as merchant_account_id
                             ,cast(sic_cd as string) as mcc
                             from finalDf""").dropDuplicates()\
							                 .withColumn('edl_load_ts',lit(current_timestamp()))\
											 .withColumn('as_of_date',lit(runDay))
 fctFinalDf.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return fctFinalDf
 
#######################################################################################################################
# Function for loading the new tc360 fraud claim tran attributes hive table                                           #
#######################################################################################################################
def load_tc360_fct_attributes(fctFinalDf,tc360_fct_hive,writeMode,spark):
 print("######################## Starting loading tc360 fraud claim case attributes hive table ######################################")              
 spark.sql("set hive.exec.dynamic.partition=true")
 spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 fctFinalDf.write.mode(writeMode).insertInto(tc360_fct_hive,overwrite=True)
 print("Total record loaded into hive table is: "+ str(fctFinalDf.count()))
 print("######################## Finished loading tc360 fraud claim case attributes hive table ###########################################")

########################################################################################################################
# defining main function for the tc360 claim case tran attribute script                                                #
########################################################################################################################
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 hive = readHiveContext(spark)
 registerJavaFunction(sqlContext)
 configFileName,runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 ftCaseTranDf = readFCTran(inputParams['ftCaseTranPath'],spark)
 fccDf = readFCC(inputParams['ftccPath'],spark)
 fctFinalDf = calculate_fct_attributes(fccDf,ftCaseTranDf,runDay,spark)
 load_tc360_fct_attributes(fctFinalDf,inputParams['tc360_fct_hive'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()