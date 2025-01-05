###############################################################################
# Script Name:amfServiceLexidAccXref.py                                       #
# Purpose: Precursor script to load account details with lexid                #
# Created by: Upendra Kumar                                                   #
# Create Date: 06/20/2021                                                     #
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
from time import strftime
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta
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
                if ('edl_amf_lexid_syfacc' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain edl_amf_lexid_syfacc")
                else:
                    edl_amf_lexid_syfacc = config.get('HIVE', 'edl_amf_lexid_syfacc')
                if ('inputFileDemiliter' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section HIVE doesn't contain inputFileDemiliter")
                else:
                    inputFileDemiliter = config.get('SCHEMA', 'inputFileDemiliter')
                if ('runDay' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain runDay")
                else:
                    runDay = config.get('HIVE', 'runDay')
                if ('syf_acct_token_xref' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain syf_acct_token_xref")
                else:
                    syf_acct_token_xref = config.get('HIVE', 'syf_acct_token_xref')
                if ('edl_amf_acc_xref' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain edl_amf_acc_xref")
                else:
                    edl_amf_acc_xref = config.get('HIVE', 'edl_amf_acc_xref')
                if ('lexidSyfacc' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain lexidSyfacc")
                else:
                    lexidSyfacc = config.get('HIVE', 'lexidSyfacc')
                if ('falconRawPath' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain falconRawPath")
                else:
                    falconRawPath = config.get('HIVE', 'falconRawPath')
                if ('pcAcctLexid' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain pcAcctLexid")
                else:
                    pcAcctLexid = config.get('HIVE','pcAcctLexid')
                if ('pcStmtFact' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain pcStmtFact")
                else:
                    pcStmtFact = config.get('HIVE','pcStmtFact')
                if ('pcAppFact' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain pcAppFact")
                else:
                    pcAppFact = config.get('HIVE','pcAppFact')
                if ('accXrefColumnNames' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain accXrefColumnNames")
                else:
                    accXrefColumnNames = config.get('SCHEMA','accXrefColumnNames')
                if ('stmtFactP3ColumnNames' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain stmtFactP3ColumnNames")
                else:
                    stmtFactP3ColumnNames = config.get('SCHEMA','stmtFactP3ColumnNames')
                if ('stmtFactP4ColumnNames' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain stmtFactP4ColumnNames")
                else:
                    stmtFactP4ColumnNames = config.get('SCHEMA','stmtFactP4ColumnNames')
                if ('stmtFactP5ColumnNames' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain stmtFactP5ColumnNames")
                else:
                    stmtFactP5ColumnNames = config.get('SCHEMA','stmtFactP5ColumnNames')
                if ('adColumnsP3' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain adColumnsP3")
                else:
                    adColumnsP3 = config.get('SCHEMA','adColumnsP3')
                if ('adColumnsP4' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain adColumnsP4")
                else:
                    adColumnsP4 = config.get('SCHEMA','adColumnsP4')
                if ('adColumnsP5' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain adColumnsP5")
                else:
                    adColumnsP5 = config.get('SCHEMA','adColumnsP5')
                if ('tc360AdP3' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain tc360AdP3")
                else:
                    tc360AdP3 = config.get('HDFSPATH','tc360AdP3')
                if ('tc360AdP4' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain tc360AdP4")
                else:
                    tc360AdP4 = config.get('HDFSPATH','tc360AdP4')
                if ('tc360AdP5' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain tc360AdP5")
                else:
                    tc360AdP5 = config.get('HDFSPATH','tc360AdP5')
                if ('extStdAppFactCur' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain extStdAppFactCur")
                else:
                    extStdAppFactCur = config.get('HDFSPATH','extStdAppFactCur')
                if ('tc360AfP4' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain tc360AfP4")
                else:
                    tc360AfP4 = config.get('HDFSPATH','tc360AfP4')
                if ('tc360AfP5' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain tc360AfP5")
                else:
                    tc360AfP5 = config.get('HDFSPATH','tc360AfP5')
                if ('tc360StmtFctP3' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain tc360StmtFctP3")
                else:
                    tc360StmtFctP3 = config.get('HIVE', 'tc360StmtFctP3')
                if ('tc360StmtFctP4' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain tc360StmtFctP3")
                else:
                    tc360StmtFctP4 = config.get('HIVE', 'tc360StmtFctP4')
                if ('tc360StmtFctP5' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain tc360StmtFctP5")
                else:
                    tc360StmtFctP5 = config.get('HIVE', 'tc360StmtFctP5')
        return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'accXrefColumnNames': accXrefColumnNames
                            ,'edl_amf_lexid_syfacc': edl_amf_lexid_syfacc
                            ,'edl_amf_acc_xref': edl_amf_acc_xref
                            ,'falconRawPath': falconRawPath
                            ,'pcAcctLexid': pcAcctLexid
                            ,'pcStmtFact': pcStmtFact
                            ,'pcAppFact': pcAppFact
                            ,'syf_acct_token_xref': syf_acct_token_xref
                            ,'lexidSyfacc': lexidSyfacc
                            ,'runDay': runDay
                            ,'stmtFactP3ColumnNames': stmtFactP3ColumnNames
                            ,'stmtFactP4ColumnNames':stmtFactP4ColumnNames
                            ,'stmtFactP5ColumnNames': stmtFactP5ColumnNames
                            ,'tc360StmtFctP3': tc360StmtFctP3
                            ,'tc360StmtFctP4': tc360StmtFctP4
                            ,'tc360StmtFctP5': tc360StmtFctP5
                            ,'adColumnsP3': adColumnsP3
                            ,'adColumnsP4': adColumnsP4
                            ,'adColumnsP5': adColumnsP5
                            ,'tc360AdP3': tc360AdP3
                            ,'tc360AdP4': tc360AdP4
                            ,'tc360AdP5': tc360AdP5
                            ,'extStdAppFactCur': extStdAppFactCur
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('precursor_acct_lexid_xref').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold','200000000')
 spark.sql("SET spark.hadoop.hive.mapred.supports.subdirectories=true")
 spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
 status,protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
 spark.sql("add jar " + protegrityFile)
 spark.sql("add jar /opt/protegrity/pepspark/lib/pepspark-2.3.2.jar")
 spark.sql("add jar /usr/share/java/mysql-connector-java-5.1.17.jar")
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'") 
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

##############################################################
## Function to creating schema                               #
##############################################################
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
 
def getLatestStmts(stmtPath,schema,n,spark):
 try:
  sc = spark.sparkContext
  sqlContext = createSQLContext(spark)
  dtList = [datetime.date.today() - relativedelta(months=i) for i in range(n)]
  YYYYmm = [d.strftime("%Y-%m") for d in dtList]
  stmtFctDf = sqlContext.createDataFrame(sc.emptyRDD(),schema)
  for i in range(n):
         path = stmtPath + "/" + YYYYmm[i] + "*"
         try:
              stmtFctDf = stmtFctDf.union(spark.read.schema(schema).csv(path))
         except Exception as e:
          print("ERROR: while reading months data from statement fact table " + str(e))
         continue         
  return stmtFctDf
 except Exception as e:
  sys.exit("ERROR: while reading the statements fact data for " + int(n) + " months - " + str(e))

## Reading lexid_sysfact table
def readSyfAcc(lexidSyfacc,spark):
 try: 
  readLexIdDf = spark.read.parquet(lexidSyfacc).where(col('lexid_score1') >= 80)\
                    .select(trim(col('nbr_cardh_acct')).alias("nbr_cardh_acct"),col('lexid1'),col('lexid_score1')).dropDuplicates()
  readLexIdDf.cache()
  return readLexIdDf
 except Exception as e:
  sys.exit("ERROR: while reading lexid syfacc table - " + str(e))
#########################################################################################################################
## Reading account xref table 
def readAccXref(syf_acct_token_xref,accXrefSchema,readMode,spark):
 try:
  readAccXrefDf = spark.read.csv(syf_acct_token_xref,inferSchema=True,schema=accXrefSchema,mode=readMode)\
                           .select(trim(col('customer_acct_nbr_syf')).alias('customer_acct_nbr_syf'),col('customer_acct_nbr'))
  readAccXrefDf.cache()
  return readAccXrefDf
 except Exception as e:
  sys.exit("ERROR: while reading acct xref table - " + str(e))
 
 ## Reading acct dim tables
def readAccDim(tc360AdP3,tc360AdP4,tc360AdP5,adP3Schema,adP4Schema,adP5Schema,readMode,inputFileDemiliter,spark):
 try:
  adP3Df = spark.read.option('delimiter',inputFileDemiliter).csv(tc360AdP3,inferSchema=True,schema=adP3Schema,header=False,mode=readMode)\
               .select(trim(col('current_account_nbr')).alias('current_account_nbr'),col('orig_account_nbr'),col('bin'),col('open_date')
                      ,col('last_plastic_date'),col('nbr_of_cards_current'),lpad(trim(col('sys')),4,'0').alias('sys'),trim(col('external_status')).alias('external_status')
                      ,trim(col('external_status_reason_code')).alias('external_status_reason_code'),trim(col('card_activation_flag')).alias('card_activation_flag')
                      ,trim(col('logical_application_key')).alias('logical_application_key')).dropDuplicates()
  adP4Df = spark.read.option('delimiter',inputFileDemiliter).csv(tc360AdP4,inferSchema=True,schema=adP4Schema,header=False,mode=readMode)\
               .select(trim(col('current_account_nbr')).alias('current_account_nbr'),col('orig_account_nbr'),col('bin'),col('open_date')
                      ,col('last_plastic_date'),col('nbr_of_cards_current'),lpad(trim(col('sys')),4,'0').alias('sys'),trim(col('external_status')).alias('external_status')
                      ,trim(col('external_status_reason_code')).alias('external_status_reason_code'),trim(col('card_activation_flag')).alias('card_activation_flag')
                      ,trim(col('logical_application_key')).alias('logical_application_key')).dropDuplicates()
  adP5Df = spark.read.option('delimiter',inputFileDemiliter).csv(tc360AdP5,inferSchema=True,schema=adP5Schema,header=False,mode=readMode)\
               .select(trim(col('current_account_nbr')).alias('current_account_nbr'),col('orig_account_nbr'),col('bin'),col('open_date')
                      ,col('last_plastic_date'),col('nbr_of_cards_current'),lpad(trim(col('sys')),4,'0').alias('sys'),trim(col('external_status')).alias('external_status')
                      ,trim(col('external_status_reason_code')).alias('external_status_reason_code'),trim(col('card_activation_flag')).alias('card_activation_flag')
                      ,trim(col('logical_application_key')).alias('logical_application_key')).dropDuplicates()
 
  adDf = adP3Df.union(adP4Df).union(adP5Df)
  adDf.cache()
  return adDf
 except Exception as e:
  sys.exit("ERROR: while reading account dim tables - " + str(e))
  
def createAccLexidDf(adDf,readLexIdDf,readAccXrefDf):
 itmdDf = readAccXrefDf.join(readLexIdDf,on = readAccXrefDf['customer_acct_nbr_syf'] == readLexIdDf['nbr_cardh_acct'], how = 'left')
 acctLexidDf = adDf.join(itmdDf,on = adDf['current_account_nbr'] == itmdDf['customer_acct_nbr_syf'], how = 'left')
 acctLexidDf.cache()
 return acctLexidDf

def readStmtFact(tc360StmtFctP3,tc360StmtFctP4,tc360StmtFctP5,stmtP3Schema,stmtP4Schema,stmtP5Schema,acctLexidDf,spark):
 ###############################################################################################################################################################
 # Reading statement fact P3, P4, P5 tables from from hdfs using schema                                                                                        #
 # Fetching the most recent statements for each account                                                                                                        #
 # Taking the union of all statement facts P3, P4, P5                                                                                                          #
 ###############################################################################################################################################################
 try:
  wdSpec = Window.partitionBy('current_account_nbr').orderBy(desc('billing_cycle_date'))
  # Function call to read the most recent 2 months account statements
  readStmtFctP3 = getLatestStmts(tc360StmtFctP3,stmtP3Schema,2,spark) # Reading last 2 month data from statement fact p3 table
  readStmtFctP4 = getLatestStmts(tc360StmtFctP4,stmtP4Schema,2,spark) # Reading last 2 month data from statement fact p4 table
  readStmtFctP5 = getLatestStmts(tc360StmtFctP5,stmtP5Schema,2,spark) # Reading last 2 month data from statement fact p5 table
  readStmtFctP3 = readStmtFctP3.withColumn('rk',rank().over(wdSpec))\
                              .where(col('rk') == 1)\
                              .select(trim(col('current_account_nbr')).alias('current_account_nbr'),col('writeoff_amt'),trim(col('external_status')).alias('external_status')
                                     ,col('credit_limit_amt'),col('curr_mnth_balance_amt')).dropDuplicates()
  readStmtFctP4 = readStmtFctP4.withColumn('rk',rank().over(wdSpec))\
                              .where(col('rk') == 1)\
                              .select(trim(col('current_account_nbr')).alias('current_account_nbr'),col('writeoff_amt'),trim(col('external_status')).alias('external_status')
                                     ,col('credit_limit_amt'),col('curr_mnth_balance_amt')).dropDuplicates()
  readStmtFctP5 = readStmtFctP5.withColumn('rk',rank().over(wdSpec))\
                              .where(col('rk') == 1)\
                              .select(trim(col('current_account_nbr')).alias('current_account_nbr'),col('writeoff_amt'),trim(col('external_status')).alias('external_status')
                                     ,col('credit_limit_amt'),col('curr_mnth_balance_amt')).dropDuplicates() 
  stmtFactDf = readStmtFctP3.union(readStmtFctP4).union(readStmtFctP5)
  stmtFactDf = stmtFactDf.join(acctLexidDf['orig_account_nbr','current_account_nbr'],['current_account_nbr'],'left')
  stmtFactDf.cache()
  return stmtFactDf
 except Exception as e:
  sys.exit("ERROR: while reading statement fact tables - " +str(e))
  
def readAppFact(extStdAppFactCur,spark):
 ###############################################################################################################################################################
 # Reading application fact P3, P4, P5 tables from from hdfs using schema                                                                                      #
 # Taking the union of all statement facts P3, P4, P5                                                                                                          #
 ###############################################################################################################################################################
 try:
  windowSpec = Window.partitionBy('logical_application_key').orderBy(desc('entered_date'))
  appFactDf = spark.read.orc(extStdAppFactCur).filter("trim(final_decision_code)='A'")\
                                                      .select(trim(col('logical_application_key')).alias('logical_application_key')
                                                      ,trim(col('final_decision_code')).alias('final_decision_code')
                                                      ,col('credit_limit_amt')
                                                      ,col('entered_date'))\
                                                      .withColumn('rk',rank().over(windowSpec))\
                                                      .filter("rk=1")\
                                                      .drop(*['entered_date','rk'])\
                                                      .dropDuplicates() 
  appFactDf.cache()
  return appFactDf
 except Exception as e:
  sys.exit("ERROR: while reading standarized application fact table - " + str(e))
  
def createLoadReadyDf(acctLexidDf,stmtFactDf,appFactDf):
 try:
  acctLexidDf = acctLexidDf.select(col('lexid1').cast('bigint').alias('lexid1')
                   ,col('current_account_nbr').cast('string').alias('current_account_nbr')
                   ,col('orig_account_nbr').cast('string').alias('orig_account_nbr')
                   ,trim(col('customer_acct_nbr')).cast('string').alias('customer_acct_nbr')
                   ,col('bin').cast('string').alias('acct_bin')
                   ,col('open_date').cast('date').alias('open_date')
                   ,col('last_plastic_date').cast('date').alias('last_plastic_date')
                   ,col('nbr_of_cards_current').cast('int').alias('nbr_of_cards_current')
                   ,col('sys').cast('string').alias('sys')
                   ,col('external_status').cast('string').alias('external_status')
                   ,col('external_status_reason_code').cast('string').alias('external_status_reason_code')
                   ,col('card_activation_flag').cast('string').alias('card_activation_flag')
                   ,col('logical_application_key').cast('string').alias('logical_application_key')).dropDuplicates().withColumn('edl_load_ts',lit(current_timestamp()))
 
  stmtFactDf = stmtFactDf.select(col('orig_account_nbr').cast('string').alias('orig_account_nbr')
                               ,col('current_account_nbr').cast('string').alias('current_account_nbr')
                               ,col('writeoff_amt').cast('double').alias('writeoff_amt')
                               ,col('external_status').cast('string').alias('external_status')
                               ,col('credit_limit_amt').cast('double').alias('credit_limit_amt')
                               ,col('curr_mnth_balance_amt').cast('double').alias('curr_mnth_balance_amt')).dropDuplicates().withColumn('edl_load_ts',lit(current_timestamp()))
 
  appFactDf = appFactDf.select(col('logical_application_key').cast('string').alias('logical_application_key')
                             ,col('final_decision_code').cast('string').alias('final_decision_code')
                             ,col('credit_limit_amt').cast('double').alias('credit_limit_amt')).dropDuplicates().withColumn('edl_load_ts',lit(current_timestamp()))
  acctLexidDf.cache()
  stmtFactDf.cache()
  appFactDf.cache()
  return acctLexidDf,stmtFactDf,appFactDf
 except Exception as e:
  sys.exit("ERROR: while forming the load ready data frame - " + str(e))
 
def precursorHiveLoad(acctLexidDf,stmtFactDf,appFactDf,pcAcctLexid,pcStmtFact,pcAppFact,writeMode,spark):
 try:
  print("######################## Start loading pre-cursor data into hive ######################################")              
  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  acctLexidDf.repartition(1000).write.mode(writeMode).format('parquet').save(pcAcctLexid)
  stmtFactDf.repartition(1000).write.mode(writeMode).format('parquet').save(pcStmtFact)
  appFactDf.repartition(1000).write.mode(writeMode).format('parquet').save(pcAppFact)
  print("######################## Pre-cursor hive data load complete ###########################################")
 except Exception as e:
  sys.exit("ERROR: while loading pre-cursor data into hive table - " + str(e))

########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 hive = readHiveContext(spark)
 configFileName,runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 accXrefSchema = createSchema(inputParams['accXrefColumnNames'])
 stmtP3Schema = createSchema(inputParams['stmtFactP3ColumnNames'])
 stmtP4Schema = createSchema(inputParams['stmtFactP4ColumnNames'])
 stmtP5Schema = createSchema(inputParams['stmtFactP5ColumnNames'])
 adP3Schema = createSchema(inputParams['adColumnsP3'])
 adP4Schema = createSchema(inputParams['adColumnsP4'])
 adP5Schema = createSchema(inputParams['adColumnsP5'])
 readLexIdDf = readSyfAcc(inputParams['lexidSyfacc'],spark)
 readAccXrefDf = readAccXref(inputParams['syf_acct_token_xref'],accXrefSchema,inputParams['readMode'],spark)
 adDf = readAccDim(inputParams['tc360AdP3'],inputParams['tc360AdP4'],inputParams['tc360AdP5'],adP3Schema,adP4Schema,adP5Schema,inputParams['readMode'],inputParams['inputFileDemiliter'],spark)
 acctLexidDf = createAccLexidDf(adDf,readLexIdDf,readAccXrefDf)
 stmtFactDf = readStmtFact(inputParams['tc360StmtFctP3'],inputParams['tc360StmtFctP4'],inputParams['tc360StmtFctP5'],stmtP3Schema,stmtP4Schema,stmtP5Schema,acctLexidDf,spark)
 appFactDf = readAppFact(inputParams['extStdAppFactCur'],spark)
 acctLexidDf,stmtFactDf,appFactDf = createLoadReadyDf(acctLexidDf,stmtFactDf,appFactDf)
 precursorHiveLoad(acctLexidDf,stmtFactDf,appFactDf,inputParams['pcAcctLexid'],inputParams['pcStmtFact'],inputParams['pcAppFact'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()