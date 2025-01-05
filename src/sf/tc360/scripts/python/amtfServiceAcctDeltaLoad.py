###############################################################################
# Script Name:amtf_new_tc360_acct_attributes.py                               #
# Purpose: To calculate new tc360 account attributes incrementally            #
# Created by: Upendra Kumar                                                   #
# Create Date: 02/28/2021                                                     #
# Modify date:                                                                #
###############################################################################

from pyspark.sql.types import FloatType,IntegerType,ArrayType,StringType,DataType,StructField,StructType,TimestampType,DateType,LongType,DecimalType
from pyspark.sql import SparkSession,SQLContext,HiveContext,Window,Row
from pyspark.sql.functions import sum as _sum,mean as _mean, stddev_pop as _stddev,col,coalesce,lit,split,trim,size,lpad,length,to_date,concat,substring,current_date,expr,datediff,udf,array,desc,date_sub,count,collect_list,max,min,to_timestamp,row_number,rank,collect_set,explode,round,current_timestamp,date_format,broadcast,countDistinct,regexp_replace,when
from math import sqrt,exp,log
from ConfigParser import ConfigParser
from time import *
import commands
import getopt
import sys
import os
sys.dont_write_bytecode = True
import datetime
from dateutil.relativedelta import relativedelta
sys.dont_write_bytecode = True
import datetime
import amtf_reusable_functions as rFunctions
############################################ Function definition section #####################################################################
## Context Build and Variable Setting
def validateArg():
    runDay = None
    printError = 'spark-submit script_name.py -f <config_file_path>/<config_file_name> -r runDay'
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f:r:')
    except getopt.error as msg:
        print('Something went wrong!')
        print('Example for entering argument to the script is:')
        sys.exit(printError)

    runDay = opts[1:]
    for opt, arg in opts:
        if(opt == '-f'):
            configFileName = arg
        elif(opt == '-r'):
            runDay = arg
    if(configFileName is None or configFileName == ''):
        print(printError)
        sys.exit('ERROR: Config File Name not provided with argument -f')
    elif(runDay is None or runDay == ''):
        print(printError)
        sys.exit('ERROR: runDay not provided with argument -r')
    return configFileName, runDay

###################################################################################################################
##Validate config file
def validateConfigFile(configFileName):
    if(not(os.path.exists(configFileName))):
        sys.exit('ERROR: The Configuration file ' + configFileName + ' does not exist')
    else:
        config = ConfigParser()
        config.optionxform = str
        config.read(configFileName)
        ## Checking the Config File Sections
        if('SCHEMA' not in config.sections()):
            sys.exit('ERROR: The Configuration file does not have section: SCHEMA')
        elif('HIVE' not in config.sections()):
            sys.exit('ERROR: The Configuration file does not have section: HIVE')
        elif('HDFSPATH' not in config.sections()):
            sys.exit('ERROR: The Configuration file does not have section: HDFSPATH')
        else:
                # check the options in each section
                if('readModeRawFile' not in config.options('FUNCTIONAL')):
                    sys.exit('ERROR: Section FUNCTIONAL does not contain readModeRawFile')
                else:
                    readMode = config.get('FUNCTIONAL', 'readModeRawFile')
                    readOptions = ['DROPMALFORMED', 'PERMISSIVE']
                if (readMode in readOptions):
                    readMode = readMode
                if ('writeModeTokenFile' not in config.options('FUNCTIONAL')):
                   sys.exit('ERROR: Section FUNCTIONAL does not contain readModeRawFile')
                else:
                     writeMode = config.get('FUNCTIONAL','writeModeTokenFile')
                     writeOptions = ['overwrite','append','ignore','error']
                     if writeMode in writeOptions:
                        writeMode = writeMode
                     else:
                       sys.exit('ERROR: input write mode seems invalid, please check back the write mode in config file')
                if ('inputFileDemiliter' not in config.options('SCHEMA')):
                    sys.exit('ERROR: Section HIVE does not contain inputFileDemiliter')
                else:
                    inputFileDemiliter = config.get('SCHEMA', 'inputFileDemiliter')
                if ('runDay' not in config.options('HIVE')):
                    sys.exit('ERROR: Section HIVE does not contain runDay')
                else:
                    runDay = config.get('HIVE', 'runDay')
                if ('wrksRawUIColumns' not in config.options('SCHEMA')):
                    sys.exit('ERROR: Section SCHEMA does not contain wrksRawUIColumns')
                else:
                    wrksRawUIColumns = config.get('SCHEMA', 'wrksRawUIColumns')
                if ('falconInjectionDate' not in config.options('HIVE')):
                    sys.exit('ERROR: Section HIVE does not contain falconInjectionDate')
                else:
                    falconInjectionDate = config.get('HIVE', 'falconInjectionDate')
                if ('pcStmtFact' not in config.options('HIVE')):
                    sys.exit('ERROR: Section HIVE does not contain pcStmtFact')
                else:
                    pcStmtFact = config.get('HIVE', 'pcStmtFact')
                if ('pcAcctLexid' not in config.options('HIVE')):
                    sys.exit('ERROR: Section HIVE does not contain pcAcctLexid')
                else:
                    pcAcctLexid = config.get('HIVE', 'pcAcctLexid')
                if ('tc360fcc' not in config.options('HDFSPATH')):
                    sys.exit('ERROR: Section HDFSPATH does not contain tc360fcc')
                else:
                    tc360fcc = config.get('HDFSPATH', 'tc360fcc')
                if ('colsAcctTable' not in config.options('SCHEMA')):
                    sys.exit('ERROR: Section SCHEMA does not contain colsAcctTable')
                else:
                    colsAcctTable = config.get('SCHEMA', 'colsAcctTable')
                if ('pcAppFact' not in config.options('HIVE')):
                    sys.exit('ERROR: Section HIVE does not contain pcAppFact')
                else:
                    pcAppFact = config.get('HIVE', 'pcAppFact')
                if ('falconRawPath' not in config.options('HIVE')):
                    sys.exit('ERROR: Section HIVE does not contain falconRawPath')
                else:
                    falconRawPath = config.get('HIVE', 'falconRawPath')
                if ('ts797ExtPath' not in config.options('HDFSPATH')):
                    sys.exit('ERROR: Section HDFSPATH does not contain ts797ExtPath')
                else:
                    ts797ExtPath = config.get('HDFSPATH', 'ts797ExtPath')
                if ('ts323ExtPath' not in config.options('HDFSPATH')):
                    sys.exit('ERROR: Section HDFSPATH does not contain ts323ExtPath')
                else:
                    ts323ExtPath = config.get('HDFSPATH', 'ts323ExtPath')
                if ('ts257ExtPath' not in config.options('HDFSPATH')):
                    sys.exit('ERROR: Section HDFSPATH does not contain ts257ExtPath')
                else:
                    ts257ExtPath = config.get('HDFSPATH', 'ts257ExtPath')
                if ('wrksUIRawExtPath' not in config.options('HDFSPATH')):
                    sys.exit('ERROR: Section HDFSPATH does not contain wrksUIRawExtPath')
                else:
                    wrksUIRawExtPath = config.get('HDFSPATH', 'wrksUIRawExtPath')
                if ('gibberish_corpus' not in config.options('HDFSPATH')):
                  sys.exit('ERROR: Section HDFSPATH does not contain gibberish_corpus')
                else:
                    gibberish_corpus = config.get('HDFSPATH','gibberish_corpus')
                if ('gibberish_threshold' not in config.options('HDFSPATH')):
                  sys.exit('ERROR: Section HDFSPATH does not contain gibberish_threshold')
                else:
                    gibberish_threshold = config.get('HDFSPATH','gibberish_threshold')
                if ('extPathTBAttr' not in config.options('HDFSPATH')):
                  sys.exit('ERROR: Section HDFSPATH does not contain extPathTBAttr')
                else:
                    extPathTBAttr = config.get('HDFSPATH','extPathTBAttr')
                if ('tc360_account_hive' not in config.options('HIVE')):
                  sys.exit('ERROR: Section HIVE does not contain tc360_account_hive')
                else:
                    tc360_account_hive = config.get('HIVE','tc360_account_hive')
                if ('extPathAcct' not in config.options('HDFSPATH')):
                  sys.exit('ERROR: Section HDFSPATH does not contain extPathAcct')
                else:
                    extPathAcct = config.get('HDFSPATH','extPathAcct')
                if ('extPathAcctDelta' not in config.options('HDFSPATH')):
                  sys.exit('ERROR: Section HDFSPATH does not contain extPathAcctDelta')
                else:
                    extPathAcctDelta = config.get('HDFSPATH','extPathAcctDelta')
                if ('edl_amf_falcon_raw' not in config.options('HIVE')):
                  sys.exit('ERROR: Section HIVE does not contain edl_amf_falcon_raw')
                else:
                    edl_amf_falcon_raw = config.get('HIVE','edl_amf_falcon_raw')
                if ('debug' not in config.options('FUNCTIONAL')):
                  sys.exit('ERROR: Section FUNCTIONAL does not contain debug')
                else:
                    debug = config.get('FUNCTIONAL','debug')
                if ('arr_size' not in config.options('FUNCTIONAL')):
                  sys.exit('ERROR: Section FUNCTIONAL does not contain arr_size')
                else:
                    arr_size = config.get('FUNCTIONAL','arr_size')
                
        return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'pcAcctLexid': pcAcctLexid
                            ,'falconRawPath': falconRawPath
                            ,'tc360_account_hive': tc360_account_hive
                            ,'pcStmtFact': pcStmtFact
                            ,'pcAppFact': pcAppFact
                            ,'tc360fcc': tc360fcc
                            ,'ts797ExtPath': ts797ExtPath
                            ,'ts323ExtPath': ts323ExtPath
                            ,'ts257ExtPath': ts257ExtPath
                            ,'wrksUIRawExtPath': wrksUIRawExtPath
                            ,'extPathAcct': extPathAcct
                            ,'extPathTBAttr': extPathTBAttr
                            ,'extPathAcctDelta': extPathAcctDelta
                            ,'runDay': runDay
                            ,'wrksRawUIColumns': wrksRawUIColumns
                            ,'falconInjectionDate': falconInjectionDate
                            ,'gibberish_corpus': gibberish_corpus
                            ,'gibberish_threshold': gibberish_threshold
                            ,'edl_amf_falcon_raw': edl_amf_falcon_raw
                            ,'colsAcctTable': colsAcctTable
                            ,'debug': debug
                            ,'arr_size': arr_size
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('Tc360AcctIncrLoad').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold','200000000')
 spark.conf.set('spark.sql.broadcastTimeout','-1')
 spark.conf.set('spark.driver.maxResultSize','0')
 spark.sparkContext.addFile('/data1/EDL/domains/amf_qa/edl_amf_qa/python/amtf_reusable_functions.py')
 spark.sql('SET spark.hadoop.hive.mapred.supports.subdirectories=true')
 spark.sql('SET mapreduce.input.fileinputformat.input.dir.recursive=true')
 spark.sql('SET spark.dynamicAllocation.enabled=true')
 spark.conf.set('spark.hadoop.mapreduce.output.textoutputformat.overwrite','true')
 spark.conf.set('spark.qubole.outputformat.overwriteFileInWrite','true')
 status,protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
 spark.sql('add jar ' + protegrityFile)
 spark.sql('add jar /opt/protegrity/pepspark/lib/pepspark-2.3.2.jar')
 spark.sql('add jar /usr/share/java/mysql-connector-java-5.1.17.jar')
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
        sys.exit('The Column Names string is blank. Please provide valid column names')
    else:
        columnStructFields = []
        for column in columnNames.split(','):
           if (column in ('MERGER_DATE','ACQ_SCORE_STRATEGY_FLIP_DATE','ACQ_LASER_STRATEGY_FLIP_DATE','ACM_SCORE_FLIP_DATE')):
              columnStructFields.append(StructField(column, LongType(), True))
           elif (column in ('EDL_LOAD_TS','load_timestamp')):
              columnStructFields.append(StructField(column, TimestampType(), True))
           elif (column in ('load_time','unload_time')):
              columnStructFields.append(StructField(column,DecimalType(13,0),True))
           elif (column in ('dynamic_field_length')):
              columnStructFields.append(StructField(column,DecimalType(5,0),True))
           elif(column in ('d_date','as_of_date')):
              columnStructFields.append(StructField(column,DateType(),True))
           else:
              columnStructFields.append(StructField(column, StringType(), True))
    schema = StructType(columnStructFields)
    return schema
########################################################################################################################

## Function to read pre-cursors table for lexid,stmt, and app fact data
def readPrecursorTables(pcAcctLexid,spark):
 try:
  lexidDf = spark.read.parquet(pcAcctLexid).drop(*['edl_load_ts'])
  return lexidDf
 except Exception as e:
  sys.exit('ERROR: while reading pre-cursor tables - ' + str(e))

## Function to read previous day run
def readPreviousDayRun(extPathAcct,previousDay,spark):
 try:
  colsASA = ['acct_sales_amt_'+ str(i+1) for i in range(10)]
  colsASTs = ['acct_sales_ts_'+ str(i+1) for i in range(10)]
  colsASVaa = ['acct_sales_vaa_scr_'+ str(i+1) for i in range(10)]
  colsASR3 = ['acct_sales_r3_scr_'+ str(i+1) for i in range(10)]
  colsASMCDI = ['acct_sales_mcdi_scr_'+ str(i+1) for i in range(10)]
  extPathAcct = extPathAcct+"/as_of_date="+previousDay.strftime('%Y-%m-%d')
  accPDR = spark.read.parquet(extPathAcct)\
                    .drop(*['edl_load_ts'])\
                    .select(col('orig_account_nbr').alias('prev_orig_account_nbr')
                           ,col('current_account_nbr').alias('prev_current_account_nbr')
                           ,col('current_account_nbr_pty').alias('prev_current_account_nbr_pty')
                           ,col('acct_total_sales_cnt').alias('prev_acct_total_sales_cnt')
                           ,col('acct_total_sales_amt').alias('prev_acct_total_sales_amt')
                           ,col('acct_total_sales_amt_avg').alias('prev_acct_total_sales_amt_avg')
                           ,col('acct_sale_per_day').alias('prev_acct_sale_per_day')
                           ,col('acct_sale_amt_per_day').alias('prev_acct_sale_amt_per_day')
                           ,col('acct_total_sales_declined_cnt').alias('prev_acct_total_sales_declined_cnt')
                           ,col('acct_total_sales_declined_amt').alias('prev_acct_total_sales_declined_amt')
                           ,col('acct_total_sales_declined_amt_avg').alias('prev_acct_total_sales_declined_amt_avg')
                           ,col('acct_total_sales_amt_std').alias('prev_acct_total_sales_amt_std')
                           ,col('acct_total_sales_declined_amt_std').alias('prev_acct_total_sales_declined_amt_std')
                           ,col('acct_fraud_claim_ls_tot').alias('prev_acct_fraud_claim_ls_tot')
                           ,col('acct_fraud_claim_other_tot').alias('prev_acct_fraud_claim_other_tot')
                           ,col('acct_fraud_claim_cnp_tot').alias('prev_acct_fraud_claim_cnp_tot')
                           ,col('acct_fraud_claim_tnf_tot').alias('prev_acct_fraud_claim_tnf_tot')
                           ,col('acct_fraud_claim_non_tnf_tot').alias('prev_acct_fraud_claim_non_tnf_tot')
                           ,col('acct_fraud_claim_non_tnf_gross_amt').alias('prev_acct_fraud_claim_non_tnf_gross_amt')
                           ,col('acct_fraud_claim_non_tnf_net_amt').alias('prev_acct_fraud_claim_non_tnf_net_amt')
                           ,col('acct_fraud_claim_tnf_gross_amt').alias('prev_acct_fraud_claim_tnf_gross_amt')
                           ,col('acct_fraud_claim_tnf_net_amt').alias('prev_acct_fraud_claim_tnf_net_amt')
                           ,col('acct_total_payment').alias('prev_acct_total_payment')
                           ,array(colsASA).alias('arrListASA'),array(colsASTs).alias('arrListASTs'),array(colsASVaa).alias('arrListASVaa'),array(colsASR3).alias('arrListASR3'),array(colsASMCDI).alias('arrListASMCDI')
                           ).dropDuplicates()
  accPDR.cache()
  return accPDR
 except Exception as e:
  sys.exit('ERROR: while reading previous day attributes from account table - ' + str(e))
 
# Function to compute running standard deviation 
def createTxnAmtLists(deltaAcctDf,accPDR):
 try:
  wdIncrSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('tran_date'))
  updateTxnsDf = deltaAcctDf.join(accPDR,deltaAcctDf['orig_account_nbr'] == accPDR['prev_orig_account_nbr'],how = 'inner') # joining to accPDR to get the approve/decline txns for update
  approvedListDf = updateTxnsDf.filter("ffsl_fraud_trancode in ('MA','CA','AP')")\
                             .withColumn('tranList',collect_list('tran_amount').over(wdIncrSpec))\
                             .groupBy('orig_account_nbr')\
                             .agg(max('tranList').alias('approvedList'))\
                             .drop(*['ffsl_fraud_trancode'])
  declinedListDf = updateTxnsDf.filter("ffsl_fraud_trancode in ('MD','CD','DP')")\
                             .withColumn('tranList',collect_list('tran_amount').over(wdIncrSpec))\
                             .groupBy('orig_account_nbr')\
                             .agg(max('tranList').alias('declinedList'))\
                             .drop(*['ffsl_fraud_trancode'])                             
  combinedListDf = approvedListDf.join(broadcast(declinedListDf),['orig_account_nbr'],'full')
  return combinedListDf 
 except Exception as e:
  sys.exit('ERROR: while forming tranasctions lists for incremental attributes - ' + str(e))

def createPreviousDayLists(deltaAcctDf):
 try:
  wdMaxListSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('tran_date_ts'))
  deltaListDf = deltaAcctDf.withColumn('saList',collect_list('tran_amount').over(wdMaxListSpec))\
                           .withColumn('saTsList',collect_list('tran_date_ts').over(wdMaxListSpec))\
                           .withColumn('saVaaList',collect_list('ffsl_vaa_risk_scor_nr').over(wdMaxListSpec))\
                           .withColumn('saR3List',collect_list('ffsl_score_2').over(wdMaxListSpec))\
                           .withColumn('saMCDIList',collect_list('ffsl_ems_frad_scor_nr').over(wdMaxListSpec))\
                           .groupBy('orig_account_nbr')\
                           .agg(max('saList').alias('saList_10')
                               ,max('saTsList').alias('saTsList_10')
                               ,max('saVaaList').alias('saVaaList_10')
                               ,max('saR3List').alias('saR3List_10')
                               ,max('saMCDIList').alias('saMCDIList_10'))
  return deltaListDf
 except Exception as e:
  sys.exit('ERROR: while creating delta columns lists - ' + str(e))

def expandArray(acctIncrAttrDf,arr_size):
 try:
  acctIncrAttrDf = acctIncrAttrDf.select(['orig_account_nbr']+['acct_total_sales_cnt']+['acct_total_sales_amt']+['acct_total_sales_amt_avg']+['acct_total_sales_amt_std']+['acct_total_sales_declined_cnt']+['acct_total_sales_declined_amt']+['acct_total_sales_declined_amt_avg']+['acct_total_sales_declined_amt_std']+['acct_fraud_claim_ls_tot']+['acct_fraud_claim_other_tot']+['acct_fraud_claim_cnp_tot']+['acct_fraud_claim_tnf_tot']+['acct_fraud_claim_non_tnf_tot']+['acct_fraud_claim_non_tnf_gross_amt']+['acct_fraud_claim_non_tnf_net_amt']+['acct_fraud_claim_tnf_gross_amt']+['acct_fraud_claim_tnf_net_amt']+['acct_total_payment']+[expr('saListFin['+str(i+1)+']') for i in range(0,10)]+[expr('saTsListFin['+str(i+1)+']') for i in range(0,10)]+[expr('saVaaListFin['+str(i+1)+']') for i in range(0,10)]+[expr('saR3ListFin['+str(i+1)+']') for i in range(0,10)]+[expr('saMCDIListFin['+str(i+1)+']') for i in range(0,10)])
  saColumns = ['orig_account_nbr']+['acct_total_sales_cnt']+['acct_total_sales_amt']+['acct_total_sales_amt_avg']+['acct_total_sales_amt_std']+['acct_total_sales_declined_cnt']+['acct_total_sales_declined_amt']+['acct_total_sales_declined_amt_avg']+['acct_total_sales_declined_amt_std']+['acct_fraud_claim_ls_tot']+['acct_fraud_claim_other_tot']+['acct_fraud_claim_cnp_tot']+['acct_fraud_claim_tnf_tot']+['acct_fraud_claim_non_tnf_tot']+['acct_fraud_claim_non_tnf_gross_amt']+['acct_fraud_claim_non_tnf_net_amt']+['acct_fraud_claim_tnf_gross_amt']+['acct_fraud_claim_tnf_net_amt']+['acct_total_payment']+['acct_sales_amt_'+str(i+1) for i in range(0,10)]+['acct_sales_ts_'+str(i+1) for i in range(0,10)]+['acct_sales_vaa_scr_'+str(i+1) for i in range(0,10)]+['acct_sales_r3_scr_'+str(i+1) for i in range(0,10)]+['acct_sales_mcdi_scr_'+str(i+1) for i in range(0,10)]
  acctIncrAttrDf = acctIncrAttrDf.drop(*['saListFin','saTsListFin','saVaaListFin','saR3ListFin','saMCDIListFin']).toDF(*saColumns)
  return acctIncrAttrDf
 except Exception as e:
  sys.exit('ERROR: while expanding lists of size 10 - ' + str(e))

## Function to compute delta account attributes
def createDeltaAcctAttrDf(accXref,readFalconDf,accPDR,fccDf,previousDay,spark): 
 try:
  deltaAcctDf = accXref.join(readFalconDf, on = accXref['current_account_nbr_pty'] == readFalconDf['ffsl_account_number'], how = 'inner')
  deltaListDf = createPreviousDayLists(deltaAcctDf)
  newAcctFalconAttrDf = computeDeltaFalconAttr(deltaAcctDf,accPDR)
  deltaFccAttrDf = calculateFccAttributes(deltaAcctDf,fccDf,previousDay)
  combinedListDf = createTxnAmtLists(deltaAcctDf,accPDR)
  # Forming delta dataframe by joining all subsdiary data frames
  deltaAcctAttrDf = accXref.join(combinedListDf,['orig_account_nbr'],'left') # joining actXref with approved and declined txns from falcon previous day transactions
  deltaAcctAttrDf = deltaAcctAttrDf.join(deltaListDf,['orig_account_nbr'],'left') # joining with last most recent txns previous day
  deltaAcctAttrDf = deltaAcctAttrDf.join(newAcctFalconAttrDf,['orig_account_nbr'],'left')
  deltaAcctAttrDf = deltaAcctAttrDf.join(deltaFccAttrDf,['orig_account_nbr'], 'left') 
  deltaAcctAttrDf = deltaAcctAttrDf.dropDuplicates()
  
  deltaAcctAttrDf.cache()
  return deltaAcctAttrDf
 except Exception as e:
  sys.exit('ERROR: while computing delta account attributes - ' + str(e))

## Function to compute falcon delta attributes  
def computeDeltaFalconAttr(deltaAcctDf,accPDR):
 try:
  deltaWdSpec = Window.partitionBy('orig_account_nbr').orderBy(lit('X'))
  accPDR = accPDR.withColumn('prev_current_account_nbr_pty',explode(split(regexp_replace(col('prev_current_account_nbr_pty').cast('string'), '([^0-9a-zA-Z,])',''), ',')))  
  newAcctFalconDf = deltaAcctDf.join(accPDR, on = (deltaAcctDf['orig_account_nbr'] == accPDR['prev_orig_account_nbr']) & (deltaAcctDf['current_account_nbr_pty'] == accPDR['prev_current_account_nbr_pty']), how = 'left_anti')
  newAcctFalconDf = newAcctFalconDf.select(col('orig_account_nbr'),col('tran_amount'),col('tran_date'),col('ffsl_fraud_trancode'),col('tran_date_ts')).dropDuplicates()
  newAcctFalconAttrDf = newAcctFalconDf.withColumn('delta_acct_total_sales_cnt',_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then 1 else 0 end")).over(deltaWdSpec))\
                              .withColumn('delta_acct_total_sales_amt',_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then tran_amount else 0 end")).over(deltaWdSpec))\
                              .withColumn('delta_acct_total_sales_amt_avg',_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then tran_amount end")).over(deltaWdSpec))\
                              .withColumn('delta_acct_total_sales_declined_cnt',_sum(expr("case when ffsl_fraud_trancode in ('MD','CD','DP') then 1 else 0 end")).over(deltaWdSpec))\
                              .withColumn('delta_acct_total_sales_declined_amt',_sum(expr("case when ffsl_fraud_trancode in ('MD','CD','DP') then tran_amount else 0 end")).over(deltaWdSpec))\
                              .withColumn('delta_acct_total_sales_declined_amt_avg',_mean(expr("case when ffsl_fraud_trancode in ('MD','CD','DP') then tran_amount end")).over(deltaWdSpec))\
                              .withColumn('delta_acct_total_sales_amt_std',_stddev(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then tran_amount end")).over(deltaWdSpec))\
                              .withColumn('delta_acct_total_sales_declined_amt_std',_stddev(expr("case when ffsl_fraud_trancode in ('MD','CD','DP') then tran_amount end")).over(deltaWdSpec))\
                              .withColumn('delta_acct_total_payment',_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then tran_amount else 0 end")).over(deltaWdSpec))\
                              .drop(*['tran_amount','ffsl_fraud_trancode','ffsl_total_pay_amt','tran_date','tran_date_ts']).dropDuplicates()
  return newAcctFalconAttrDf
 except Exception as e:
  sys.exit('ERROR: while computing new falcon attributes - ' + str(e))
 
## Function to calculate incremental attributes 
def calculateIncrAttributes(accPDR,deltaAcctAttrDf,falconInjectionDate,arr_size,runDay):
 # Registering udf's
 try:
  # UDF declaration section
  updateNewValueUdf = udf(rFunctions.updateNewValue,FloatType())
  computeIncrMeanUdf = udf(rFunctions.computeIncrMean,FloatType())
  computeIncrStddevUdf = udf(rFunctions.computeIncrementalStddev,FloatType())
  arrayConcatenateSaUdf = udf(rFunctions.arrayConcatenate,ArrayType(FloatType()))
  arrayConcatenateSaTsUdf = udf(rFunctions.arrayConcatenate,ArrayType(TimestampType()))
  arrayConcatenateSaVaaUdf = udf(rFunctions.arrayConcatenate,ArrayType(IntegerType()))
  arrayConcatenateSaR3Udf = udf(rFunctions.arrayConcatenate,ArrayType(IntegerType()))
  arrayConcatenateSaMCDIUdf = udf(rFunctions.arrayConcatenate,ArrayType(IntegerType()))
  
  oldAccDf = accPDR.join(deltaAcctAttrDf, on = accPDR['prev_orig_account_nbr'] == deltaAcctAttrDf['orig_account_nbr'], how = 'left_anti')
  newAccDf = deltaAcctAttrDf.join(accPDR, on = accPDR['prev_orig_account_nbr'] == deltaAcctAttrDf['orig_account_nbr'], how = 'left_anti')
  updateAccDf = accPDR.join(deltaAcctAttrDf, on = accPDR['prev_orig_account_nbr'] == deltaAcctAttrDf['orig_account_nbr'], how = 'inner').drop(*['current_account_nbr_pty']).dropDuplicates()
  updateIncrAttrDf = updateAccDf\
                              .withColumn('acct_total_sales_cnt',updateNewValueUdf(col('prev_acct_total_sales_cnt'),col('delta_acct_total_sales_cnt')))\
                              .withColumn('acct_total_sales_amt',updateNewValueUdf(col('prev_acct_total_sales_amt'),col('delta_acct_total_sales_amt')))\
                              .withColumn('acct_total_sales_amt_avg',computeIncrMeanUdf(updateAccDf.approvedList,col('prev_acct_total_sales_amt_avg'),col('prev_acct_total_sales_cnt')))\
                              .withColumn('acct_total_sales_amt_std',computeIncrStddevUdf(updateAccDf.approvedList,col('prev_acct_total_sales_cnt')*(col('prev_acct_total_sales_amt_std')**2),updateAccDf['prev_acct_total_sales_amt_avg'],updateAccDf['prev_acct_total_sales_cnt']))\
                              .withColumn('acct_total_sales_declined_cnt',updateNewValueUdf(col('prev_acct_total_sales_declined_cnt'),col('delta_acct_total_sales_declined_cnt')))\
                              .withColumn('acct_total_sales_declined_amt',updateNewValueUdf(col('prev_acct_total_sales_declined_amt'),col('delta_acct_total_sales_declined_amt')))\
                              .withColumn('acct_total_sales_declined_amt_avg',computeIncrMeanUdf(updateAccDf.declinedList,col('prev_acct_total_sales_declined_amt_avg'),col('prev_acct_total_sales_declined_cnt')))\
                              .withColumn('acct_total_sales_declined_amt_std',computeIncrStddevUdf(updateAccDf.declinedList,col('prev_acct_total_sales_declined_cnt')*(col('prev_acct_total_sales_declined_amt_std')**2),updateAccDf['prev_acct_total_sales_declined_amt_avg'],updateAccDf['prev_acct_total_sales_declined_cnt']))\
                              .withColumn('acct_fraud_claim_ls_tot',updateNewValueUdf(col('prev_acct_fraud_claim_ls_tot'),col('acct_fraud_claim_ls_tot')))\
                              .withColumn('acct_fraud_claim_other_tot',updateNewValueUdf(col('prev_acct_fraud_claim_other_tot'),col('acct_fraud_claim_other_tot')))\
                              .withColumn('acct_fraud_claim_cnp_tot',updateNewValueUdf(col('prev_acct_fraud_claim_cnp_tot'),col('acct_fraud_claim_cnp_tot')))\
                              .withColumn('acct_fraud_claim_tnf_tot',updateNewValueUdf(col('prev_acct_fraud_claim_tnf_tot'),col('acct_fraud_claim_tnf_tot')))\
                              .withColumn('acct_fraud_claim_non_tnf_tot',updateNewValueUdf(col('prev_acct_fraud_claim_non_tnf_tot'),col('acct_fraud_claim_non_tnf_tot')))\
                              .withColumn('acct_fraud_claim_non_tnf_gross_amt',updateNewValueUdf(col('prev_acct_fraud_claim_non_tnf_gross_amt'),col('acct_fraud_claim_non_tnf_gross_amt')))\
                              .withColumn('acct_fraud_claim_non_tnf_net_amt',updateNewValueUdf(col('prev_acct_fraud_claim_non_tnf_net_amt'),col('acct_fraud_claim_non_tnf_net_amt')))\
                              .withColumn('acct_fraud_claim_tnf_gross_amt',updateNewValueUdf(col('prev_acct_fraud_claim_tnf_gross_amt'),col('acct_fraud_claim_tnf_gross_amt')))\
                              .withColumn('acct_fraud_claim_tnf_net_amt',updateNewValueUdf(col('prev_acct_fraud_claim_tnf_net_amt'),col('acct_fraud_claim_tnf_net_amt')))\
                              .withColumn('acct_total_payment',updateNewValueUdf(col('prev_acct_total_payment'),col('delta_acct_total_payment')))\
                              .withColumn('saListFin',arrayConcatenateSaUdf(col('saList_10').cast('array<float>'),col('arrListASA'),lit(int(arr_size))))\
                              .withColumn('saTsListFin',arrayConcatenateSaTsUdf(col('saTsList_10').cast('array<timestamp>'),col('arrListASTs'),lit(int(arr_size))))\
                              .withColumn('saVaaListFin',arrayConcatenateSaVaaUdf(col('saVaaList_10').cast('array<int>'),col('arrListASVaa'),lit(int(arr_size))))\
                              .withColumn('saR3ListFin',arrayConcatenateSaR3Udf(col('saR3List_10').cast('array<int>'),col('arrListASR3'),lit(int(arr_size))))\
                              .withColumn('saMCDIListFin',arrayConcatenateSaMCDIUdf(col('saMCDIList_10').cast('array<int>'),col('arrListASMCDI'),lit(int(arr_size))))\
                              .select(col('orig_account_nbr'),col('acct_total_sales_cnt'),col('acct_total_sales_amt'),col('acct_total_sales_amt_avg'),col('acct_total_sales_amt_std'),col('acct_total_sales_declined_cnt'),col('acct_total_sales_declined_amt'),col('acct_total_sales_declined_amt_avg'),col('acct_total_sales_declined_amt_std'),col('acct_fraud_claim_ls_tot'),col('acct_fraud_claim_other_tot'),col('acct_fraud_claim_cnp_tot'),col('acct_fraud_claim_tnf_tot'),col('acct_fraud_claim_non_tnf_tot'),col('acct_fraud_claim_non_tnf_gross_amt'),col('acct_fraud_claim_non_tnf_net_amt'),col('acct_fraud_claim_tnf_gross_amt'),col('acct_fraud_claim_tnf_net_amt'),col('acct_total_payment'),col('saListFin'),col('saTsListFin'),col('saVaaListFin'),col('saR3ListFin'),col('saMCDIListFin'))
 
  oldIncrAttrDf = oldAccDf\
                           .withColumn('acct_total_sales_cnt',col('prev_acct_total_sales_cnt'))\
                           .withColumn('acct_total_sales_amt',col('prev_acct_total_sales_amt'))\
                           .withColumn('acct_total_sales_amt_avg',col('prev_acct_total_sales_amt_avg'))\
                           .withColumn('acct_total_sales_amt_std',col('prev_acct_total_sales_amt_std'))\
                           .withColumn('acct_total_sales_declined_cnt',col('prev_acct_total_sales_declined_cnt'))\
                           .withColumn('acct_total_sales_declined_amt',col('prev_acct_total_sales_declined_amt'))\
                           .withColumn('acct_total_sales_declined_amt_avg',col('prev_acct_total_sales_declined_amt'))\
                           .withColumn('acct_total_sales_declined_amt_std',col('prev_acct_total_sales_declined_amt_std'))\
                           .withColumn('acct_fraud_claim_ls_tot',col('prev_acct_fraud_claim_ls_tot'))\
                           .withColumn('acct_fraud_claim_other_tot',col('prev_acct_fraud_claim_other_tot'))\
                           .withColumn('acct_fraud_claim_cnp_tot',col('prev_acct_fraud_claim_cnp_tot'))\
                           .withColumn('acct_fraud_claim_tnf_tot',col('prev_acct_fraud_claim_tnf_tot'))\
                           .withColumn('acct_fraud_claim_non_tnf_tot',col('prev_acct_fraud_claim_non_tnf_tot'))\
                           .withColumn('acct_fraud_claim_non_tnf_gross_amt',col('prev_acct_fraud_claim_non_tnf_gross_amt'))\
                           .withColumn('acct_fraud_claim_non_tnf_net_amt',col('prev_acct_fraud_claim_non_tnf_net_amt'))\
                           .withColumn('acct_fraud_claim_tnf_gross_amt',col('prev_acct_fraud_claim_tnf_gross_amt'))\
                           .withColumn('acct_fraud_claim_tnf_net_amt',col('prev_acct_fraud_claim_tnf_net_amt'))\
                           .withColumn('acct_total_payment',col('prev_acct_total_payment'))\
                           .withColumn('saListFin',col('arrListASA'))\
                           .withColumn('saTsListFin',col('arrListASTs'))\
                           .withColumn('saVaaListFin',col('arrListASVaa'))\
                           .withColumn('saR3ListFin',col('arrListASR3'))\
                           .withColumn('saMCDIListFin',col('arrListASMCDI'))\
                           .select(col('prev_orig_account_nbr').alias('orig_account_nbr'),col('acct_total_sales_cnt'),col('acct_total_sales_amt'),col('acct_total_sales_amt_avg'),col('acct_total_sales_amt_std'),col('acct_total_sales_declined_cnt'),col('acct_total_sales_declined_amt'),col('acct_total_sales_declined_amt_avg'),col('acct_total_sales_declined_amt_std'),col('acct_fraud_claim_ls_tot'),col('acct_fraud_claim_other_tot'),col('acct_fraud_claim_cnp_tot'),col('acct_fraud_claim_tnf_tot'),col('acct_fraud_claim_non_tnf_tot'),col('acct_fraud_claim_non_tnf_gross_amt'),col('acct_fraud_claim_non_tnf_net_amt'),col('acct_fraud_claim_tnf_gross_amt'),col('acct_fraud_claim_tnf_net_amt'),col('acct_total_payment'),col('saListFin'),col('saTsListFin'),col('saVaaListFin'),col('saR3ListFin'),col('saMCDIListFin'))
 
  newIncrAttrDf = newAccDf\
                          .select(col('orig_account_nbr')
                                  ,col('delta_acct_total_sales_cnt').alias('acct_total_sales_cnt')
                                  ,col('delta_acct_total_sales_amt').alias('acct_total_sales_amt')
                                  ,col('delta_acct_total_sales_amt_avg').alias('acct_total_sales_amt_avg')
                                  ,col('delta_acct_total_sales_amt_std').alias('acct_total_sales_amt_std')
                                  ,col('delta_acct_total_sales_declined_cnt').alias('acct_total_sales_declined_cnt')
                                  ,col('delta_acct_total_sales_declined_amt').alias('acct_total_sales_declined_amt')
                                  ,col('delta_acct_total_sales_declined_amt_avg').alias('acct_total_sales_declined_amt_avg')
                                  ,col('delta_acct_total_sales_declined_amt_std').alias('acct_total_sales_declined_amt_std')
                                  ,col('acct_fraud_claim_ls_tot')
                                  ,col('acct_fraud_claim_other_tot')
                                  ,col('acct_fraud_claim_cnp_tot')
                                  ,col('acct_fraud_claim_tnf_tot')
                                  ,col('acct_fraud_claim_non_tnf_tot')
                                  ,col('acct_fraud_claim_non_tnf_gross_amt')
                                  ,col('acct_fraud_claim_non_tnf_net_amt')
                                  ,col('acct_fraud_claim_tnf_gross_amt')
                                  ,col('acct_fraud_claim_tnf_net_amt'),col('delta_acct_total_payment').alias('acct_total_payment'),col('saList_10').alias('saListFin'),col('saTsList_10').alias('saTsListFin'),col('saVaaList_10').alias('saVaaListFin'),col('saR3List_10').alias('saR3ListFin'),col('saMCDIList_10').alias('saMCDIListFin'))
                                  
  acctIncrAttrDf = updateIncrAttrDf.union(oldIncrAttrDf).union(newIncrAttrDf) # Forming union of all old,update, new dataframes
  acctIncrAttrDf = expandArray(acctIncrAttrDf,arr_size)       
  acctIncrAttrDf.cache()
  return acctIncrAttrDf
 except Exception as e:
  sys.exit('ERROR: while computing incremental account attributes - ' + str(e))
  
def readFCC(tc360fcc,previousDay,spark):
 try:
  fccDf = spark.read.parquet(tc360fcc).where(col('reported_date') <= lit(previousDay.strftime('%Y-%m-%d')))\
                                     .select(col('case_id')
                                            ,col('current_account_nbr_pty').alias('fcc_current_account_nbr_pty')
                                            ,col('loss_type_cd')
                                            ,col('possible_fraud_cd'),col('gross_fraud_amt'),col('net_fraud_amt')).dropDuplicates()
  fccDf.cache()
  return fccDf
 except Exception as e:
  sys.exit('ERROR: while reading fraud claim case table for previous day - ' + str(e))
  
# formatting df's to use in account attribute computation
def deltaFormatDfs(lexidDf,edl_amf_falcon_raw,falconRawPath,debug,runDay,previousDay,spark):
 try:
  wdSpecAccStatus =  Window.partitionBy('orig_account_nbr').orderBy(lit('X'))
  accXref = lexidDf[['orig_account_nbr','current_account_nbr','customer_acct_nbr','external_status']]\
           .withColumn('count',count('current_account_nbr').over(wdSpecAccStatus))\
           .withColumn('allClsCnt',_sum(expr("case when (trim(external_status) not in ('A','') and external_status is not null) and orig_account_nbr <> '0000000000000000' then 1 else 0 end")).over(wdSpecAccStatus))\
           .where(col('count') != col('allClsCnt'))\
           .drop(*['external_status','count','allClsCnt','current_account_nbr'])\
           .dropDuplicates()\
           .withColumnRenamed('customer_acct_nbr','current_account_nbr_pty')
  lexidDf = lexidDf.filter("(trim(external_status) in ('A','') or external_status is null) and orig_account_nbr <> '0000000000000000'")\
                   .drop(*['external_status','external_status_reason_code'])\
                   .dropDuplicates()\
                   .groupBy('orig_account_nbr')\
                   .agg(coalesce(collect_set('lexid1'),array().cast('array<bigint>')).alias('cust_lexid')
                       ,coalesce(collect_set('current_account_nbr'),array().cast('array<string>')).alias('current_account_nbr')
                       ,coalesce(collect_set('customer_acct_nbr'),array().cast('array<string>')).alias('current_account_nbr_pty')
                       ,max('sys').alias('sys'),max('acct_bin').alias('acct_bin')
                       ,to_date(max('last_plastic_date')).alias('acct_last_card_issue_date')
                       ,max('nbr_of_cards_current').alias('acct_total_cards_issued')
                       ,min('open_date').alias('acct_open_date'))\
                   .withColumn('acct_days_on_book',datediff(lit(runDay),to_date('acct_open_date')))\
                   .withColumn('acct_days_since_last_card',datediff(lit(runDay),to_date('acct_last_card_issue_date')))\
                   .withColumn('acct_card_network',expr("case when sys in ('6362') then 'DISC' \
                                                                        when sys in ('1364','1366','1409','1192','1246','1192','1468','1469','1312','1410','1408','1136','1363','1365','1327','1326','1407','1411','3935') then 'MC' \
                                                                        when sys in ('5156','5166','3179','5160','3941','3394','3394','4543','3659','3665','5168','5931','3895','3894') then 'VISA' \
                                                                        else 'PLCC' end"))
     
  if debug == 'no':      
       falconDf = spark.sql("""select * from """ + edl_amf_falcon_raw).where(col('as_of_date') == lit(previousDay.strftime('%Y-%m-%d')))
  else:
       falconDf = spark.read.orc(falconRawPath).where(col('as_of_date') == lit(previousDay.strftime('%Y-%m-%d')))
  
  readFalconDf = falconDf.select(col('ffsl_account_number')
                             ,col('ffsl_tran_amount').alias('tran_amount')
                             ,col('ffsl_fraud_trancode')
                             ,col('ffsl_spcl_cndt_in')
                             ,trim(col('ffsl_ecmm_scrt_ucaf_cd')).alias('ffsl_ecmm_scrt_ucaf_cd')
                             ,trim(col('ffsl_ecom_in')).alias('ffsl_ecom_in')
                             ,trim(col('ffsl_mc_aav_ctrl')).alias('ffsl_mc_aav_ctrl')
                             ,trim(col('ffsl_frad_cdk01_tx')).alias('ffsl_frad_cdk01_tx')
                             ,trim(col('ffsl_mrch_ctry_cd')).alias('ffsl_mrch_ctry_cd')
                             ,trim(col('ffsl_cvv2_cvc2_otcm_cd')).alias('ffsl_cvv2_cvc2_otcm_cd')
                             ,col('ffsl_vaa_risk_scor_nr')
                             ,col('ffsl_ems_frad_scor_nr')
                             ,col('ffsl_mrch_nm')
                             ,trim(col('ffsl_addr_vrfc_otcm_cd')).alias('ffsl_addr_vrfc_otcm_cd')
                             ,col('ffsl_score_2')
                             ,col('ffsl_merch_cat_code')
                             ,col('ffsl_frad_mrch_id')
                             ,col('ffsl_total_pay_amt')
                             ,lpad(trim(col('ffsl_entry_mode_ind')),2,'0').alias('ffsl_entry_mode_ind')
                             ,col('ffsl_date_yy'),col('ffsl_date_mm'),col('ffsl_date_dd'),col('ffsl_time_hh'),col('ffsl_time_mm'),col('ffsl_time_ss'))\
                             .dropDuplicates()\
                             .withColumn('tran_date',to_date(concat(substring(current_date(),1,2),col('ffsl_date_yy'),lit('-'),col('ffsl_date_mm'),lit('-'),col('ffsl_date_dd'))))\
                             .withColumn('tran_date_ts',to_timestamp(concat(substring(current_date(),1,2),col('ffsl_date_yy'),lit('-'),col('ffsl_date_mm'),lit('-'),col('ffsl_date_dd'),lit(' '),col('ffsl_time_hh'),lit(':'),col('ffsl_time_mm'),lit(':'),col('ffsl_time_ss')),'yyyy-MM-dd HH:mm:ss'))\
                             .withColumn('ffsl_mrch_ctry_cd',expr("ptyUnprotectStr(ffsl_mrch_ctry_cd,'dtStrNPL')"))
  lexidDf.cache()
  readFalconDf.cache()
  accXref.cache()
  return lexidDf,readFalconDf,accXref
 except Exception as e:
  sys.exit('ERROR: while formatting the dataframes as per the script requirements - ' + str(e))

####################################################################################################################################
## Calculating C360 FCC attributes
def calculateFccAttributes(lexidDf,fccDf,previousDay):
 #################################################################################################################################################
 # Performing the aggregations on data frame and calculating attributes from fraud claim case                                                    #
 # Computing the distinct count of case id's                                                                                                     #
 # Joining the dataframes to get the look-up fields                                                                                              #
 #################################################################################################################################################
 try:
  fccDf = fccDf.where(col('reported_date') == lit(previousDay.strftime('%Y-%m-%d')))
  fccAggDf = lexidDf[['current_account_nbr_pty','orig_account_nbr']].join(broadcast(fccDf), on = (lexidDf['current_account_nbr_pty'] == fccDf['fcc_current_account_nbr_pty']), how = 'left')
  fccAttrDf = fccAggDf.groupBy('orig_account_nbr')\
                     .agg(countDistinct(expr("case when trim(loss_type_cd) in ('00','01') then case_id end")).alias('acct_fraud_claim_ls_tot')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) in ('02','04','05') then case_id end")).alias('acct_fraud_claim_other_tot')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '06' then case_id end")).alias('acct_fraud_claim_cnp_tot')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '03' then case_id end")).alias('acct_fraud_claim_tnf_tot')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) <> '03' then case_id end")).alias('acct_fraud_claim_non_tnf_tot')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_non_tnf_gross_amt')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' then net_fraud_amt else 0 end")).alias('acct_fraud_claim_non_tnf_net_amt')\
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_tnf_gross_amt')\
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' then net_fraud_amt else 0 end")).alias('acct_fraud_claim_tnf_net_amt')\
                                       ).dropDuplicates()
  return fccAttrDf
 except Exception as e:
  sys.exit('ERROR: while computing fraud claim case attributes - ' + str(e))
  
def createLoadReadyDf(finalIncrAttrDf,runDay,spark):
 ##Forming final tc360 account level attributes data frame
 try:
  tc360DeltaAcctAttrDF = finalIncrAttrDf.withColumn('edl_load_ts',lit(current_timestamp())).withColumn('as_of_date',lit(runDay))
  return tc360DeltaAcctAttrDF
 except Exception as e:
  sys.exit('ERROR: while creating load ready dataframe - ' + str(e))

#######################################################################################################################
# Function for loading the C360 attributes hive table                                                                 #
#######################################################################################################################
def loadTc360AcctAttrHive(tc360AcctAttrDF,extPathAcctDelta,writeMode,spark):
 print('######################## Start loading delta tc360 attribute hive table ######################################')              
 try:
  spark.sql('set hive.exec.dynamic.partition=true')
  spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
  tc360AcctAttrDF.write.partitionBy('as_of_date').mode(writeMode).format('parquet').save(extPathAcctDelta)
  print('######################## Delta tc360 attribute hive table load complete ###########################################')
 except Exception as e:
  sys.exit('ERROR: while loading incremental account attributes into hive - ' + str(e))

########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel('ERROR')
 sqlContext = createSQLContext(spark)
 hive = readHiveContext(spark)
 configFileName, runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 previousDay = rFunctions.getPreviousRunDay(runDay)
 fccDf = readFCC(inputParams['tc360fcc'],previousDay,spark)
 lexidDf = readPrecursorTables(inputParams['pcAcctLexid'],spark)
 lexidDf,readFalconDf,accXref = deltaFormatDfs(lexidDf,inputParams['edl_amf_falcon_raw'],inputParams['falconRawPath'],inputParams['debug'],runDay,previousDay,spark)
 accPDR = readPreviousDayRun(inputParams['extPathAcct'],previousDay,spark)
 deltaAcctAttrDf = createDeltaAcctAttrDf(accXref,readFalconDf,accPDR,fccDf,previousDay,spark)
 acctIncrAttrDf = calculateIncrAttributes(accPDR,deltaAcctAttrDf,inputParams['falconInjectionDate'],inputParams['arr_size'],runDay)
 tc360DeltaAcctAttrDF = createLoadReadyDf(acctIncrAttrDf,runDay,spark)
 loadTc360AcctAttrHive(tc360DeltaAcctAttrDF,inputParams['extPathAcctDelta'],inputParams['writeMode'],spark)
 
if __name__ == '__main__':
    main()