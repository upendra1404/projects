###############################################################################
# Script Name:amf_c360_attributes_script.py                                   #
# Purpose: To calculate C360 attributes and load those into EDL hive table    #
# Created by: Upendra Kumar                                                   #
# Create Date: 10/28/2020                                                     #
# Modify date: 12/28/2020                                                     #
###############################################################################



from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from ConfigParser import ConfigParser
from pyspark.sql.functions import mean as _mean, stddev_pop as _stddev, col
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
                if ('c360AmfQA' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain c360AmfQA")
                else:
                    c360AmfQA = config.get('HIVE', 'c360AmfQA')
                if ('lexidSyfacc' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain lexidSyfacc")
                else:
                    lexidSyfacc = config.get('HIVE', 'lexidSyfacc')
                if ('edl_amf_falcon_raw' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain edl_amf_falcon_raw")
                else:
                    edl_amf_falcon_raw = config.get('HIVE', 'edl_amf_falcon_raw')
                if ('edl_amf_c360_hive_ldr' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain edl_amf_c360_hive_ldr")
                else:
                    edl_amf_c360_hive_ldr = config.get('HIVE','edl_amf_c360_hive_ldr')
                if ('accXrefColumnNames' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain accXrefColumnNames")
                else:
                    accXrefColumnNames = config.get('SCHEMA','accXrefColumnNames')
        return {

                            'readMode': readMode,
                            'writeMode': writeMode,
                            'accXrefColumnNames': accXrefColumnNames,
                            'edl_amf_lexid_syfacc': edl_amf_lexid_syfacc,
                            'edl_amf_acc_xref': edl_amf_acc_xref,
                            'edl_amf_falcon_raw': edl_amf_falcon_raw,
                            'edl_amf_c360_hive_ldr': edl_amf_c360_hive_ldr,
                            'syf_acct_token_xref': syf_acct_token_xref,
                            'lexidSyfacc': lexidSyfacc,
                            'c360AmfQA': c360AmfQA,
                            'runDay': runDay
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
## Reading lexid_sysfact table
def readLexID(lexidSyfacc,spark):
 readLexIdDF = spark.read.parquet(lexidSyfacc).filter("lexid_score1 >= 80").dropDuplicates()
 readLexIdDF = readLexIdDF.select(trim(col('nbr_cardh_acct')).alias("nbr_cardh_acct"),"lexid1","lexid_score1")
 readLexIdDF.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return readLexIdDF

#########################################################################################################################
## Reading account xref table 
def readAccXref(syf_acct_token_xref,accXrefSchema,readMode,spark):
 spark.read.csv(syf_acct_token_xref,inferSchema=True,schema=accXrefSchema,mode=readMode).createOrReplaceTempView("syf_acct_token_xref_vw")  
 readAccXrefDF = spark.sql("""select trim(customer_acct_nbr_syf) as customer_acct_nbr_syf,
                                     customer_acct_nbr 
                                     from syf_acct_token_xref_vw""")
 readAccXrefDF.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return readAccXrefDF
## Reading all account dim tables - P3,P4,P5
def readAccDim(hive):
 accDimP3 = hive.sql("""select current_account_nbr,orig_account_nbr from edlstg.ext_cdci_account_dim_xcdcip3""").dropDuplicates()
 accDimP4 = hive.sql("""select current_account_nbr,orig_account_nbr from edlstg.ext_cdci_account_dim_xcdcip4""").dropDuplicates()
 accDimP5 = hive.sql("""select current_account_nbr,orig_account_nbr from edlstg.ext_cdci_account_dim_xcdcip5""").dropDuplicates()
 accDimDF = accDimP3.union(accDimP4).union(accDimP5)
 return accDimDF

##########################################################################################################################
## Join lexid sysfact and account xref to fetch customer_acct_nbr
## fetch not null account number from xref table
def readCustAcc(readLexIdDF,readAccXrefDF,accDimDF,spark):
 readLexIdDF.createOrReplaceTempView("readLexIdDF_vw")
 readAccXrefDF.createOrReplaceTempView("readAccXrefDF_vw")
 accDimDF.createOrReplaceTempView("accDimDF_vw")
 spark.sql("""select a.*,
                                     b.* 
                                     from readLexIdDF_vw a inner join readAccXrefDF_vw b
                                     on a.nbr_cardh_acct = b.customer_acct_nbr_syf
                                     """).createOrReplaceTempView("intermediateDF")
 readCustAccNoDF = spark.sql("""select
                     intermediateDF.*,
                     orig_account_nbr
                     from intermediateDF inner join accDimDF_vw
                     on trim(current_account_nbr) = trim(customer_acct_nbr_syf)
                     """)
 readCustAccNoDF = readCustAccNoDF.select(trim(col('customer_acct_nbr')).alias("customer_acct_nbr"),
                                               col('customer_acct_nbr_syf').alias("current_account_nbr"),
                                               col('orig_account_nbr'),
                                               col('lexid1').alias("lexid")).dropDuplicates()
 readCustAccNoDF.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return readCustAccNoDF

################################################################################################################################
### Reading Falcon Table for last 30 days from run date
## Dropping the duplicates based on date timestamp
## filtering the fraud txn codes ('MA', 'MD', 'MS', 'CA', 'CD', 'CS')
def readFalconTable(edl_amf_falcon_raw,runDay,spark):
 ###############################################################################################################################################################
 # Dropping duplicates records from falcon raw table based on timestamp fields - ffsl_date_yy,ffsl_date_mm,ffsl_date_dd,ffsl_time_hh,ffsl_time_mm,ffsl_time_ss #
 ###############################################################################################################################################################
 spark.sql("""select trim(ffsl_account_number) as ffsl_account_number,
                                      ffsl_tran_amount,
                                      ffsl_strategy,
                                      ptyUnprotectStr(ffsl_mrch_ctry_cd,'dtStrNPL') as ffsl_mrch_ctry_cd,
                                      ffsl_fraud_trancode,
                                      trim(ffsl_date_yy) as ffsl_date_yy,
                                      trim(ffsl_date_mm) as ffsl_date_mm,
                                      trim(ffsl_date_dd) as ffsl_date_dd,
                                      trim(ffsl_time_hh) as ffsl_time_hh,
                                      trim(ffsl_time_mm) as ffsl_time_mm,
                                      trim(ffsl_time_ss) as ffsl_time_ss 
                                      from  """ + edl_amf_falcon_raw
                                      + """ where ffsl_fraud_trancode in ('MA', 'MD', 'MS', 'CA', 'CD', 'CS') 
 and to_date(concat(substr(current_date(),1,2),trim(ffsl_date_yy),'-',trim(ffsl_date_mm),'-',trim(ffsl_date_dd))) >= date_sub(""" + runDay + """,int(31))
 and to_date(concat(substr(current_date(),1,2),trim(ffsl_date_yy),'-',trim(ffsl_date_mm),'-',trim(ffsl_date_dd))) <= date_sub(""" + runDay + """,int(1)) 
 and as_of_date >= date_sub(""" + runDay + """,int(31))""").dropDuplicates().createOrReplaceTempView("falconDF")
 readFalconDF = spark.sql("""select 
                                    ffsl_account_number,
                                    ffsl_tran_amount,
                                    ffsl_strategy,
                                    ffsl_mrch_ctry_cd,
                                    ffsl_fraud_trancode,
                                    to_date(concat(substr(current_date(),1,2),ffsl_date_yy,'-',ffsl_date_mm,'-',ffsl_date_dd)) as tran_date
                                    from falconDF""")
 readFalconDF.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return readFalconDF

def createAggDF(readFalconDF,readCustAccNoDF,spark):
 ################################################################################################################
 # Create data frame for C360 attribute aggregation                                                             #
 ################################################################################################################
 readFalconDF.createOrReplaceTempView("readFalconDF")
 readCustAccNoDF.createOrReplaceTempView("readCustAccNo")
 aggregateDF = spark.sql("""select readFalconDF.*,
                                   readCustAccNo.* 
                                   from  readFalconDF inner join readCustAccNo
                                   on ffsl_account_number = customer_acct_nbr
                                   """)
 aggregateDF = aggregateDF.select("lexid",
                                   "current_account_nbr",
                                   "orig_account_nbr",
                                   col('ffsl_account_number').alias("account_number"),
                                   col('ffsl_tran_amount').alias("tran_amount"),
                                   "ffsl_strategy",
                                   "ffsl_mrch_ctry_cd",
                                   "ffsl_fraud_trancode",
                                   "tran_date"
                                   )                                  
 aggregateDF.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return aggregateDF

####################################################################################################################################
## Calculating C360 attributes
def calculateC360Atributes(aggregateDF,runDay,spark):
 lkpAccNoDF = aggregateDF.select(col('lexid'),col('account_number'),col('current_account_nbr'),col('orig_account_nbr')).dropDuplicates()
 #################################################################################################################################################
 # Performing the aggregations on data frames and calculating the C360 attributes                                                                #
 # Calculating 5 c360 defense edge attributes for tran_date between runDay-1 to runDay-30; attrOverLast30days                                    #
 # Calculating 2 c360 defense edge attributes for tran_date between runDay-2 to runDay-30; attrOverLast31Days                                    #
 #################################################################################################################################################
 attrOverLast30days = aggregateDF.filter(expr("tran_date >= date_sub(" + runDay + ",int(30)) and tran_date <= date_sub(" + runDay + ",int(1))"))\
                                 .groupBy('lexid')\
                                 .agg(_mean(col('tran_amount')).alias('tran_amt_mean_last_30_days'),\
                                      _stddev(col('tran_amount')).alias('tran_amt_std_last_30_days'),\
                                       count('tran_amount').alias('tran_cnt_total_last_30_days'),\
                                       sum(expr("case when tran_date = date_sub(" + runDay + ",int(1)) then 1 else 0 end")).alias("tran_cnt_prev_day"),\
                                       max(expr("case when trim(ffsl_mrch_ctry_cd) not in ('840','124','316','581','583','584','585','630','850','882') and \
                                       tran_date = date_sub(" + runDay + ",int(1)) and trim(ffsl_strategy) <> '55' then 'Y' else 'N' end")).alias("foreign_tran_prev_day"))
 attrOverLast31Days = aggregateDF.filter(expr("tran_date >= date_sub(" + runDay + ",int(31)) and tran_date <= date_sub(" + runDay + ",int(2))"))\
                      .groupBy("lexid","tran_date")\
                      .agg(count('tran_amount').alias('count'))\
                      .groupBy('lexid')\
                      .agg(_mean(col('count')).alias('tran_cnt_per_day_mean_last_30_days'),\
                           _stddev(col('count')).alias('tran_cnt_per_day_std_last_30_days'))
 #####################################################################################################################################################################
 # joining the two different dataframes of 30 days date windows with full outer join as these two dataframes are having records from 2 different 30 days date ranges 
 # attrOverLast30days : day-1 through day-30 ; attrOverLast31Days: Day-2 through day-31
 attrCombinedDF = attrOverLast30days.join(attrOverLast31Days,['lexid'],'full')
 # joining lkpAccNoDF with attrCombinedDF as left join in order to fetch the account_number,lexid along with c360 attributes
 c360DefenceAttrDF = lkpAccNoDF.join(attrCombinedDF,['lexid'],'left_outer')
 #####################################################################################################################################################################
 # Setting the default 'N' for those lexid and account number whose txns do not fall on previous day
 c360DefenceAttrDF.dropDuplicates().replace(float('nan'),None).na.fill("N",["foreign_tran_prev_day"]).createOrReplaceTempView("c360DefenceAttrDF")
 #####################################################################################################################################################################
 ##Forming final c360 defense edge attributes data frame for hive table load 
 finalC360AttributeDF = spark.sql("""select cast(lexid as varchar(30)) as lexid,
                             cast(current_account_nbr as varchar(16)) as current_account_nbr,
                             cast(orig_account_nbr as varchar(16)) as orig_account_nbr,
                             cast(account_number as varchar(16)) as account_number,
                             cast(round(nvl(tran_amt_mean_last_30_days,0),2) as Decimal(10,2)) as tran_amt_mean_last_30_days,
                             cast(round(nvl(tran_amt_std_last_30_days,0),2) as Decimal(10,2)) as tran_amt_std_last_30_days,
                             cast(round(nvl(tran_cnt_per_day_mean_last_30_days,0),2) as Decimal(10,2)) as tran_cnt_per_day_mean_last_30_days,
                             cast(round(nvl(tran_cnt_per_day_std_last_30_days,0),2) as Decimal(10,2)) as tran_cnt_per_day_std_last_30_days,
                             cast(nvl(tran_cnt_total_last_30_days,0) as Decimal(10,0)) as tran_cnt_total_last_30_days,
                             cast(nvl(tran_cnt_prev_day,0) as Decimal(10,0)) as tran_cnt_prev_day,
                             cast(foreign_tran_prev_day as char(1)) as foreign_tran_prev_day,
                             current_timestamp() as edl_load_ts,
                             current_date() as as_of_date
                             from c360DefenceAttrDF""").dropDuplicates()
 finalC360AttributeDF.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
 return finalC360AttributeDF

#######################################################################################################################
# Function for loading the C360 attributes hive table                                                                 #
#######################################################################################################################
def loadC360AttributeHive(finalC360AttributeDF,edl_amf_c360_hive_ldr,writeMode,spark):
 print("######################## Starting loading c360 attribute hive table ######################################")              
 spark.sql("set hive.exec.dynamic.partition=true")
 spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 finalC360AttributeDF.write.mode(writeMode).insertInto(edl_amf_c360_hive_ldr,overwrite=True)
 print("Total record loaded into hive table is: "+ str(finalC360AttributeDF.count()))
 print("######################## Finished loading c360 attribute hive table ###########################################")

########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 hive = readHiveContext(spark)
 registerJavaFunction(sqlContext)
 configFileName = validateArg()
 inputParams = validateConfigFile(configFileName)
 accXrefSchema = createSchema(inputParams['accXrefColumnNames'])
 accDimDF = readAccDim(hive)
 readLexIdDF = readLexID(inputParams['lexidSyfacc'],spark)
 readAccXrefDF = readAccXref(inputParams['syf_acct_token_xref'],accXrefSchema,inputParams['readMode'],spark)
 readCustAccNoDF = readCustAcc(readLexIdDF,readAccXrefDF,accDimDF,spark)
 readFalconDF = readFalconTable(inputParams['edl_amf_falcon_raw'],inputParams['runDay'],spark)
 aggregateDF = createAggDF(readFalconDF,readCustAccNoDF,spark)
 finalC360AttributeDF = calculateC360Atributes(aggregateDF,inputParams['runDay'],spark)
 loadC360AttributeHive(finalC360AttributeDF,inputParams['edl_amf_c360_hive_ldr'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()