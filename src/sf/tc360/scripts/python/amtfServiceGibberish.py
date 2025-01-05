###############################################################################
# Script Name:amfServiceGibberishLogic.py                                     #
# Purpose: Gibberish logic implementation                                     #
# Created by: Upendra Kumar                                                   #
# Create Date: 09/07/2021                                                     #
# Modify date:                                                                #
###############################################################################

from pyspark.sql.types import FloatType,IntegerType,ArrayType,StringType,DataType,StructField,StructType,TimestampType,DateType,LongType,DecimalType
from pyspark.sql import SparkSession,SQLContext,HiveContext,Window,Row
from pyspark.sql.functions import sum as _sum,mean as _mean, stddev_pop as _stddev,col,coalesce,lit,split,trim,size,lpad,length,to_date,concat,substring,current_date,expr,datediff,udf,array,desc,date_sub,count,collect_list,max,min,to_timestamp,row_number,rank,collect_set,explode,round,current_timestamp,date_format,broadcast,countDistinct,regexp_replace
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
from math import sqrt,exp,log
from ast import literal_eval
import moduleReadFalcon as moduleReadFalcon

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
                if ('inputFileDemiliter' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section HIVE doesn't contain inputFileDemiliter")
                else:
                    inputFileDemiliter = config.get('SCHEMA', 'inputFileDemiliter')
                if ('runDay' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain runDay")
                else:
                    runDay = config.get('HIVE', 'runDay')
                if ('falconRawPath' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain falconRawPath")
                else:
                    falconRawPath = config.get('HIVE', 'falconRawPath')
                if ('extGibberishPath' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HIVE doesn't contain extGibberishPath")
                else:
                    extGibberishPath = config.get('HDFSPATH','extGibberishPath')
                if ('gibberish_corpus' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain gibberish_corpus")
                else:
                    gibberish_corpus = config.get('HDFSPATH','gibberish_corpus')
                if ('gibberish_threshold' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain gibberish_threshold")
                else:
                    gibberish_threshold = config.get('HDFSPATH','gibberish_threshold')
                if ('edl_amf_falcon_raw' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain edl_amf_falcon_raw")
                else:
                    edl_amf_falcon_raw = config.get('HIVE','edl_amf_falcon_raw')
                if ('debug' not in config.options('FUNCTIONAL')):
                  sys.exit("ERROR: Section FUNCTIONAL doesn't contain debug")
                else:
                    debug = config.get('FUNCTIONAL','debug')
                
        return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'falconRawPath': falconRawPath
                            ,'extGibberishPath': extGibberishPath
                            ,'runDay': runDay
                            ,'gibberish_corpus': gibberish_corpus
                            ,'gibberish_threshold': gibberish_threshold
                            ,'edl_amf_falcon_raw': edl_amf_falcon_raw
                            ,'debug': debug
                }

def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('gibberish_score_app').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold','200000000')
 spark.conf.set('spark.sql.broadcastTimeout','3600000')
 spark.sql("SET spark.hadoop.hive.mapred.supports.subdirectories=true")
 spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
 status,protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
 spark.sql("add jar " + protegrityFile)
 spark.sql("add jar /opt/protegrity/pepspark/lib/pepspark-2.3.2.jar")
 spark.sql("add jar /usr/share/java/mysql-connector-java-5.1.17.jar")
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'") 
 return spark

def int_gibberish():
 en_chars = 'abcdefghijklmnopqrstuvwxyz '
 k = len(en_chars)
 char_to_idx = {c: idx for idx,c in enumerate(en_chars)}
 return char_to_idx,en_chars,k

def clean_and_tokenize(text,en_chars):
 return [char.lower() for char in text if char.lower() in en_chars]

def nGram(n,text,en_chars):
 tokenized_txt = clean_and_tokenize(text,en_chars)
 for idx in range(len(tokenized_txt)-n+1):
   yield tokenized_txt[idx:idx+n]
   
def compute_prob_matrix(gibberish_corpus,n,k,en_chars,char_to_idx,spark):   
 prob_matrix = [[10.]*k for i in range(k)]
 df0 = spark.read.csv(gibberish_corpus,header = True)
 corpusList = [i[0] for i in df0.select('Sentence').collect()] # Creating python list from pyspark data frame
 for line in corpusList:
   for fc,sc in nGram(n,line,en_chars):
    prob_matrix[char_to_idx[fc]][char_to_idx[sc]] += 1
 for i, row in enumerate(prob_matrix):
   s = float(sum(row))
   for j in range(len(row)):
     row[j] = log(row[j] / s) # computing probability matrix with log scale
 return prob_matrix

def avg_transition_probability(log_prob_mat,text,char_to_idx,en_chars,n):
  log_prob = 0.0
  transition_ct = 0
  char_to_idx = literal_eval(char_to_idx)
  log_prob_mat = literal_eval(log_prob_mat)
  if text is not None:
   for fc,sc in nGram(n,text,en_chars):
    log_prob += log_prob_mat[char_to_idx[fc]][char_to_idx[sc]]
    transition_ct += 1
  return exp(log_prob / (transition_ct or 1))

def readFalcon(edl_amf_falcon_raw,falconRawPath,debug,runDay,spark):
 try:
  if debug == 'no':      
       falconDf = spark.sql("""select * from """ + edl_amf_falcon_raw).where((col('as_of_date') < lit(runDay)) & (col('as_of_date') >= date_sub(lit(runDay),int(90))))
  else:
       falconDf = spark.read.orc(falconRawPath).where((col('as_of_date') < lit(runDay)) & (col('as_of_date') >= date_sub(lit(runDay),int(90))))
  falconDf = falconDf.select(trim(col('ffsl_account_number')).alias('ffsl_account_number'),trim(col('ffsl_mrch_nm')).alias('ffsl_mrch_nm')).dropDuplicates()
  falconDf.cache()
  return falconDf
 except Exception as e:
  sys.exit("ERROR: while reading falcon table - " + str(e))
  
def computeGibberishScore(falconDf,log_prob_mat,gibberish_threshold,char_to_idx,en_chars,n,runDay):
 try:
  avg_transition_probability_udf = udf(avg_transition_probability,FloatType())
  mrchGibberishDf = falconDf.select(col('ffsl_account_number')
                                  ,expr("ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL')").alias('ffsl_mrch_nm'))\
                           .repartition(1000)\
                           .withColumn('non_gibberish_score',avg_transition_probability_udf(lit(str(log_prob_mat)),col('ffsl_mrch_nm'),lit(str(char_to_idx)),lit(en_chars),lit(n)))\
                           .withColumn('gibberish_score',1/(0.001+col('non_gibberish_score')))\
                           .withColumn('gibberish_flag',col('gibberish_score')>lit(int(gibberish_threshold)))
  mrchGibberishDf = mrchGibberishDf.withColumn('ffsl_mrch_nm',expr("ptyProtectStr(ffsl_mrch_nm,'dtStrNPL')"))
  mrchGibberishDf.cache()
  return mrchGibberishDf
 except Exception as e:
  sys.exit("ERROR: while computing gibberish score for merchants - " + str(e))
 
def loadGibberishScores(mrchGibberishDf,extGibberishPath,writeMode,spark):
 try:
  print("######################## Starting loading Gibberish scores hive table ######################################")              
  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  mrchGibberishDf.write.mode(writeMode).option('header','true').format('parquet').save(extGibberishPath)
  print("######################## Gibberish scores hive table load complete ###########################################")
 except Exception as e:
  sys.exit("ERROR: while loading the data into hive table - " + str(e))
 
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 configFileName,runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 char_to_idx,en_chars,k = int_gibberish() # initialising gibirish parameters
 log_prob_mat = compute_prob_matrix(inputParams['gibberish_corpus'],2,k,en_chars,char_to_idx,spark)
 falconDf = readFalcon(inputParams['edl_amf_falcon_raw'],inputParams['falconRawPath'],inputParams['debug'],runDay,spark)
 mrchGibberishDf = computeGibberishScore(falconDf,log_prob_mat,inputParams['gibberish_threshold'],char_to_idx,en_chars,2,runDay)
 loadGibberishScores(mrchGibberishDf,inputParams['extGibberishPath'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()