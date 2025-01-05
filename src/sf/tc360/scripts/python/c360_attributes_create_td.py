###############################################################################
# Script Name:amf_c360_attributes_create_td.py                                #
# Purpose: To load test data to c360 attr hive table                          #
# Created by: Upendra Kumar                                                   #
# Create Date: 17/11/2020                                                     #
###############################################################################

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *
from ConfigParser import ConfigParser
import random
import commands
import getopt
import sys
import os


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
    print("Validating Config File")
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
                if ('edl_amf_c360_hive_ldr' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain edl_amf_c360_hive_ldr")
                else:
                    edl_amf_c360_hive_ldr = config.get('HIVE','edl_amf_c360_hive_ldr')
                if ('inputFile' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section SCHEMA doesn't contain inputFile")
                else:
                    inputFile = config.get('SCHEMA','inputFile')
                if ('c360_col_names' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section SCHEMA doesn't contain c360_col_names")
                else:
                    columnNames = config.get('SCHEMA','c360_col_names')
                if ('sparkJar' not in config.options('FUNCTIONAL')):
                    sys.exit("ERROR: Section SCHEMA doesn't contain sparkJar")
                else:
                    sparkJar = config.get('FUNCTIONAL','sparkJar')
                if ('c360_attr_localInputFiles' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain c360_attr_localInputFiles")
                else:
                   c360_attr_localInputFiles = config.get('HDFSPATH','c360_attr_localInputFiles')
                if ('c360_attr_inputFiles' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain c360_attr_inputFiles")
                else:
                   c360_attr_inputFiles = config.get('HDFSPATH','c360_attr_inputFiles')
                if ('inputFileDemiliter' not in config.options('SCHEMA')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain inputFileDemiliter")
                else:
                 inputFileDemiliter = config.get('SCHEMA','inputFileDemiliter')
                if ('writeModeTokenFile' not in config.options('FUNCTIONAL')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain writeModeTokenFile")
                else:
                  writeModeTokenFile = config.get('FUNCTIONAL','writeModeTokenFile')
                  writeOptions = ['overwrite','append','ignore','error']
                  if (writeModeTokenFile in writeOptions):
                       writeMode = writeModeTokenFile
                  else:
                       sys.exit("ERROR: config write mode is invalid. Please chack back the write mode in the config file")
                print("Config File Successfully Validated")
        print("##################################################################################")
        return {
                    'readMode': readMode,
                    'writeMode': writeMode,
                    'edl_amf_c360_hive_ldr': edl_amf_c360_hive_ldr,
                    'inputFile': inputFile,
                    'columnNames': columnNames,
                    'sparkJar': sparkJar,
                    'c360_attr_localInputFiles': c360_attr_localInputFiles,
                    'c360_attr_inputFiles': c360_attr_inputFiles,
                    'inputFileDemiliter': inputFileDemiliter
                }
## Create input file schema
def createSchema(columnNames):
 print("###################################################################################################")
 print("Creating file schema: Starts")
 if (not columnNames):
        sys.exit("The Column Names string is blank. Please provide valid column names")
 else:
        columnStructFields = []
        for column in columnNames.split(","):
             columnStructFields.append(StructField(column, StringType(), True))
 schema = StructType(columnStructFields)
 print("Creating file schema: Complete")
 return schema
 
    
### Create spark session
def createSparkSession(sparkJar):
 print("###################################################################################################")
 print("Creating spark session: Starts")
 spark = SparkSession.builder.\
                enableHiveSupport().appName('onetimedataloadPSCC').getOrCreate()
 spark.sql("SET hive.mapred.supports.subdirectories=true")
 spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
 spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
 status,protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
 spark.sql("add jar " + protegrityFile)
 spark.sql("add jar " + sparkJar)
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'")
 print("Creating spark session: Complete")
 return spark
 

def checkFilePresence(localFilePath,hdfsFilePath,inputFile,spark):
 print("###################################################################################################")
 print("Validating csv files at local server location: Starts")
 fileStatus = os.system("ls " + localFilePath + '/' + inputFile)
 if(fileStatus != 0):
     sys.exit("File does exist at local path: " + localFilePath)
 else:
     existFileAtHdfs = os.system("hdfs dfs -ls " + hdfsFilePath + "*.csv")
     if(existFileAtHdfs != 0):
          mvStatus = os.system("hdfs dfs -put -f " + localFilePath + '/' + inputFile + " "  + hdfsFilePath)
     else:
          os.system("hdfs dfs -rm -r " + hdfsFilePath + "*")
          mvStatus = os.system("hdfs dfs -put -f " + localFilePath + '/' + inputFile + " "  + hdfsFilePath)
     if(mvStatus != 0):
              sys.exit("Something went wrong!!. Please check log for more detail errors")
     else:
              print("File copied successfully")
 print("Validating csv files at local server location: Complete")
   
### Reading csv file at local path
def readCSVFile(hdfsFilePath,inputFileDemiliter,readMode,schema,spark):
 print("###################################################################################################")
 print("Reading csv files: Starts")
 spark.read.option('delimiter',inputFileDemiliter).csv(hdfsFilePath+"*.csv",header=True,inferSchema=True,schema=schema,mode=readMode).createOrReplaceTempView("readCSV_vw")
 readCSV = spark.sql("""select lexid,
                             current_account_nbr,
                             orig_account_nbr,                             
                             ptyProtectStr(cast(account_number as string),'dtStrNPL') as account_number,
                             tran_amt_mean_last_30_days,
                             tran_amt_std_last_30_days,
                             tran_cnt_per_day_mean_last_30_days,
                             tran_cnt_per_day_std_last_30_days,
                             tran_cnt_total_last_30_days,
                             tran_cnt_prev_day,
                             foreign_tran_prev_day
                             from readCSV_vw""").withColumn('edl_load_ts',lit(current_timestamp()))\
                                                .withColumn('as_of_date',lit(current_date()))
 print("Records read from csv files are: " + str(readCSV.count()))
 print("Reading csv files: Complete")
 return readCSV
## Loading data into c360 hive table
def loadIntoTable(readCSV,edl_amf_c360_hive_ldr,writeMode,spark):
 print("###################################################################################################")
 print("Loading data into c360 hive table: Starts")              
 spark.sql("set hive.exec.dynamic.partition=true")
 spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 readCSV.write.mode(writeMode).insertInto(edl_amf_c360_hive_ldr, overwrite=True)
 print("Total record loaded into c360 hive table is: "+ str(readCSV.count()))
 print("Loading data into c360 hive table: Complete")
 print("###################################################################################################")

##Defining main funtion for pyspark script
def main():
 configFileName = validateArg()
 inputParams = validateConfigFile(configFileName)
 spark = createSparkSession(inputParams['sparkJar'])
 schema = createSchema(inputParams['columnNames'])
 checkFilePresence(inputParams['c360_attr_localInputFiles'],inputParams['c360_attr_inputFiles'],inputParams['inputFile'],spark)
 readCSV = readCSVFile(inputParams['c360_attr_inputFiles'],inputParams['inputFileDemiliter'],inputParams['readMode'],schema,spark)
 loadIntoTable(readCSV,inputParams['edl_amf_c360_hive_ldr'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()