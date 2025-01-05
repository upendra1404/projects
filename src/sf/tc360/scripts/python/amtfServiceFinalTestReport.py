###############################################################################
# Script Name:amtfServiceFinalTestReport.py                                   #
# Purpose: To collect test statistics and send out test report                #
# Created by: Upendra Kumar                                                   #
# Create Date: 11/18/2021                                                     #
# Modify date:                                                                #
###############################################################################
import logging, os, sys
import ConfigParser
from ConfigParser import ConfigParser
import commands
import subprocess, re
from string import strip
from datetime import date, timedelta
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import when,explode, regexp_replace, datediff, sum as _sum,count,collect_set,dense_rank,rank,coalesce,broadcast,countDistinct,isnull,round,struct,lag,lead,current_date,current_timestamp,date_format,date_sub,datediff,trunc,upper,substring,split,lit,udf,max,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import getopt

############################################ Function definition section #####################################################################
## Context Build and Variable Setting
def validateArg():
    printError = 'spark-submit script_name.py -f <config_file_path>/<config_file_name> -r q_tab_arr -u d_tab_arr'
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f:r:u:x:')
    except getopt.error as msg:
        print('Something went wrong!')
        print('Example for entering argument to the script is:' + msg)
        sys.exit(printError)

    for opt, arg in opts:
        if(opt == '-f'):
            configFileName = arg
        elif(opt == '-r'):
            q_tab_arr = arg
        elif(opt == '-u'):
            d_tab_arr = arg
        elif(opt == '-x'):
            is_hdfs_path_read = arg
    if(configFileName is None or configFileName == ''):
        print(printError)
        sys.exit('ERROR: Config File Name not provided with argument -f')
    elif(q_tab_arr is None or q_tab_arr == ''):
        print(printError)
        sys.exit('ERROR: q_tab_arr not provided with argument -r')
    elif(d_tab_arr is None or d_tab_arr == ''):
        print(printError)
        sys.exit('ERROR: d_tab_arr not provided with argument -u')
    elif(is_hdfs_path_read is None or is_hdfs_path_read == ''):
        print(printError)
        sys.exit('ERROR: is_hdfs_path_read not provided with argument -x')
    
    argsList = {
                  'q_tab_arr': q_tab_arr
                 ,'d_tab_arr': d_tab_arr
                 ,'is_hdfs_path_read': is_hdfs_path_read
               }
    return configFileName, argsList

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
                if ('extGenValPath' not in config.options('HDFSPATH')):
                    sys.exit('ERROR: Section HDFSPATH does not contain extGenValPath')
                else:
                    extGenValPath = config.get('HDFSPATH', 'extGenValPath')
                if ('from_email' not in config.options('FUNCTIONAL')):
                    sys.exit('ERROR: Section FUNCTIONAL does not contain from_email')
                else:
                    from_email = config.get('FUNCTIONAL', 'from_email')
                if ('to_email' not in config.options('FUNCTIONAL')):
                    sys.exit('ERROR: Section FUNCTIONAL does not contain to_email')
                else:
                    to_email = config.get('FUNCTIONAL', 'to_email')
                if ('email_script_with_path' not in config.options('FUNCTIONAL')):
                    sys.exit('ERROR: Section FUNCTIONAL does not contain email_script_with_path')
                else:
                    email_script_with_path = config.get('FUNCTIONAL', 'email_script_with_path')
                
        return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'extGenValPath': extGenValPath
                            ,'from_email': from_email
                            ,'to_email': to_email
                            ,'email_script_with_path': email_script_with_path
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('email_test_outcome_report').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold','200000000')
 spark.conf.set('spark.sql.broadcastTimeout','-1')
 spark.sql('SET spark.hadoop.hive.mapred.supports.subdirectories=true')
 spark.sql('SET mapreduce.input.fileinputformat.input.dir.recursive=true')
 status,protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
 spark.sql('add jar ' + protegrityFile)
 spark.sql('add jar /opt/protegrity/pepspark/lib/pepspark-2.3.2.jar')
 spark.sql('add jar /usr/share/java/mysql-connector-java-5.1.17.jar')
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'")
 spark.sql('add jar /data1/EDL/domains/cdl/synapps/edlservices-crypto-sparkudf-extensions/crypto-sparkudf-extensions-jar-with-dependencies.jar')
 return spark

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

def createSQLContext(spark):
 sc = spark.sparkContext
 sqlContext = SQLContext(sc)
 return sqlContext

def init_time_var():
 run_day = str(date.today())
 return run_day
 
def init():
  html_report_list = []
  html_report_list.append("<!DOCTYPE html>")
  html_report_list.append("<head>")
  html_report_list.append("<style>")
  html_report_list.append("table,tr,td,th{")
  html_report_list.append("border: 1px solid black;")
  html_report_list.append("border-collapse: collapse;")
  html_report_list.append("}")
  html_report_list.append("</style>")
  html_report_list.append("</head>")
  html_report_list.append("<body>")
  html_report_list.append("<h3> Enterprise Data Lake - Test validation summary report</h3>")
  html_report_list.append('<table style="width:100%">')
  html_report_list.append("<tr>")
  html_report_list.append("<th bgcolor=Yellow> Table type </th>")
  html_report_list.append("<th bgcolor=Yellow> QA Table</th>")
  html_report_list.append("<th bgcolor=Yellow> Dev Table</th>")
  html_report_list.append("<th bgcolor=Yellow> Schema discrepancies</th>")
  html_report_list.append("<th bgcolor=Yellow> QA count</th>")
  html_report_list.append("<th bgcolor=Yellow> Dev count</th>")  
  html_report_list.append("<th bgcolor=Yellow> Duplicates</th>")
  html_report_list.append("<th bgcolor=Yellow> Data discrepancies</th>")
  html_report_list.append("<th bgcolor=Yellow> Run day </th>")
  html_report_list.append("<th bgcolor=Yellow> Timestamp </th>")
  html_report_list.append("</tr>")
  return html_report_list

def init_statistics(df):
   #Type casting statistics fields into integers
    schema_diff_count = int(df.agg({'schema_diff_count':'max'}).collect()[0][0])
    qa_count = int(df.agg({'qa_count':'max'}).collect()[0][0])
    dev_count = int(df.agg({'dev_count':'max'}).collect()[0][0])
    duplicate_count = int(df.agg({'duplicate_count':'max'}).collect()[0][0])
    disc_count = int(df.agg({'disc_count':'max'}).collect()[0][0])
    edl_load_ts = df.agg({'edl_load_ts':'max'}).collect()[0][0] 
    return schema_diff_count,qa_count,dev_count,duplicate_count,disc_count,edl_load_ts
 
def formulate_table(html_report_list,is_hdfs_path_read,qTable,dTable,schema_diff_count,qa_count,dev_count,duplicate_count,disc_count,edl_load_ts,run_day):
 try:
     if abs(qa_count-dev_count) > 0: is_count_mismatch = True
     else: is_count_mismatch = False
     html_report_list.append("<tr>")
     if int(is_hdfs_path_read) == 1: html_report_list.append("<td> HDFS </td>")
     else: html_report_list.append("<td> Hive </td>")
     html_report_list.append("<td>" + str(qTable) + "</td>")
     html_report_list.append("<td>" + str(dTable) + "</td>")
     if schema_diff_count > 0: html_report_list.append("<td bgcolor = Red>" + str(schema_diff_count) + "</td>")
     else: html_report_list.append("<td>" + str(schema_diff_count) + "</td>")
     if is_count_mismatch: html_report_list.append("<td bgcolor = Red>" + str(qa_count) + "</td>")
     else: html_report_list.append("<td>" + str(qa_count) + "</td>")
     if is_count_mismatch: html_report_list.append("<td bgcolor = Red>" + str(dev_count) + "</td>")
     else: html_report_list.append("<td>" + str(dev_count) + "</td>")
     if duplicate_count > 0: html_report_list.append("<td bgcolor = Red>" + str(duplicate_count) + "</td>")
     else: html_report_list.append("<td>" + str(duplicate_count) + "</td>")
     if disc_count > 0: html_report_list.append("<td bgcolor = Red>" + str(disc_count) + "</td>")
     else: html_report_list.append("<td>" + str(disc_count) + "</td>")
     html_report_list.append("<td>" + str(run_day) + "</td>")
     html_report_list.append("<td>" + str(edl_load_ts) + "</td>")
     html_report_list.append("</tr>")
     return html_report_list
 except Exception as e:
   sys.exit("ERROR: while forming the html table - " + str(e))

def collectStatistics(html_report_list,q_tab_arr,d_tab_arr,extGenValPath,is_hdfs_path_read,run_day,spark):
 try:
  qArr = [str(x) for x in q_tab_arr.split(",")]
  dArr = [str(x) for x in d_tab_arr.split(",")]
  for i in range(0,len(qArr)):
    if int(is_hdfs_path_read) == 0: extTSPath = extGenValPath + "/statistics/" + [qArr[i].split(".",1)][0][1]
    else: extTSPath = extGenValPath + "/statistics/" + qArr[i]
    dsDf = spark.read.parquet(extTSPath) # reading data from validation hdfs path
    schema_diff_count,qa_count,dev_count,duplicate_count,disc_count,edl_load_ts = init_statistics(dsDf) # Calling init_statistics function to type cast the test statistics variables
    html_report_list = formulate_table(html_report_list,is_hdfs_path_read,qArr[i],dArr[i],schema_diff_count,qa_count,dev_count,duplicate_count,disc_count,edl_load_ts,run_day)
  return html_report_list
 except Exception as e:
  sys.exit("ERROR: while collecting the test statistics: " + str(e))

def create_test_summary(q_tab_arr,d_tab_arr,extGenValPath,is_hdfs_path_read,run_day,sqlContext,spark):
 try:
  schema_cols = "qa_table,dev_table,disc_count,schema_diff_count,qa_count,dev_count,duplicate_count"
  schema = createSchema(schema_cols)
  summaryDf = sqlContext.createDataFrame(spark.sparkContext.emptyRDD(),schema) # creating a empty data frame
  qArr = [str(x) for x in q_tab_arr.split(",")]
  dArr = [str(x) for x in d_tab_arr.split(",")]
  for i in range(0,len(qArr)):
     if int(is_hdfs_path_read) == 0: extTSPath = extGenValPath + "/statistics/" + [qArr[i].split(".",1)][0][1]
     else: extTSPath = extGenValPath + "/statistics/" + qArr[i]
     df = spark.read.parquet(extTSPath).withColumn('qa_table',lit(qArr[i])).withColumn('dev_table',lit(dArr[i])).select(col('qa_table'),col('dev_table'),col('disc_count'),col('schema_diff_count'),col('qa_count'),col('dev_count'),col('duplicate_count'))
     summaryDf = summaryDf.union(df)
  summaryDf = summaryDf.withColumn('run_day',lit(run_day)).withColumn('edl_load_ts',lit(current_timestamp()))
  return summaryDf
 except Exception as e:
  sys.exit("ERROR: while creating test summary - " + str(e))

def load_test_summary(summaryDf,extGenValPath,writeMode,spark):
  try:
   spark.sql('set hive.exec.dynamic.partition=true')
   spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
   extDDPath = extGenValPath + "/summary" 
   summaryDf.repartition(1).write.mode(writeMode).option('header','true').format('parquet').save(extDDPath)
  except Exception as e:
   sys.exit("ERROR: while loading the test summary into hive - " + str(e))

def send_email(html_report_list,email_script_with_path,from_email,to_email,run_day):
    subject = 'Final test outcome report for ' + str(run_day)
    html_report = ''.join(html_report_list)
    logging.info("INFO:: ##### html test outcome report sent by email ######\n" + html_report)

    ## Calling bash script kafka_maint_rpt_sent_email.sh for sending email
    subprocess.call([email_script_with_path, from_email, to_email, subject, html_report])
  
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 sqlContext = createSQLContext(spark)
 spark.sparkContext.setLogLevel('ERROR')
 configFileName, argsList = validateArg()
 inputParams = validateConfigFile(configFileName)
 run_day = init_time_var()
 html_report_list =  init() # initialising the html report list 
 html_report_list = collectStatistics(html_report_list,argsList['q_tab_arr'],argsList['d_tab_arr'],inputParams['extGenValPath'],argsList['is_hdfs_path_read'],run_day,spark)
 summaryDf = create_test_summary(argsList['q_tab_arr'],argsList['d_tab_arr'],inputParams['extGenValPath'],argsList['is_hdfs_path_read'],run_day,sqlContext,spark)
 load_test_summary(summaryDf,inputParams['extGenValPath'],inputParams['writeMode'],spark) # loading test summary into hive
 send_email(html_report_list,inputParams['email_script_with_path'],inputParams['from_email'],inputParams['to_email'],run_day) # sending email test report
 
if __name__ == '__main__':
    main()