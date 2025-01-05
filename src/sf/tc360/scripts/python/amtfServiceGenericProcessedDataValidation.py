###############################################################################
# Script Name:amtfServiceGenericProcessedDataValidation.py                    #
# Purpose: To perform generic data validation                                 #
# Created by: Upendra Kumar                                                   #
# Create Date: 11/07/2021                                                     #
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
        opts, args = getopt.getopt(sys.argv[1:], 'f:r:n:s:u:w:x:o:p:q:c:')
    except getopt.error as msg:
        print('Something went wrong!')
        print('Example for entering argument to the script is:')
        sys.exit(printError)

    for opt, arg in opts:
        if(opt == '-f'):
            configFileName = arg
        elif(opt == '-r'):
            runDay = arg
        elif(opt == '-n'):
            n = arg
        elif(opt == '-s'):
            qTab = arg
        elif(opt == '-u'):
            dTab = arg
        elif(opt == '-w'):
            cs = arg
        elif(opt == '-x'):
            isPartitioned = arg
        elif(opt == '-o'):
            hdfs_processed_qa = arg
        elif(opt == '-p'):
            hdfs_processed_dev = arg
        elif(opt == '-q'):
            is_hdfs_path_read = arg
        elif(opt == '-c'):
            is_ets_control = arg
    if(configFileName is None or configFileName == ''):
        print(printError)
        sys.exit('ERROR: Config File Name not provided with argument -f')
    elif(runDay is None or runDay == ''):
        print(printError)
        sys.exit('ERROR: runDay not provided with argument -r')
    elif(n is None or n == ''):
        print(printError)
        sys.exit('ERROR: n not provided with argument -n')
    elif(qTab is None or qTab == ''):
        print(printError)
        sys.exit('ERROR: qTab not provided with argument -s')
    elif(dTab is None or dTab == ''):
        print(printError)
        sys.exit('ERROR: dTab not provided with argument -u')
    elif(cs is None or cs == ''):
        print(printError)
        sys.exit('ERROR: cs not provided with argument -w')
    elif(isPartitioned is None or isPartitioned == ''):
        print(printError)
        sys.exit('ERROR: isPartitioned not provided with argument -x')
    elif(hdfs_processed_qa is None or hdfs_processed_qa == ''):
        print(printError)
        sys.exit('ERROR: hdfs_processed_qa not provided with argument -o')
    elif(hdfs_processed_dev is None or hdfs_processed_dev == ''):
        print(printError)
        sys.exit('ERROR: hdfs_processed_dev not provided with argument -p')
    elif(is_hdfs_path_read is None or is_hdfs_path_read == ''):
        print(printError)
        sys.exit('ERROR: is_hdfs_path_read not provided with argument -q')
    elif(is_ets_control is None or is_ets_control == ''):
        print(printError)
        sys.exit('ERROR: is_ets_control not provided with argument -c')
    
    argsList = {
                  'runDay': runDay
                 ,'n': n
                 ,'qTab': qTab
                 ,'dTab': dTab
                 ,'cs': cs
                 ,'isPartitioned': isPartitioned
                 ,'hdfs_processed_qa': hdfs_processed_qa
                 ,'hdfs_processed_dev': hdfs_processed_dev
                 ,'is_hdfs_path_read': is_hdfs_path_read
                 ,'is_ets_control': is_ets_control
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
                if ('cols_schema' not in config.options('SCHEMA')):
                    sys.exit('ERROR: Section SCHEMA does not contain cols_schema')
                else:
                    cols_schema = config.get('SCHEMA', 'cols_schema')
                if ('schemaPathQA' not in config.options('HDFSPATH')):
                    sys.exit('ERROR: Section HDFSPATH does not contain schemaPathQA')
                else:
                    schemaPathQA = config.get('HDFSPATH', 'schemaPathQA')
                if ('schemaPathDev' not in config.options('HDFSPATH')):
                    sys.exit('ERROR: Section HDFSPATH does not contain schemaPathDev')
                else:
                    schemaPathDev = config.get('HDFSPATH', 'schemaPathDev')
                
        return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'extGenValPath': extGenValPath
                            ,'cols_schema': cols_schema
                            ,'schemaPathQA': schemaPathQA
                            ,'schemaPathDev': schemaPathDev
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('generic_processed_data_comparison_engine').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold','200000000')
 spark.conf.set('spark.sql.broadcastTimeout','-1')
 spark.conf.set('spark.driver.maxResultSize','0')
 spark.sparkContext.addFile('/data1/EDL/domains/amf_qa/edl_amf_qa/python/amtf_reusable_functions.py')
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

def readQASchema(schemaPathQA,qTab,hdfs_processed_qa,is_hdfs_path_read,isPartitioned,schema,readMode,runDay,spark):
 try:
  header_schema_cols = 'column_name,data_type,nullable'  
  if int(is_hdfs_path_read) == 0:
    arr = [str(x) for x in qTab.split('.')]
    schemaPathQA = schemaPathQA + "/" + arr[1] + ".csv"
    qaSchemaDf = spark.read.option('delimiter','|').csv(schemaPathQA,inferSchema=True,schema=schema,header=False,mode=readMode)
    qaSchemaDf = qaSchemaDf.drop(*['bc1','bc2','bc3']).withColumn('column_name',trim(col('column_name'))).withColumn('data_type',trim(col('data_type')))
    qaSchemaDf = qaSchemaDf.filter("column_name != '' and column_name not in ('# col_name','# Partition Information') and column_name <> 'col_name'").dropDuplicates(['column_name'])
  elif int(is_hdfs_path_read) == 1 and isPartitioned == 'yes':
    header_schema = createSchema(header_schema_cols)
    hdfs_processed_qa = hdfs_processed_qa + "/" + qTab + "/as_of_date=" + runDay
    try:
     df = spark.read.parquet(hdfs_processed_qa)
    except Exception as e:
     sys.exit("ERROR: while reading parquet file for QA schema read - " + str(e))
    schemaArray = rFunctions.read_schema_array(df.schema)
    qaSchemaDf = spark.createDataFrame(data = schemaArray,schema=header_schema).drop(*['nullable'])
  elif int(is_hdfs_path_read) == 1 and isPartitioned == 'no':
    hdfs_processed_qa = hdfs_processed_qa + "/" + qTab
    header_schema = createSchema(header_schema_cols)
    try:
     df = spark.read.parquet(hdfs_processed_qa)
    except Exception as e:
     sys.exit("ERROR: while reading parquet file for QA schema read - " + str(e))
    schemaArray = rFunctions.read_schema_array(df.schema)
    qaSchemaDf = spark.createDataFrame(data = schemaArray,schema=header_schema).drop(*['nullable'])
  qaSchemaDf.cache()
  return qaSchemaDf
 except Exception as e:
  sys.exit("ERROR: while reading qa table schema: " + str(e))
  
def readDevSchema(schemaPathDev,dTab,hdfs_processed_dev,is_hdfs_path_read,isPartitioned,schema,readMode,runDay,spark):
 try:
  header_schema_cols = 'column_name,data_type,nullable'  
  if int(is_hdfs_path_read) == 0:
    arr = [str(x) for x in dTab.split('.')]
    schemaPathDev = schemaPathDev + "/" + arr[1] + ".csv"
    devSchemaDf = spark.read.option('delimiter','|').csv(schemaPathDev,inferSchema=True,schema=schema,header=False,mode=readMode)
    devSchemaDf = devSchemaDf.drop(*['bc1','bc2','bc3']).withColumn('column_name',trim(col('column_name'))).withColumn('data_type',trim(col('data_type')))
    devSchemaDf = devSchemaDf.filter("column_name != '' and column_name not in ('# col_name','# Partition Information') and column_name <> 'col_name'").dropDuplicates(['column_name'])
  elif int(is_hdfs_path_read) == 1 and isPartitioned == 'yes':
    header_schema = createSchema(header_schema_cols)
    hdfs_processed_dev = hdfs_processed_dev + "/" + dTab + "/as_of_date=" + runDay
    try:
     df = spark.read.parquet(hdfs_processed_dev)
     schemaArray = rFunctions.read_schema_array(df.schema)
     devSchemaDf = spark.createDataFrame(data = schemaArray,schema=header_schema).drop(*['nullable'])
    except Exception as e:
     sys.exit("ERROR: while reading parquet file for dev schema - " + str(e))
  elif int(is_hdfs_path_read) == 1 and isPartitioned == 'no':
    header_schema = createSchema(header_schema_cols)
    hdfs_processed_dev = hdfs_processed_dev + "/" + dTab
    try:
     df = spark.read.parquet(hdfs_processed_dev)
     schemaArray = rFunctions.read_schema_array(df.schema)
     devSchemaDf = spark.createDataFrame(data = schemaArray,schema=header_schema).drop(*['nullable'])
    except Exception as e:
     sys.exit("ERROR: while reading parquet file for dev schema - " + str(e))
  devSchemaDf.cache()
  return devSchemaDf
 except Exception as e:
  sys.exit("ERROR: while reading dev table schema: " + str(e))

def validateSchema(qaSchemaDf,devSchemaDf):
 try:
  qaSchemaDf = qaSchemaDf.withColumnRenamed('column_name','q_column_name').withColumnRenamed('data_type','q_data_type')
  devSchemaDf = devSchemaDf.withColumnRenamed('column_name','d_column_name').withColumnRenamed('data_type','d_data_type')
  schemaDiff = qaSchemaDf.join(devSchemaDf,qaSchemaDf.q_column_name == devSchemaDf.d_column_name,'full')
  schemaDiff = schemaDiff.where((col('q_column_name') != col('d_column_name')) | (col('q_data_type') != col('d_data_type')))
  schemaDiff = schemaDiff.fillna({'q_column_name':'CNF','q_data_type':'DTNF','d_column_name':'CNF','d_data_type':'DTNF'})
  if schemaDiff.count() > 0: print("Schema mismatches found. exiting the process...")
  #sys.exit(1)
  schemaDiff.cache()
  return schemaDiff
 except Exception as e:
  sys.exit("ERROR: while validating the schema: " + str(e))
  
## Function to generate dynamic sql strings
def generate_sql_strings(cs,df,n,isETS):
 try:
  uniqueKey = rFunctions.create_unique_key(cs,n) # Fetching unique key from the cs
  cols_list = derive_columns_list(df,uniqueKey) # Creating consolidate columns list from dev table
  if int(isETS) == 0: arr,qSelectStr,dSelectStr = rFunctions.create_select_cols_strings(cs,isETS,n) # Creating qa and dev select sql strings
  else: arr,qSelectStr,dSelectStr = rFunctions.create_select_cols_strings(cols_list,isETS,0)
  qSelectUniqueKey,dSelectUniqueKey = rFunctions.create_select_unique_keys(cs,n) # Creating qa and dev select unique key sql strings
  if int(isETS) == 0: commClause = rFunctions.create_comm_clause(cs,isETS,n) # Creating comparision sql string
  else: commClause = rFunctions.create_comm_clause(cols_list,isETS,0)
  joinStr = rFunctions.create_join_condition(cs,n) # Creating join sql string
  collect_str = rFunctions.create_collect_list_str(cols_list)
  return qSelectStr,dSelectStr,uniqueKey,commClause,joinStr,qSelectUniqueKey,dSelectUniqueKey,collect_str,cols_list
 except Exception as e:
  sys.exit('ERROR: while creating sql strings - ' + str(e))

# Function to check count of qa and dev tables
def countCheck(qTab,dTab,hdfs_processed_qa,hdfs_processed_dev,is_hdfs_path_read,isPartitioned,runDay,spark):
 print("Start fetching the total count...")
 try:
  if int(is_hdfs_path_read) == 1 and isPartitioned == 'yes':
       hdfs_processed_qa = hdfs_processed_qa + "/" + qTab + "/as_of_date=" + runDay
       hdfs_processed_dev = hdfs_processed_dev + "/" + dTab + "/as_of_date=" + runDay
       try:
        qaCountDf = spark.read.parquet(hdfs_processed_qa)
        devCountDf = spark.read.parquet(hdfs_processed_dev)
       except Exception as e:
        sys.exit("ERROR: while reading parquet files for count check - " + str(e))
  elif int(is_hdfs_path_read) == 1 and isPartitioned == 'no':
       hdfs_processed_qa = hdfs_processed_qa + "/" + qTab
       hdfs_processed_dev = hdfs_processed_dev + "/" + dTab
       try:
        qaCountDf = spark.read.parquet(hdfs_processed_qa)
        devCountDf = spark.read.parquet(hdfs_processed_dev)
       except Exception as e:
        sys("ERROR: while reading parquet files for count check - " + str(e))
  elif int(is_hdfs_path_read) == 0 and isPartitioned == 'yes':
       qaCountDf = spark.sql("""select * from """ + qTab + """ where as_of_date=to_date('""" + runDay + """')""")
       devCountDf = spark.sql("""select * from """ + dTab + """ where as_of_date=to_date('""" + runDay + """')""")
  elif int(is_hdfs_path_read) == 0 and isPartitioned == 'no':
       qaCountDf = spark.sql("""select * from """ + qTab)
       devCountDf = spark.sql("""select * from """ + dTab)
  
  if qaCountDf.count() <> devCountDf.count():
    print("QA count:" + str(qaCountDf.count()) + " does not match with dev count:" + str(devCountDf.count()))
  else:
    print("QA count:" + str(qaCountDf.count()) + " matches with dev count:" + str(devCountDf.count()))
  print("Total count check complete...")
  return qaCountDf,devCountDf
 except Exception as e:
  sys.exit("ERROR: while comparing record count - " + str(e))

def derive_columns_list(df,uniqueKey):
 try:
  cols_list = list(df.select('column_name').toPandas()['column_name'])
  cols_list = [str(x) for x in cols_list if x not in uniqueKey]
  cols_list = list(set(cols_list)- set(['edl_load_ts','as_of_date']))
  cols_list.sort(reverse = True )
  return cols_list
 except Exception as e:
  sys.exit("ERROR: while deriving columns list - " + str(e))

# Function to check duplicate records 
def duplicateCheck(dTab,uniqueKey,cols_list,collect_str,hdfs_processed_dev,is_hdfs_path_read,isPartitioned,runDay,spark):
 print("Start validating duplicate record check .....")
 try:
  if int(is_hdfs_path_read) == 1 and isPartitioned == 'yes':
     hdfs_processed_dev = hdfs_processed_dev + "/" + dTab + "/as_of_date=" + runDay
     try:
      spark.read.parquet(hdfs_processed_dev).createOrReplaceTempView('dTab')
     except Exception as e:
      sys.exit("ERROR: while reading parquet file for duplicate check - " + str(e))
     dupDf = spark.sql("select " + uniqueKey + ", count(*) as dupCount," + collect_str + " from dTab group by " + uniqueKey + " having count(1) > 1")
     dedup_cols = rFunctions.dedup_columns(dupDf,cols_list,spark) # Function call to get columns for drop
     if dedup_cols is not None: dupDf = dupDf.drop(*dedup_cols).dropDuplicates()
  elif int(is_hdfs_path_read) == 1 and isPartitioned == 'no':
     hdfs_processed_dev = hdfs_processed_dev + "/" + dTab
     try:
      spark.read.parquet(hdfs_processed_dev).createOrReplaceTempView('dTab')
     except Exception as e:
      sys.exit("ERROR: while reading parquet file for duplicate check - " + str(e))
     dupDf = spark.sql("select " + uniqueKey + ", count(*) as dupCount, " + collect_str + " from dTab group by " + uniqueKey + " having count(1) > 1")
     dedup_cols = rFunctions.dedup_columns(dupDf,cols_list,spark) # Function call to get columns for drop
     if dedup_cols is not None: dupDf = dupDf.drop(*dedup_cols).dropDuplicates()
  elif int(is_hdfs_path_read) == 0 and isPartitioned == 'yes':
     dupDf = spark.sql("select " + uniqueKey + ", count(*) as dupCount," + collect_str + " from " + dTab + " where as_of_date=to_date('" + runDay + "') group by " + uniqueKey + " having count(1) > 1")
     dedup_cols = rFunctions.dedup_columns(dupDf,cols_list,spark) # Function call to get columns for drop
     if dedup_cols is not None: dupDf = dupDf.drop(*dedup_cols).dropDuplicates()
  elif int(is_hdfs_path_read) == 0 and isPartitioned == 'no':
     dupDf = spark.sql("select " + uniqueKey + ",count(*) as dupCount, " + collect_str + " from " + dTab + " group by " + uniqueKey + " having count(1) > 1")
     dedup_cols = rFunctions.dedup_columns(dupDf,cols_list,spark) # Function call to get columns for drop
     if dedup_cols is not None: dupDf = dupDf.drop(*dedup_cols).dropDuplicates()
  print("Duplicate record check complete.....")
  dupDf.cache()
  return dupDf
 except Exception as e:
  sys.exit("ERROR: while computing duplicate check - " + str(e))
 
#Function to compare data between two hive tables 
def perform_data_validation(qTab,dTab,qSelectStr,dSelectStr,uniqueKey,commClause,joinStr,qSelectUniqueKey,dSelectUniqueKey,isPartitioned,hdfs_processed_qa,hdfs_processed_dev,is_hdfs_path_read,runDay,spark):
 try:
  if int(is_hdfs_path_read) == 1 and isPartitioned == 'yes':
    hdfs_processed_qa = hdfs_processed_qa + "/" + qTab + "/as_of_date=" + runDay
    hdfs_processed_dev = hdfs_processed_dev + "/" + dTab + "/as_of_date=" + runDay
    try:
     spark.read.parquet(hdfs_processed_qa).createOrReplaceTempView('qTab')
     spark.read.parquet(hdfs_processed_dev).createOrReplaceTempView('dTab')
    except Exception as e:
     sys.exit("ERROR: while reading parquet file for table scan - " + str(e))
    sqlQuery = "select " + qSelectUniqueKey + "," + dSelectUniqueKey\
                          + "," + qSelectStr + "," + dSelectStr\
                          + " from qTab full outer join dTab ON ("\
                          + joinStr + ") "\
                          + "where (" + commClause + ")"
  elif int(is_hdfs_path_read) == 1 and isPartitioned == 'no':
    hdfs_processed_qa = hdfs_processed_qa + "/" + qTab
    hdfs_processed_dev = hdfs_processed_dev + "/" + dTab
    try:
     spark.read.parquet(hdfs_processed_qa).createOrReplaceTempView('qTab')
     spark.read.parquet(hdfs_processed_dev).createOrReplaceTempView('dTab')
    except Exception as e:
     sys.exit("ERROR: while reading parquet file for table scan - " + str(e))
    sqlQuery = "select " + qSelectUniqueKey + "," + dSelectUniqueKey\
                         + "," + qSelectStr + "," + dSelectStr\
                         + " from qTab full outer join dTab ON ("\
                         + joinStr + ") "\
                         + "where (" \
                         + commClause + ")"
  elif int(is_hdfs_path_read) == 0 and isPartitioned == 'yes':
    sqlQuery = "select " + qSelectUniqueKey + ","\
                         + dSelectUniqueKey + ","\
                         + qSelectStr + ","\
                         + dSelectStr + " from "\
                         + qTab + " qTab full outer join "\
                         + dTab + " dTab ON ("\
                         + joinStr + ") "\
                         + "where qTab.as_of_date = to_date('" + runDay + "') and dTab.as_of_date = to_date('" + runDay + "') and ("\
                         + commClause + ")"
  elif int(is_hdfs_path_read) == 0 and isPartitioned == 'no':
    sqlQuery = "select " + qSelectUniqueKey + "," + dSelectUniqueKey\
                         + "," + qSelectStr + "," + dSelectStr\
                         + " from " + qTab + " qTab full outer join " \
                         + dTab + " dTab ON ("\
                         + joinStr + ") "\
                         + "where (" + commClause + ")"
  commDf = spark.sql(sqlQuery)
  commDf.cache()
  return commDf
 except Exception as e:
  sys.exit('ERROR: while comparing data between qa and dev tables processed data - '+ str(e))

def createLDR(commDf,schemaDiff,qa_count,dev_count,dupDf,runDay,spark):
 try:
  schema_diff_count = schemaDiff.count()
  disc_count = commDf.count()
  duplicate_count = dupDf.count()
  statistics_cols = "disc_count,schema_diff_count,qa_count,dev_count,duplicate_count"
  statistics_schema = createSchema(statistics_cols)
  statistics_cols_val = [disc_count,schema_diff_count,qa_count,dev_count,duplicate_count]
  rdd = spark.sparkContext.parallelize([statistics_cols_val])
  statisticsDf = spark.createDataFrame(data=rdd, schema = statistics_schema)
  commDf = commDf.withColumn('edl_load_ts',current_timestamp())
  statisticsDf = statisticsDf.select(col('disc_count').cast('int').alias('disc_count')
                                    ,col('schema_diff_count').cast('int').alias('schema_diff_count')
                                    ,col('qa_count').cast('int').alias('qa_count')
                                    ,col('dev_count').cast('int').alias('dev_count')
                                    ,col('duplicate_count').cast('int').alias('duplicate_count')
                                     ).withColumn('edl_load_ts',current_timestamp())
  schemaDiff = schemaDiff.withColumn('edl_load_ts',lit(current_timestamp()))
  dupDf = dupDf.withColumn('edl_load_ts',lit(current_timestamp()))
  
  return commDf,statisticsDf,dupDf,schemaDiff
 except Exception as e:
  sys.exit("ERROR: while creating load ready dataframe: " + str(e))

def load_data_validations_results(commDf,dupDf,schemaDiff,statisticsDf,extGenValPath,qTab,is_hdfs_path_read,writeMode,spark):
 print('######################## Starting loading data comm validations ######################################')              
 try:
  if int(is_hdfs_path_read) == 0:
   extDDPath = extGenValPath + "/" + [qTab.split(".",1)][0][1]
   extDSPath = extGenValPath + "/statistics/" + [qTab.split(".",1)][0][1]
   extSchDiscPath = extGenValPath + "/schema/" + [qTab.split(".",1)][0][1]
   extDuplicatePth = extGenValPath + "/duplicate/" + [qTab.split(".",1)][0][1]
  else:
   extDDPath = extGenValPath + "/" + qTab
   extDSPath = extGenValPath + "/statistics/" + qTab
   extSchDiscPath = extGenValPath + "/schema/" + qTab
   extDuplicatePth = extGenValPath + "/duplicate/" + qTab
  spark.sql('set hive.exec.dynamic.partition=true')
  spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
  if commDf.count() >= 1: commDf.write.mode(writeMode).option('header','true').format('parquet').save(extDDPath)
  else: commDf.repartition(1).write.mode(writeMode).option('header','true').format('parquet').save(extDDPath)
  if statisticsDf.count() >= 1: statisticsDf.write.mode(writeMode).option('header','true').format('parquet').save(extDSPath)
  else: statisticsDf.repartition(1).write.mode(writeMode).option('header','true').format('parquet').save(extDSPath)
  if schemaDiff.count() >= 1: schemaDiff.write.mode(writeMode).option('header','true').format('parquet').save(extSchDiscPath)
  else: schemaDiff.repartition(1).write.mode(writeMode).option('header','true').format('parquet').save(extSchDiscPath)
  if dupDf.count() >= 1: dupDf.write.mode(writeMode).option('header','true').format('parquet').save(extDuplicatePth)
  else: dupDf.repartition(1).write.mode(writeMode).option('header','true').format('parquet').save(extDuplicatePth)
  print('######################## Finished loading data comm validations ###########################################')
 except Exception as e:
  sys.exit('ERROR: while loading data comm validations - ' + str(e))

########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel('ERROR')
 configFileName, argsList = validateArg()
 inputParams = validateConfigFile(configFileName)
 schema = createSchema(inputParams['cols_schema'])
 qaSchemaDf = readQASchema(inputParams['schemaPathQA'],argsList['qTab'],argsList['hdfs_processed_qa'],argsList['is_hdfs_path_read'],argsList['isPartitioned'],schema,inputParams['readMode'],argsList['runDay'],spark)
 devSchemaDf = readDevSchema(inputParams['schemaPathDev'],argsList['dTab'],argsList['hdfs_processed_dev'],argsList['is_hdfs_path_read'],argsList['isPartitioned'],schema,inputParams['readMode'],argsList['runDay'],spark)
 schemaDiff = validateSchema(qaSchemaDf,devSchemaDf)
 qaCountDf,devCountDf = countCheck(argsList['qTab'],argsList['dTab'],argsList['hdfs_processed_qa'],argsList['hdfs_processed_dev'],argsList['is_hdfs_path_read'],argsList['isPartitioned'],argsList['runDay'],spark)
 qSelectStr,dSelectStr,uniqueKey,commClause,joinStr,qSelectUniqueKey,dSelectUniqueKey,collect_str,cols_list = generate_sql_strings(argsList['cs'],devSchemaDf,argsList['n'],argsList['is_ets_control'])
 dupDf = duplicateCheck(argsList['dTab'],uniqueKey,cols_list,collect_str,argsList['hdfs_processed_dev'],argsList['is_hdfs_path_read'],argsList['isPartitioned'],argsList['runDay'],spark)
 commDf = perform_data_validation(argsList['qTab'],argsList['dTab'],qSelectStr,dSelectStr,uniqueKey,commClause,joinStr,qSelectUniqueKey,dSelectUniqueKey,argsList['isPartitioned'],argsList['hdfs_processed_qa'],argsList['hdfs_processed_dev'],argsList['is_hdfs_path_read'],argsList['runDay'],spark)
 commDf,statisticsDf,dupDf,schemaDiff = createLDR(commDf,schemaDiff,qaCountDf.count(),devCountDf.count(),dupDf,argsList['runDay'],spark)
 load_data_validations_results(commDf,dupDf,schemaDiff,statisticsDf,inputParams['extGenValPath'],argsList['qTab'],argsList['is_hdfs_path_read'],inputParams['writeMode'],spark)
 
if __name__ == '__main__':
    main()