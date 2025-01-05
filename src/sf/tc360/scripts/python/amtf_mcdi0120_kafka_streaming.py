###############################################################################
# Script Name:amtf_mcdi_permanent_storage.py                                  #
# Purpose: To subscribe data from kafka topic and load into EDL               #
# Created by: Upendra Kumar                                                   #
# Create Date: 02/28/2021                                                     #
# Modify date:                                                                #
###############################################################################

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkFiles
from pyspark import SparkContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import *
from pyspark.sql.functions import split
from pyspark.sql.types import StructType,StringType
from pyspark.sql.functions import from_json,col
from ConfigParser import ConfigParser
import commands
import getopt
import sys
import os

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
                if ('kafkaBootStrapServer' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain kafkaBootStrapServer")
                else:
                    kafkaBootStrapServer = config.get('KAFKA', 'kafkaBootStrapServer')
                if ('msg0120Topic' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain msg0120Topic")
                else:
                    msg0120Topic = config.get('KAFKA', 'msg0120Topic')
                if ('kafkaKeyStore' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain kafkaKeyStore")
                else:
                    kafkaKeyStore = config.get('KAFKA', 'kafkaKeyStore')
                if ('keystore_password_alias' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain keystore_password_alias")
                else:
                    keystore_password_alias = config.get('KAFKA', 'keystore_password_alias')
                if ('kafkaTrustStore' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain kafkaTrustStore")
                else:
                    kafkaTrustStore = config.get('KAFKA', 'kafkaTrustStore')
                if ('truststore_password_alias' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain truststore_password_alias")
                else:
                    truststore_password_alias = config.get('KAFKA', 'truststore_password_alias')
                if ('ssl_key_password_alias' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain ssl_key_password_alias")
                else:
                    ssl_key_password_alias = config.get('KAFKA', 'ssl_key_password_alias')
                if ('keyDeserialzer' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain keyDeserialzer")
                else:
                    keyDeserialzer = config.get('KAFKA', 'keyDeserialzer')
                if ('valueDeserialzer' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain valueDeserialzer")
                else:
                    valueDeserialzer = config.get('KAFKA', 'valueDeserialzer')
                if ('chkPt0120' not in config.options('HDFSPATH')):
                    sys.exit("ERROR: Section HDFSPATH doesn't contain chkPt0120")
                else:
                    chkPt0120 = config.get('HDFSPATH', 'chkPt0120')
                if ('msg0120HdfsPath' not in config.options('HDFSPATH')):
                    sys.exit("ERROR: Section HDFS doesn't contain msg0120HdfsPath")
                else:
                    msg0120HdfsPath = config.get('HDFSPATH', 'msg0120HdfsPath')
                if ('starting_offsets' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain starting_offsets")
                else:
                    starting_offsets = config.get('KAFKA', 'starting_offsets')
                if ('jceks_path' not in config.options('KAFKA')):
                    sys.exit("ERROR: Section KAFKA doesn't contain jceks_path")
                else:
                    jceks_path = config.get('KAFKA', 'jceks_path')
                    
        jceksConfig = {
                      'keystore_password_alias': keystore_password_alias
                      ,'truststore_password_alias': truststore_password_alias
                      ,'ssl_key_password_alias': ssl_key_password_alias
                      ,'jceks_path':jceks_path
                      }
                
        kafkaParams = {
                            'readMode': readMode
                            ,'writeMode': writeMode
                            ,'kafkaBootStrapServer': kafkaBootStrapServer
                            ,'msg0120Topic': msg0120Topic
                            ,'kafkaKeyStore': kafkaKeyStore
                            ,'kafkaTrustStore': kafkaTrustStore
                            ,'keyDeserialzer': keyDeserialzer
                            ,'valueDeserialzer': valueDeserialzer
                            ,'msg0120HdfsPath': msg0120HdfsPath
                            ,'chkPt0120': chkPt0120
                            ,'starting_offsets': starting_offsets
                      }
        return kafkaParams,jceksConfig
 
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder\
                     .appName('mcdi0120_kafka_edl_ingestion')\
                     .config("mapreduce.input.fileinputformat.input.dir.recursive","true")\
                     .config("spark.hadoop.hive.mapred.supports.subdirectories","true")\
                     .config("spark.sql.parquet.writeLegacyFormat","true")\
                     .config("spark.driver.allowMultipleContexts","true")\
                     .config("spark.sql.parquet.compression.codec", "snappy")\
                     .config("hive.exec.dynamic.partition.mode","nonstrict")\
                     .config("hive.exec.dynamic.partition","true")\
                     .config("hive.exec.parallel","true")\
                     .config("spark.streaming.concurrentJobs", "2")\
                     .enableHiveSupport()\
                     .getOrCreate()
 return spark

def parseJceksFile(jceksConfig,spark):
 hadoopConfiguration = spark.sparkContext._jsc.hadoopConfiguration()
 hadoopConfiguration.set('hadoop.security.credential.provider.path',jceksConfig['jceks_path'])
 try:
     for key in jceksConfig:
        alias = jceksConfig[key]
        jceksPassword = hadoopConfiguration.getPassword(alias)
        rawVal = ""
        if jceksPassword is not None:
           for i in range(len(jceksPassword)):
              rawVal = rawVal + str(jceksPassword[i])
              jceksConfig[key] = rawVal
     return jceksConfig
 except Exception as e:
          sys.exit("Error occured while parsing the jceks password credentials:" + str(e))      
      
def create0120Schema():
 columns0120Msg = [StructField('eventName',StringType(),True)\
              ,StructField('eventTimestamp',StringType(),True)\
              ,StructField('requestMetadata',StringType(),True)\
              ,StructField('cmx_tran_id',StringType(),True)\
              ,StructField('auth_decline_rsn_cd',StringType(),True)\
              ,StructField('auth_decision_response',StringType(),True)\
              ,StructField('transaction_date',StringType(),True)\
              ,StructField('status',StringType(),True)\
              ,StructField('amf_status',StringType(),True)]
 schema0120 = StructType(columns0120Msg)
 return schema0120
 
#######################################################################################################################
def init0120KafkaConsumer(kafkaParams,jceksConfig,spark):
 try:
        mcdi0120MsgDf = spark\
                        .readStream\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers",kafkaParams['kafkaBootStrapServer'])\
                        .option("kafka.security.protocol","SSL")\
                        .option("key.deserializer",kafkaParams['keyDeserialzer'])\
                        .option("value.deserializer",kafkaParams['valueDeserialzer'])\
                        .option("kafka.ssl.truststore.location",kafkaParams['kafkaTrustStore'])\
                        .option("kafka.ssl.truststore.password",jceksConfig['truststore_password_alias'])\
                        .option("kafka.ssl.keystore.location",kafkaParams['kafkaKeyStore'])\
                        .option("kafka.ssl.keystore.password",jceksConfig['keystore_password_alias'])\
                        .option("kafka.ssl.key.password",jceksConfig['ssl_key_password_alias'])\
                        .option("failOnDataLoss","false")\
                        .option("startingOffsets",kafkaParams['starting_offsets'])\
                        .option("subscribe",kafkaParams['msg0120Topic'])\
                        .load()
        mcdi0120MsgDf = mcdi0120MsgDf.selectExpr("CAST(key AS STRING)"
                                         , "CAST(value AS STRING)"
                                         , "CAST(topic AS STRING)"
                                         , "CAST(partition AS STRING)"
                                         , "CAST(offset AS STRING)"
                                         , "timestamp"
                                         , "CAST(timestampType AS STRING)")
        return mcdi0120MsgDf
 except Exception as e:
          print("ERROR: while configuring the initialising the kafka consumer " + str(e))
          sys.exit(1)
 
 
def transform0120Msg(mcdi0120MsgDf,schema0120):
 mcdi0120MsgDf = mcdi0120MsgDf.select("value").withColumn("json",from_json(col("value"),schema0120)).select(col("json.*"))
 mcdi0120MsgDf = mcdi0120MsgDf.select(col('cmx_tran_id').cast('string').alias("cmx_tran_id")\
                     ,col('auth_decline_rsn_cd').cast('string').alias("auth_decline_rsn_cd")\
                     ,col('auth_decision_response').cast('string').alias("auth_decision_response")\
                     ,col('transaction_date').cast('string').alias("transaction_date")\
                     ,col('status').cast('string').alias("status")\
                     ,col('amf_status').cast('string').alias("amf_status")).withColumn("as_of_date",current_date()).withColumn("edl_load_ts",current_timestamp())
 return mcdi0120MsgDf
 
def wrt0120Msg2HDFS(mcdi0120MsgDf,msg0120HdfsPath,chkPt0120):
 try:
     mcdi0120Qry = mcdi0120MsgDf\
                .writeStream\
                .format("parquet")\
                .option("checkpointLocation",chkPt0120)\
                .option("path",msg0120HdfsPath)\
                .partitionBy("as_of_date")\
                .start()\
                .awaitTermination()
 except Exception as e:
         print("Exception occurred while writing the 0120 mcdi stream to hdfs:" + str(e))
         sys.exit(1)
                        
########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 configFileName = "amfDataFlow.config"
 kafkaParams,jceksConfig = validateConfigFile(configFileName)
 jceksConfig = parseJceksFile(jceksConfig,spark)
 schema0120 = create0120Schema()
 spark.sparkContext.setLogLevel("ERROR")
 mcdi0120MsgDf = init0120KafkaConsumer(kafkaParams,jceksConfig,spark)
 mcdi0120MsgDf = transform0120Msg(mcdi0120MsgDf,schema0120)
 wrt0120Msg2HDFS(mcdi0120MsgDf,kafkaParams['msg0120HdfsPath'],kafkaParams['chkPt0120'])
 
if __name__ == "__main__":
    main()