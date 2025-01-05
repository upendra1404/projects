###############################################################################
# Script Name:amfSeviceC360PurgePartitions.py                                 #
# Purpose: To determine partitions older than 90 days and purge it            #
# Created by: Upendra Kumar                                                   #
# Create Date: 01/06/2021                                                     #
# Modify date: 12/28/2020                                                     #
###############################################################################

from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from ConfigParser import ConfigParser
from pyspark.sql import Row
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
                if ('edl_amf_c360_hive_ldr' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain edl_amf_c360_hive_ldr")
                else:
                    edl_amf_c360_hive_ldr = config.get('HIVE','edl_amf_c360_hive_ldr')
                if ('c360AmfQA' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain c360AmfQA")
                else:
                    c360AmfQA = config.get('HIVE','c360AmfQA')
        return {
                     'edl_amf_c360_hive_ldr': edl_amf_c360_hive_ldr,
                     'c360AmfQA': c360AmfQA
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

########################################################################################################################
# Reading hive partitions for c360 attibute table                                                                      #
# Calling function validatePartitions() to filter out partitions fall on 1st day of the month                          #
# Forming a 1st day date of each month from as_of_date and comparing against as_of_date                                #
########################################################################################################################
def readPartitions(c360AmfQA,edl_amf_c360_hive_ldr,spark):
 rdPartitionsDF = spark.sql("""select distinct cast(as_of_date as string) as as_of_date from """ + edl_amf_c360_hive_ldr + """ where as_of_date < date_sub(current_date(),int(90))""")
 if (rdPartitionsDF.count() > 0):
       vldPartitionsDF = validatePartitions(rdPartitionsDF,c360AmfQA,edl_amf_c360_hive_ldr,spark)
       if (vldPartitionsDF.count() > 0):
          print("Partition exist either for more than 90 days to purge or extra partitions present on hive to purge")
          vldPartitionsDF.show()
       else:
          print("No partitions exist for more than 90 days to purge !!")
          sys.exit(0)
 else:
       print("No partitions exist for more than 90 days!!")
       sys.exit(0) 
 return vldPartitionsDF

#######################################################################################################################
# Function to validate that partitions do not fall on 1st day of each month                                                 #
#######################################################################################################################
def validatePartitions(rdPartitionsDF,c360AmfQA,edl_amf_c360_hive_ldr,spark):  
 vldPartitionsDF = rdPartitionsDF\
                  .withColumn('firstDayOfMonth',trunc(col('as_of_date'),"month"))\
                  .filter("as_of_date <> firstDayOfMonth")\
                  .drop(col('firstDayOfMonth'))
 deltaPartitions = partitionOnHiveNotOnHDFS(c360AmfQA,edl_amf_c360_hive_ldr,spark)
 vldPartitionsDF = vldPartitionsDF.union(deltaPartitions)
 return vldPartitionsDF

### Function to list down delta partitions from hive which are inturn missing on hdfs
def partitionOnHiveNotOnHDFS(c360AmfQA,edl_amf_c360_hive_ldr,spark):
 sc = spark.sparkContext
 sqlContext = createSQLContext(spark)
 rowRDD = sc.parallelize(listHDFSDir(c360AmfQA,spark)).map(lambda(n): Row(n))
 hdfsPartitionDF = sqlContext.createDataFrame(rowRDD,['as_of_date'])
 hdfsPartitionDF = hdfsPartitionDF.withColumn('as_of_date',split(hdfsPartitionDF['as_of_date'],'=').getItem(1))
 readHivePartitions = spark.sql("""select distinct as_of_date from """ + edl_amf_c360_hive_ldr + """ where as_of_date >= date_sub(current_date(),int(90))""")
 deltaPartitions = readHivePartitions.subtract(hdfsPartitionDF)
 return deltaPartitions

### List down hive partitions directories
def listHDFSDir(c360AmfQA,spark):
 sc = spark.sparkContext
 fs = (sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()))
 return [str(f.getPath()) for f in fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(c360AmfQA))]

### Function to purge hdfs path
def getPurgeFromHDFS(path,spark):
 sc = spark.sparkContext
 fs = (sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()))
 fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path),True)

 
 ### Function to drop the hive partitions and delete hdfs data files
def dropPartitions(vldPartitionsDF,c360AmfQA,edl_amf_c360_hive_ldr,spark):
 print("####################################### Start dropping the partitions #########################################")
 for partitions in vldPartitionsDF.rdd.map(lambda x: x.as_of_date).collect():
      spark.sql("""ALTER TABLE """ + edl_amf_c360_hive_ldr + """ DROP IF EXISTS PARTITION (as_of_date = '{partitions}') purge""".format(partitions=partitions))
      getPurgeFromHDFS(c360AmfQA + "/" + "as_of_date=" + partitions,spark)
 spark.sql("""msck repair table """ + edl_amf_c360_hive_ldr)
 print("####################################### Finished dropping the partitions #########################################")
 
########################################################################################################################
### defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 configFileName = validateArg()
 inputParams = validateConfigFile(configFileName)
 vldPartitionsDF = readPartitions(inputParams['c360AmfQA'],inputParams['edl_amf_c360_hive_ldr'],spark)
 dropPartitions(vldPartitionsDF,inputParams['c360AmfQA'],inputParams['edl_amf_c360_hive_ldr'],spark)
 
if __name__ == "__main__":
    main()