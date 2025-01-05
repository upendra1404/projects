###############################################################################
# Script Name:computeFalconAttributes.py                                      #
# Purpose: To calculate new tc360 account attributes from Falcon table        #
# Created by: Upendra Kumar                                                   #
# Create Date: 04/27/2021                                                     #
# Modify date:                                                                #
###############################################################################

from pyspark.sql.types import FloatType,IntegerType,ArrayType,StringType,DataType,StructField,StructType
from pyspark.sql import SparkSession,SQLContext,HiveContext,Window,Row
from pyspark.sql.functions import sum as _sum,mean as _mean, stddev_pop as _stddev,col,coalesce,lit,split,trim,size,lpad,length,to_date,concat,substring,current_date,expr,datediff,udf,array,desc,date_sub,count,collect_list,max,min,to_timestamp,row_number,rank,collect_set,explode,round,current_timestamp
from math import sqrt,exp,log
from ConfigParser import ConfigParser
from ast import literal_eval as safe_eval
import commands
import getopt
import sys
import os
sys.dont_write_bytecode = True
import datetime
from dateutil.relativedelta import relativedelta

def createSQLContext(spark):
 sc = spark.sparkContext
 sqlContext = SQLContext(sc)
 return sqlContext
 
###############################################################################################################################
# Reading entire Falcon table                                                                                                 #
# Dropping the duplicates based on date timestamp                                                                             #
# No filtering on fraud txn codes initially                                                                                   #
###############################################################################################################################
def readFalconTable(falconRawPath,edl_amf_falcon_raw,runDay,debug,spark):
 ###########################################################################################################################################################################
 # Reading and dropping duplicates records from falcon raw table based on timestamp fields - ffsl_date_yy,ffsl_date_mm,ffsl_date_dd,ffsl_time_hh,ffsl_time_mm,ffsl_time_ss #
 ###########################################################################################################################################################################
 try:
  if debug == 'yes':
   falconDf = spark.read.orc(falconRawPath).where(col('as_of_date') < lit(runDay)).select("*")
  else:
   falconDf = spark.sql("""select * from """ + edl_amf_falcon_raw).where(col('as_of_date') < lit(runDay)).select("*")
  
  falconDf.cache()
  return falconDf
 except Exception as e:
  sys.exit("ERROR: while reading falcon data - " + str(e))