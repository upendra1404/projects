###############################################################################
# Script Name:amtf_reusable_functions.py                                      #
# Purpose: re-usable functions definitions                                    #
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

################# Function declaration section starts ####################################
def getPreviousRunDay(runDay):
 try:
  previousDay = datetime.datetime.strptime(runDay,'%Y-%m-%d')+relativedelta(days = -1)
  if previousDay.weekday() == 6:
      previousDay = previousDay+relativedelta(days = -1)
  return previousDay
 except Exception as e:
  sys.exit("ERROR: while computing the previous run day - " + str(e))
  
def get_date(runDay,n):
 try:
  return datetime.datetime.strptime(runDay,'%Y-%m-%d')+relativedelta(days = -int(n))
 except Exception as e:
  sys.exit("ERROR: while compting dates n days:" + str(e))
  
def get_bin(n):
 return "{0:b}".format(n)
  
def updateNewValue(prev_val,new_val):
 update_val = 0
 if prev_val is not None and new_val is not None:
   update_val = float(prev_val)+float(new_val)
 elif prev_val is None and new_val is not None:
   update_val = float(new_val)
 elif prev_val is not None and new_val is None:
   update_val = float(prev_val)
 elif prev_val is None and new_val is None:
   update_val = None
 return update_val
  
def computeIncrMean(newList,oldMean,oldCount):
       sumForMean = 0
       if newList:
        newList = [y for y in [x for x in newList if x is not None] if str(y).strip() !='']
        if (len(newList) > 0 and oldMean is not None and oldCount is not None):
          n = 0
          oldSum = float(oldMean*oldCount)
          for i in range(0,len(newList)):
             newList[i] = float(newList[i])
             if (i == 0):
                k = oldCount + 1
                sumForMean = oldSum + newList[i]
                n += k
             else:
                sumForMean += newList[i]
                n += 1
             mean = sumForMean/(n)
        elif len(newList) > 0 and oldMean is None:
           n = 0
           for x in newList:
              sumForMean += float(x)
              n += 1
           return sumForMean/int(n)
        else:
           mean = oldMean
       else:
           mean = oldMean
       return mean
 
def computeIncrementalStddev(newList,oldSum,oldMean,oldCount):
 # Initialise the incremental sum variables    
     n = 0
     sumForMean = 0
     lagSum = 0
     lagMean = 0
     sum = 0
     if newList:
      x = [z for z in [y for y in newList if y is not None] if str(z).strip() !='']
      if (len(x) > 0 and oldMean is not None and (oldCount is not None or oldCount >0) and (oldSum is not None or oldSum==0)):
         oldCount = int(oldCount)
         for i in range(0,len(x)):
            x[i] = float(x[i])
            if (i == 0):
               k = oldCount + 1
               lagSum = float(oldSum)
               lagMean = float(oldMean)
               sumForMean = (oldMean*oldCount) + x[i]
               n += k
            else:
               sumForMean += x[i]
               n += 1
            mean = sumForMean/(n)
            sum = lagSum + ((x[i]-mean)*(x[i]-lagMean))
            lagMean = mean
            lagSum = sum
      elif (len(x) > 0 and (oldMean is None or oldMean==0 or oldCount is None or oldCount==0 or oldSum is None or oldSum==0)):
           for i in range(0,len(x)):
            x[i] = float(x[i])
            sumForMean += x[i]
            n += 1
            mean = sumForMean/(n)
            sum = lagSum + ((x[i]-mean)*(x[i]-lagMean))
            lagMean = mean
            lagSum = sum
      elif len(x) ==0 and (oldCount is None or oldSum is None): return None
      else:
         n = oldCount
         sum = oldSum
     elif oldCount is None or oldCount ==0:return None
     else:
         n = oldCount
         sum = oldSum
     if n <= 1:
       return 0
     else:
       return sqrt(sum/int(n))
   
## UDF function to calculate missing merchant id's from falcon 
def getMerchListLastDay(merchList_90d,merchList_1d):
 newList = []
 if merchList_1d is not None and merchList_90d is not None:
   newList = [x for x in merchList_1d if x not in merchList_90d]
 elif merchList_90d is None and merchList_1d is not None:
   newList = merchList_1d
 elif merchList_90d is not None and merchList_1d is None:
   newList = newList
 return newList
 
def removeDuplicatesList(x):
 return list(dict.fromkeys(x)) 

## UDF function to calculate missing txn dates from falcon 
def getMissingDates(sortedListDates,n,runDay):
    missingDates = []
    datesList_Obj = [datetime.datetime.strptime(runDay,'%Y-%m-%d') + relativedelta(days=-i) for i in range(1,n+1)]
    datesList = [datesList_Obj[i].strftime('%Y-%m-%d') for i in range(len(datesList_Obj))]
    missingDates = [dt for dt in datesList if dt not in sortedListDates]
    missingDates = [datetime.datetime.strptime(missingDates[i],'%Y-%m-%d') for i in range(len(missingDates))]
    return missingDates

def compute_timeboxed_stddev(sample):
 s_sum = float(0.0)
 s_variance = float(0.0)
 s_mean = float(0.0)
 n = int(0) 
 for x in sample:
       s_sum += float(x)
       n += 1
 s_mean = s_sum/(n)
 for x in sample:
       s_variance += (float(x)-float(s_mean))**2
 if (n >1):
   return sqrt(s_variance/(int(n)))
 else:
   return 0.0
   
def extend_list_amount(list,n):
 new_list = []
 if list is not None:
    new_list = list[:n]+[0.0]*(n-len(list[:n]))
 else:
     new_list = [0.0]*n
 return new_list
 
def extend_list_count(list,n):
 new_list = []
 if list is not None:
     new_list = list[:n]+[0]*(n-len(list[:n]))
 else:
     new_list = [0]*n
 return new_list

def arrayConcatenate(listPreviousDay,listOld,arr_size):
  listNew = []
  if (listPreviousDay and listOld):
     listNew = listPreviousDay
     for x in listOld:
       listNew.append(x)
  elif (listOld is None or len(listOld) ==0):
     listNew = listPreviousDay
  elif (listPreviousDay is None or len(listPreviousDay) ==0):
     listNew = listOld
  return listNew[:arr_size]

# Function to compare two sets
def compareList(a,b):
  c = []
  if a is None or b is None or len(a)==0 or len(b)==0: return None
  c = set(a) & set(b)
  if (c != set(a)) or (c != set(b)): bool = True
  else: bool = False
  return bool
 
def create_select_cols_strings(cs,isETS,n):
  arr = []
  qSelectStr = ''
  dSelectStr = ''
  if int(isETS) ==0: arr = [x for x in cs.split(',')]
  else: arr = cs
  for i in range(int(n),len(arr)):
   if i==int(n):
    qSelectStr = "qTab." + str(arr[i]) + " as qA_" + str(arr[i])
    dSelectStr = "dTab." + str(arr[i]) + " as dEv_" + str(arr[i])
   else:
    qSelectStr += ",qTab." +str(arr[i]) + " as qA_" + str(arr[i])
    dSelectStr += ",dTab." +str(arr[i]) + " as dEv_" + str(arr[i])
  return arr,qSelectStr,dSelectStr
 
def create_select_unique_keys(cs,n):
  arr = []
  qSelectUniqueKey = ''
  dSelectUniqueKey = ''
  arr = [x for x in cs.split(',')]
  for i in range(0,int(n)):
   if i ==0:
      qSelectUniqueKey = qSelectUniqueKey + "qTab." + str(arr[i]) + " as qA_" + str(arr[i])
      dSelectUniqueKey = dSelectUniqueKey + "dTab." + str(arr[i]) + " as dEv_" + str(arr[i])
   else:
      qSelectUniqueKey += "," + "qTab." + str(arr[i]) + " as qA_" + str(arr[i])
      dSelectUniqueKey += "," + "dTab." + str(arr[i]) + " as dEv_" + str(arr[i])
  return qSelectUniqueKey,dSelectUniqueKey
 
def create_unique_key(cs,n):
  arr = []
  uniqueKey = ''
  arr = [x for x in cs.split(',')]
  for i in range(0,int(n)):
   if i ==0:
      uniqueKey = uniqueKey + str(arr[i])
   else:
      uniqueKey += "," + str(arr[i])
  return uniqueKey
  
def create_comm_clause(cs,isETS,n):
  arr = []
  commClause = ''
  if int(isETS) ==0: arr = [x for x in cs.split(',')]
  else: arr = cs
  for i in range(int(n),len(arr)):
   if i ==int(n):
      commClause = "qTab." + str(arr[i]) + " <> dTab." +str(arr[i])
   else:
      commClause += " OR " + "qTab." + str(arr[i]) + " <> dTab." +str(arr[i])
  return commClause
  
def create_join_condition(cs,n):
  arr = []
  joinStr = ''
  arr = [x for x in cs.split(',')]
  for i in range(0,int(n)):
   if i ==0:
      joinStr = "qTab." + str(arr[i]) + " = dTab." +str(arr[i])
   else:
      joinStr += " and " + "qTab." + str(arr[i]) + " = dTab." +str(arr[i])
  return joinStr
  
def read_schema_array(schema):
 arr = []
 for f in schema.fields:
    arr.append([str(f.name),str(f.dataType),str(f.nullable)])
 return arr
 
def join_function(dfList,join_key):
 finalList = ""
 for i in range(len(dfList)-1):
  if i == 0:
     finalList = dfList[i] + ".join(" + dfList[i+1] + ",['" + join_key + "'],'left']"
  elif i > 1:
     finalList += ".join(" + dfList[i] + ",['" + join_key + "'],'left']"
 return finalList

def create_collect_list_str(columns_list):
 collect_str = ""
 for i in range(0,len(columns_list)):
     if i == 0: collect_str =  "collect_set(" + columns_list[i] + ") as " + columns_list[i]
     else: collect_str +=  "," + "collect_set(" + columns_list[i] + ") as " + columns_list[i]
 return collect_str

def dedup_columns(df,cols_list,spark):
 dup_columns = []
 df.createOrReplaceTempView("viewdF")
 collected = list(spark.sql("select * from viewdF").select(*cols_list).toPandas())
 for cols in cols_list:
  if df.rdd.isEmpty(): return None
  elif int(len(max(collected[cols]))) <= 1: dup_columns.append(cols)
 return dup_columns

################# Function declaration section ends ####################################