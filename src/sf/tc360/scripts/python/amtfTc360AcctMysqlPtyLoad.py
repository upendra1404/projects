###################################################################
# Purpose: Loading Tc360 aci attribute into MYSQL Pty table       #
# Created by: Upendra Kumar                                       #
# Created on: 18-Oct-2021                                         #
# Modified on :                                                   #
###################################################################
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from ConfigParser import ConfigParser
import os
import sys
import getopt
import datetime
import commands

## Validate the input arguments passed into spark submit command
def validateArg():
    mysqlAmfHostname = None
    mysqlAmfDbname = None
    mysqlAmfJdbcPort = None
    mysqlAmfUserId = None
    mysqlAmfPass = None
    mysqlAmfScriptPath = None
    mySQLJCEKSPath = None
    runDay = None

    printError = "spark-submit script_name.py -f <config_file_path>/<config_file_name> -h mysqlHostname -d mysqlDatabaseName -n $<mysql_amf_jdbc_port> -u $<mysql_amf_user_id> -p $<mysql_amf_pass> -m $<mysql_script_dir> -j $<mysql_amf_jceks> -r $<runDay>"
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:h:d:n:u:p:m:j:r:")
    except getopt.error as msg:
        print("Something went wrong!")
        print("Example for entering argument to the script is:")
        sys.exit(printError)

    sqlOpts = opts[1:]

    for opt, arg in opts:
        if(opt == "-f"):
            configFileName = arg
        elif(opt == "-h"):
            mysqlAmfHostname = arg
        elif(opt == "-d"):
            mysqlAmfDbname = arg
        elif(opt == "-n"):
            mysqlAmfJdbcPort = arg
        elif(opt == "-u"):
            mysqlAmfUserId = arg
        elif (opt == "-p"):
            mysqlAmfPass = arg
        elif(opt == "-m"):
            mysqlAmfScriptPath = arg
        elif(opt == "-j"):
            mySQLJCEKSPath = arg
        elif(opt == "-r"):
            runDay = arg
            
    if(configFileName is None or configFileName == ''):
        print(printError)
        sys.exit("ERROR: Config File Name not provided with argument -f")
    elif(mysqlAmfHostname is None or mysqlAmfHostname == ''):
        print(printError)
        sys.exit("ERROR: MYSQL Hostname not provided with argument -h")
    elif(mysqlAmfDbname is None or mysqlAmfDbname == ''):
        print(printError)
        sys.exit("ERROR: MYSQL Database name not provided with argument -d")
    elif(mysqlAmfJdbcPort is None or mysqlAmfJdbcPort == ''):
        print(printError)
        sys.exit("ERROR: MYSQL Port Number not provided with argument -n")
    elif(mysqlAmfUserId is None or mysqlAmfUserId == ''):
        print(printError)
        sys.exit("ERROR: MYSQL User ID not provided with argument -u")
    elif (mysqlAmfPass is None or mysqlAmfPass == ''):
        print(printError)
        sys.exit("ERROR: MYSQL Password not provided with argument -u")
    elif(mysqlAmfScriptPath is None or mysqlAmfScriptPath == ''):
        print(printError)
        sys.exit("ERROR: MYSQL Script Path not provided with argument -m")
    elif (mySQLJCEKSPath is None or mySQLJCEKSPath == ''):
        print(printError)
        sys.exit("ERROR: MYSQL jceks path not provided with argument -j")
    elif (runDay is None or runDay == ''):
        print(printError)
        sys.exit("ERROR: runDay is not provided with argument -r")

    return configFileName, sqlOpts

## Parsing sqlJceks file attributes
def parseSQLOpts(sqlOpts):
    mysqlAmfHostname = None
    mysqlAmfDbname = None
    mysqlAmfJdbcPort = None
    mysqlAmfUserId = None
    mysqlAmfPass = None
    mysqlAmfScriptPath = None
    mySQLJCEKSPath = None

    for opt, arg in sqlOpts:

        if(opt == "-h"):
            mysqlAmfHostname = arg
        elif(opt == "-d"):
            mysqlAmfDbname = arg
        elif(opt == "-n"):
            mysqlAmfJdbcPort = arg
        elif(opt == "-u"):
            mysqlAmfUserId = arg
        elif (opt == "-p"):
            mysqlAmfPass = arg
        elif(opt == "-m"):
            mysqlAmfScriptPath = arg
        elif(opt == "-j"):
            mySQLJCEKSPath = arg
        elif(opt == "-r"):
            runDay = arg

    sqlParams = {"mysqlAmfHostname": mysqlAmfHostname
                 ,"mysqlAmfDbname": mysqlAmfDbname
                 ,"mysqlAmfJdbcPort": mysqlAmfJdbcPort
                 ,"mysqlAmfUserId": mysqlAmfUserId
                 ,"mysqlAmfPass": mysqlAmfPass}

    configParams = {"mysqlAmfScriptPath": mysqlAmfScriptPath
                   ,"mySQLJCEKSPath": mySQLJCEKSPath}
    return sqlParams, configParams, runDay

## Retrieving password from Jceks file
def parseJceksFile(spark, sqlParams, jceksFile):
    print("Retrieving MySQL Password from the encrypted .jceks file")
    hadoopConfig=spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConfig.set("hadoop.security.credential.provider.path", jceksFile)
    for key in sqlParams:
        alias = sqlParams[key]
        jceksVal = hadoopConfig.getPassword(alias)
        rawVal = ""
        if jceksVal is not None:
            for i in range(len(jceksVal)):
                rawVal = rawVal + str(jceksVal[i])
            sqlParams[key] = rawVal
        else:
            print("ERROR: The alias (" + alias + ") does not exist in the given jceks file (" +
                        jceksFile + "). Please check that the jceks file exists, with the given alias.")
            sys.exit("MySQL configuration information is missing, exiting script now.")

    return sqlParams

## Validating config file
def validateConfigFile(configFileName):
    print("Validating Config File")
    if(not(os.path.exists(configFileName))):
        sys.exit("ERROR: The Configuration file "+configFileName+" doesn't exist")
    else:
        config = ConfigParser()
        config.optionxform = str
        config.read(configFileName)
        #Checking the Config File Sections
        if('SCHEMA' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: SCHEMA")
        elif('HIVE' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: HIVE")
        elif('HDFSPATH' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: HDFSPATH")
        elif('MYSQL' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: MYSQL")
        elif('FUNCTIONAL' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: FUNCTIONAL")
        
        else:
            #check the options in each section
            if('protegrityJar' not in config.options('FUNCTIONAL')):
                sys.exit("ERROR: Section FUNCTIONAL doesn't contain protegrityJar")
            else:
                jarPath = config.get('FUNCTIONAL','protegrityJar')

            jarExists = os.system("ls " + jarPath)
            if(jarExists !=0):
                sys.exit("ERROR: The path to the protegrity jar: " + jarPath + " does not exist")
            else:
                protegrityJar = jarPath
                
            if('readModeRawFile' not in config.options('FUNCTIONAL')):
                sys.exit("ERROR: Section FUNCTIONAL doesn't contain readModeRawFile")
            else:
                readMode = config.get('FUNCTIONAL','readModeRawFile')

            readOptions=['DROPMALFORMED','PERMISSIVE']
            if readMode in readOptions:
                readMode = readMode       
            if('mySQLJarFile' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain mySQLJarFile")
            else:
                mySQLJarFile = config.get('MYSQL','mySQLJarFile')
            if('mySQLDriver' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain mySQLDriver")
            else:
                mySQLDriver = config.get('MYSQL','mySQLDriver')
            if('c360AttrHiveLdrQry' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain c360AttrHiveLdrQry")
            else:
                c360AttrHiveLdrQry = config.get('MYSQL','c360AttrHiveLdrQry')
            if('mysqlTc360Tbl' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain mysqlTc360Tbl")
            else:
                mysqlTc360Tbl = config.get('MYSQL','mysqlTc360Tbl')
            if('edl_amf_c360_hive_ldr' not in config.options('HIVE')):
                sys.exit("ERROR: Section MYSQL doesn't contain edl_amf_c360_hive_ldr")
            else:
                edl_amf_c360_hive_ldr = config.get('HIVE','edl_amf_c360_hive_ldr')
            if('devPtyMysqlTable' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain devPtyMysqlTable")
            else:
                devPtyMysqlTable = config.get('MYSQL','devPtyMysqlTable')
            if('mysqlPtyDiffTable' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain mysqlPtyDiffTable")
            else:
                mysqlPtyDiffTable = config.get('MYSQL','mysqlPtyDiffTable')            
            if('mySQLAesJar' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain mySQLAesJar")
            else:
                mySQLAesJar = config.get('MYSQL','mySQLAesJar')
            if('sparkJar' not in config.options('FUNCTIONAL')):
                sys.exit("ERROR: Section FUNCTIONAL doesn't contain sparkJar")
            else:
                sparkJar = config.get('FUNCTIONAL','sparkJar')
            if('extPathAcct' not in config.options('HDFSPATH')):
                sys.exit("ERROR: Section MYSQL doesn't contain extPathAcct")
            else:
                extPathAcct = config.get('HDFSPATH','extPathAcct')
                
    print("Config File Successfully Validated")
    print("##################################################################################")
    
    return {'readMode': readMode,
            'mySQLJarFile': mySQLJarFile,
            'mySQLDriver': mySQLDriver,
            'protegrityJar': protegrityJar,
            'c360AttrHiveLdrQry': c360AttrHiveLdrQry,
            'mysqlTc360Tbl': mysqlTc360Tbl,
            'edl_amf_c360_hive_ldr': edl_amf_c360_hive_ldr,
            'devPtyMysqlTable': devPtyMysqlTable,
            'mysqlPtyDiffTable': mysqlPtyDiffTable,
            'mySQLAesJar': mySQLAesJar,
            'sparkJar': sparkJar,
            'extPathAcct': extPathAcct
            }

## Forming the jdbc mysql connection string 
def buildJdbcUrl(hostname, jdbcPort, dbName,mySQLUser, mySQLPass):
    print("##################################################################################")
    print("Creating MYSQL JDBC URL")

    jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&rewriteBatchedStatements=true".format(hostname, jdbcPort, dbName, mySQLUser)

    print("MYSQL JDBC URL Created")
    print("##################################################################################")
    return jdbcUrl
    
### create spark session
def createSparkSession(protegrityJar,mySQLJarFile,mySQLAesJar,sparkJar):
 spark = SparkSession.builder.\
                     enableHiveSupport().appName('Tc360AcctAttrApp').getOrCreate()
 sqlContext = SQLContext(spark.sparkContext)
 spark.sql("SET hive.mapred.supports.subdirectories=true")
 spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
 status,protegrityFile = commands.getstatusoutput("ls " + protegrityJar + "/" + "pephive-*.jar")
 spark.sql("add jar " + protegrityFile)
 spark.sql("add jar " + sparkJar)
 spark.sql("add jar " + mySQLJarFile)
 spark.sql("add jar " + mySQLAesJar)
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'")
 sqlContext.registerJavaFunction("encrypt", "com.syf.edl.services.creditservice.crypto.udf.EncrypUDF")
 sqlContext.registerJavaFunction("decrypt", "com.syf.edl.services.creditservice.crypto.udf.DecryptUDF")
 return spark

## Creating Hive context
def readHiveContext(spark):
 hive = HiveContext(spark)
 return hive

## Reading hive table for c360 attributes
def readTc360Account(extPathAcct,c360AttrHiveLdrQry,runDay,spark):
 try:
  print("#########################################################################################################################")
  print("Reading Tc360 attribute hive table: Starts")
  extPathAcct = extPathAcct+"/as_of_date="+ runDay
  tc360AcctDf = spark.read.parquet(extPathAcct).where(col('aci_0_2') >=2)\
                          .select(col('current_account_nbr_pty').alias('account_number')
                                ,col('ACCT_CARD_NETWORK').alias('lexid')
                                ,col('acct_world_auth_pct_90d').alias('tran_amt_mean_last_30_days')
                                ,col('acct_bal').alias('tran_amt_std_last_30_days')
                                ,col('ACCT_SALE_AMT_PER_DAY_30_STD').alias('tran_cnt_per_day_mean_last_30_days')
                                ,col('ACCT_AS_AUTH_90D').alias('tran_cnt_per_day_std_last_30_days')
                                ,col('ACCT_FRAUD_CLAIM_NON_TNF_TOT').alias('tran_cnt_total_last_30_days')
                                ,col('ACCT_TOP_MCC_90D_AMT_PER_DAY').alias('tran_cnt_prev_day'),col('aci_0_2')).withColumn('foreign_tran_prev_day',lit('')).withColumn('aci_0_1',lit(''))
  
  tc360AcctDf = tc360AcctDf.withColumn('account_number',explode(split(regexp_replace(col("account_number"), "[^0-9A-Za-z,]", ""),","))).dropDuplicates()
  tc360AcctDf = tc360AcctDf.filter("trim(account_number) <> ''")
  tc360AcctDf.cache()
  return tc360AcctDf
 except Exception as e:
  sys.exit("ERROR: while reading account table: " + str(e))

def createLRDf(tc360AcctDf):
 try:
  load_ts = current_timestamp()
  mysqlPtyDf = tc360AcctDf.select(col('account_number').cast('varchar(100)').alias('account_number')
                   ,col('lexid').cast('varchar(30)').alias('lexid')
                   ,col('tran_amt_mean_last_30_days').cast('decimal(14,2)').alias('tc360_tran_amt_mean_last_30_days')
                   ,col('tran_amt_std_last_30_days').cast('decimal(14,2)').alias('tc360_tran_amt_std_last_30_days')
                   ,col('tran_cnt_per_day_mean_last_30_days').cast('decimal(14,2)').alias('tc360_tran_cnt_per_day_mean_last_30_days')
                   ,col('tran_cnt_per_day_std_last_30_days').cast('decimal(14,2)').alias('tc360_tran_cnt_per_day_std_last_30_days')
                   ,col('tran_cnt_total_last_30_days').cast('decimal(14,0)').alias('tc360_tran_cnt_total_last_30_days')
                   ,col('tran_cnt_prev_day').cast('decimal(14,0)').alias('tc360_tran_cnt_prev_day')
                   ,col('foreign_tran_prev_day').cast('char(1)').alias('tc360_foreign_tran_prev_day')
                   ,col('aci_0_1').cast('char(1)').alias('tc360_aci_0_1')
                   ,col('aci_0_2').cast('int').alias('tc360_aci_0_2')).dropDuplicates(['account_number']).withColumn('load_ts',date_format(load_ts,"yyyy-MM-dd hh:mm:ss"))
  return mysqlPtyDf 
 except Exception as e:
  sys.exit("ERROR: while creating mysql pty load ready df:" + str(e))

def truncatePtyMySql(sqlParams,mysqlAmfScriptPath):
 try:
  print("####################Truncating mysql pty table ####################")
  standInLoadFlag = os.system("mysql --host=" + sqlParams['mysqlAmfHostname'] + " --user=" + sqlParams['mysqlAmfUserId'] +
                                " --password='" + sqlParams['mysqlAmfPass'] +
                                "' --port=" + sqlParams['mysqlAmfJdbcPort'] + " --database=" + sqlParams['mysqlAmfDbname'] + " --enable-cleartext-plugin"
                                " < " + mysqlAmfScriptPath + "/TruncateMysql.sql")
  if standInLoadFlag !=0:
   sys.exit("Error while truncating the table")
 except Exception as e:
  sys.exit("ERROR: while truncating the mysql pty table:" + str(e))
  
 ## Loading mysql dlz table with protegrity tokenisation
def load_tc360_mysql_pty(mysqlPtyDf,jdbcUrl,mySQLDriver,mysqlTc360Tbl,mysqlAmfUserId,mysqlAmfPassword):
 try:  
     print("Loading C360 attribute MYSQL Pty table: Starts")              
     mysqlPtyDf.write.format('jdbc').\
                     options(
                             url=jdbcUrl,
                             driver=mySQLDriver,
                             dbtable=mysqlTc360Tbl,
                             user=mysqlAmfUserId,
                             password=mysqlAmfPassword).\
                             option("truncate","true").\
                             option("fetchsize","10000").\
                             option("batchsize","10000").\
                             option("serverTimezone","America/New_York").\
                             mode('overwrite').\
                             save()
     print("Total record loaded into mysql pty table is: "+ str(mysqlPtyDf.count()))
     print("Loading C360 attribute mysql pty table: Complete")
     print("###########################################################################################################################")
 except Exception as e:
  sys.exit("ERROR: while loading mysql pty table:" + str(e))
 
## Defining main function for the pyspark script
def main():
 configFileName, sqlOpts = validateArg()
 sqlParams, configParams, runDay = parseSQLOpts(sqlOpts)
 inputParams = validateConfigFile(configFileName)
 spark = createSparkSession(inputParams['protegrityJar'],inputParams['mySQLJarFile'],inputParams['mySQLAesJar'],inputParams['sparkJar'])
 spark.sparkContext.setLogLevel("ERROR")
 sqlParams = parseJceksFile(spark, sqlParams, configParams['mySQLJCEKSPath'])
 jdbcUrl = buildJdbcUrl(sqlParams['mysqlAmfHostname'], sqlParams['mysqlAmfJdbcPort'], sqlParams['mysqlAmfDbname'],sqlParams['mysqlAmfUserId'], sqlParams['mysqlAmfPass'])
 tc360AcctDf = readTc360Account(inputParams['extPathAcct'],inputParams['c360AttrHiveLdrQry'],runDay,spark)
 mysqlPtyDf = createLRDf(tc360AcctDf)
 truncatePtyMySql(sqlParams,configParams['mysqlAmfScriptPath'])
 load_tc360_mysql_pty(mysqlPtyDf,jdbcUrl,inputParams['mySQLDriver'],inputParams['mysqlTc360Tbl'],sqlParams['mysqlAmfUserId'],sqlParams['mysqlAmfPass'])
  
if __name__ == "__main__":
    main()