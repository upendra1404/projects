##########################################################################################
# Purpose: Read c360 attributes hive table and load into mysql aes table                 #
# Created by: Upendra Kumar                                                              #
# Created on: 30-Oct-2020                                                                #
# Modified on :                                                                          #
##########################################################################################

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

## Validate a list of arguments passed as part of input for spark-submit command 
def validateArg():
    mysqlAmfHostname = None
    mysqlAmfDbname = None
    mysqlAmfJdbcPort = None
    mysqlAmfUserId = None
    mysqlAmfPass = None
    mysqlAmfScriptPath = None
    mySQLJCEKSPath = None    
    printError = "spark-submit script_name.py -f <config_file_path>/<config_file_name> -h mysqlHostname -d mysqlDatabaseName -n $<mysql_amf_jdbc_port> -u $<mysql_amf_user_id> -p $<mysql_amf_pass> -m $<mysql_script_dir> -j $<mysql_amf_jceks>"
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:h:d:n:u:p:m:j:")
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
        sys.exit("ERROR: MYSQL Script Path not provided with argument -j")

    return configFileName, sqlOpts

## PArsing the mysql attributes
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

    sqlParams = {"mysqlAmfHostname": mysqlAmfHostname,
                 "mysqlAmfDbname": mysqlAmfDbname,
                 "mysqlAmfJdbcPort": mysqlAmfJdbcPort,
                 "mysqlAmfUserId": mysqlAmfUserId,
                 "mysqlAmfPass": mysqlAmfPass}

    sqlFiles = {"mysqlAmfScriptPath": mysqlAmfScriptPath,
                 "mySQLJCEKSPath": mySQLJCEKSPath}
    return sqlParams, sqlFiles

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
            if('edl_amf_c360_hive_ldr' not in config.options('HIVE')):
                sys.exit("ERROR: Section MYSQL doesn't contain edl_amf_c360_hive_ldr")
            else:
                edl_amf_c360_hive_ldr = config.get('HIVE','edl_amf_c360_hive_ldr')
            if('aesC360AttrMYSQLTable' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain aesC360AttrMYSQLTable")
            else:
                aesC360AttrMYSQLTable = config.get('MYSQL','aesC360AttrMYSQLTable')
            if('c360AttrPtyDetokQry' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain c360AttrPtyDetokQry")
            else:
                c360AttrPtyDetokQry = config.get('MYSQL','c360AttrPtyDetokQry')
            if('c360AttrAesTokQry' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain c360AttrAesTokQry")
            else:
                c360AttrAesTokQry = config.get('MYSQL','c360AttrAesTokQry')
            if('devAesMysqlTable' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain devAesMysqlTable")
            else:
                devAesMysqlTable = config.get('MYSQL','devAesMysqlTable')
            if('mysqlAesDiffTable' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain mysqlAesDiffTable")
            else:
                mysqlAesDiffTable = config.get('MYSQL','mysqlAesDiffTable')
            if('mySQLAesJar' not in config.options('MYSQL')):
                sys.exit("ERROR: Section MYSQL doesn't contain mySQLAesJar")
            else:
                mySQLAesJar = config.get('MYSQL','mySQLAesJar')
            if('sparkJar' not in config.options('FUNCTIONAL')):
                sys.exit("ERROR: Section MYSQL doesn't contain sparkJar")
            else:
                sparkJar = config.get('FUNCTIONAL','sparkJar')
            if('c360AmfQA' not in config.options('HIVE')):
                sys.exit("ERROR: Section MYSQL doesn't contain c360AmfQA")
            else:
                c360AmfQA = config.get('HIVE','c360AmfQA')
                
    print("Config File Successfully Validated")
    print("##################################################################################")
    
    return {'readMode': readMode,
            'mySQLJarFile': mySQLJarFile,
            'mySQLDriver': mySQLDriver,
            'protegrityJar': protegrityJar,
            'edl_amf_c360_hive_ldr': edl_amf_c360_hive_ldr,
            'devAesMysqlTable': devAesMysqlTable,
            'mysqlAesDiffTable': mysqlAesDiffTable,
            'aesC360AttrMYSQLTable': aesC360AttrMYSQLTable,
            'c360AttrPtyDetokQry': c360AttrPtyDetokQry,
            'c360AttrAesTokQry': c360AttrAesTokQry,
            'mySQLAesJar': mySQLAesJar,
            'sparkJar': sparkJar,
            'c360AmfQA': c360AmfQA
            }

## Forming mysql connection string
def buildJdbcUrl(hostname, jdbcPort, dbName,mySQLUser, mySQLPass):
    print("##################################################################################")
    print("Creating MYSQL JDBC URL")

    jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}&rewriteBatchedStatements=true".format(hostname, jdbcPort, dbName, mySQLUser, mySQLPass)

    print("MYSQL JDBC URL Created")
    print("##################################################################################")
    return jdbcUrl
    
### create spark session
def createSparkSession(protegrityJar,mySQLJarFile,mySQLAesJar,sparkJar):
 spark = SparkSession.builder.\
                     enableHiveSupport().appName('dlzC360AttrMYSQLLoad').getOrCreate()
 spark.sql("SET hive.mapred.supports.subdirectories=true")
 spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
 status,protegrityFile = commands.getstatusoutput("ls " + protegrityJar + "/" + "pephive-*.jar")
 spark.sql("add jar " + protegrityFile)
 spark.sql("add jar " + sparkJar)
 spark.sql("add jar " + mySQLJarFile)
 spark.sql("add jar " + mySQLAesJar)
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'")
 sqlContext = SQLContext(spark.sparkContext)
 sqlContext.registerJavaFunction("encrypt", "com.syf.edl.services.creditservice.crypto.udf.EncrypUDF")
 sqlContext.registerJavaFunction("decrypt", "com.syf.edl.services.creditservice.crypto.udf.DecryptUDF")
 return spark

## Create hive context
def readHiveContext(spark):
 hive = HiveContext(spark)
 return hive

## Reading hive table for c360 attributes and forming a mysql msi load ready DF
def readC360AttrHiveTable(c360AmfQA,c360AttrPtyDetokQry,c360AttrAesTokQry,mySQLDriver,jdbcUrl,mySQLUser,mySQLPass,spark):
 print("#########################################################################################################################")
 print("Reading c360 hive table: Starts")
 hive = readHiveContext(spark)
 ## Reading c360 hive table for most recent partition
 spark.read.parquet(c360AmfQA).createOrReplaceTempView("readHivePartitions")
 hive.sql("""select * from readHivePartitions where as_of_date = (select max(as_of_date) from readHivePartitions)""").createOrReplaceTempView("readMostRecentPartition")
 notNullHiveDF = spark.sql("""select * from readMostRecentPartition where account_number is not null or trim(account_number) <> ''""")
 nullHiveDF = spark.sql("""select * from readMostRecentPartition where account_number is null or trim(account_number) = '' limit 20""")
 ## Calling function to find the duplicates
 print("Finding duplicate records: Starts")
 duplicateDF = findDuplicates(notNullHiveDF,spark)
 if (len(duplicateDF.head(1)) > 0):
     print("Duplicate record found in c360 attributes hive table !!!")
     duplicateDF.select('lexid','account_number','count').show()
     print("Displaying first top 20 duplicate records")
     notNullHiveDF = notNullHiveDF.dropDuplicates()
 else:
      print("No Duplicates found !!!")
 notNullHiveDF.createOrReplaceTempView("notNullHiveDF_vw")
 print("Finding duplicate records: Complete")
 ## Detokenizing account number from protegrity tokenization
 dlzDetokQry = spark.sql(c360AttrPtyDetokQry + """ from notNullHiveDF_vw""").createOrReplaceTempView("dlzDetokQry_vw") 
 ## Tokenizing the account number with AES tokenizaton
 dlzAESTokDF = spark.sql(c360AttrAesTokQry + """ from dlzDetokQry_vw""").withColumn('load_ts',lit(current_timestamp()))
 print("Finding null account numbers: Starts")
 if (len(nullHiveDF.head(1)) > 0):
     print("Total records with null account number: " + str(nullHiveDF.count()))
     nullHiveDF.select('lexid','account_number').show()
     print("Displaying first top 20 records with null account numbers")
 else:
     print("No Null account number found !!!") 
 print("Finding null account numbers: Complete")
 print("Record read from c360 attribute hive with not null account numbers :" + str(dlzAESTokDF.count()))
 print("Reading c360 hive table: Complete")
 print("###########################################################################################################################")
 return dlzAESTokDF
 
def findDuplicates(duplicateDF,spark):
 duplicateDF.createOrReplaceTempView("duplicateDF_vw")
 duplicateDF = spark.sql("""select lexid,account_number,count(*) as count 
                            from duplicateDF_vw group by lexid,account_number
                            having count(*) > 1 limit 20""")
 return duplicateDF
 
## Loading mysql msi table for c360 attributes 
def loadC360MsiMysqlTable(msiC360ldrDF,jdbcUrl,mySQLDriver,aesC360AttrMYSQLTable,mysqlAmfUserId,mysqlAmfPassword):
 print("Loading C360 attribute MYSQL AES table: Starts")              
 msiC360ldrDF.write.format('jdbc').\
                     options(
                             url=jdbcUrl,
                             driver=mySQLDriver,
                             dbtable=aesC360AttrMYSQLTable,
                             user=mysqlAmfUserId,
                             password=mysqlAmfPassword).\
                             option("truncate","true").\
                             option("fetchsize","10000").\
                             option("batchsize","10000").\
                             option("serverTimezone","America/New_York").\
                             mode('overwrite').\
                             save()
 print("Total record loaded into AES mysql table is: "+ str(msiC360ldrDF.count()))
 print("Loading C360 attribute MYSQL AES table: Complete")
 print("##########################################################################################################")
 
## Finding differences between mysql msi qa and dev tables and loading the differneces into diff table
def findDiff(aesC360AttrMYSQLTable,devAesMysqlTable,jdbcUrl,mySQLDriver,mysqlAmfUserId,mysqlAmfPassword,spark):
 print("Finding the differences between qa and dev scripts output: Starts")
 ## Reading the qa and dev tables data for comparison
 qaC360MysqlMsi = spark.read.format('jdbc').\
                     option("driver",mySQLDriver).\
                     option("url",jdbcUrl).\
                     option("username",mysqlAmfUserId).\
                     option("password",mysqlAmfPassword).\
                     option("dbtable",aesC360AttrMYSQLTable).\
                     option("serverTimezone","America/New_York").\
                     load()
 qaC360MysqlMsi = qaC360MysqlMsi.withColumn('qa_count',lit(str(qaC360MysqlMsi.count())))
 qaC360MysqlMsi.createOrReplaceTempView("qaC360MysqlMsi_vw")
 devC360MysqlMsi = spark.read.format('jdbc').\
                     option("driver",mySQLDriver).\
                     option("url",jdbcUrl).\
                     option("username",mysqlAmfUserId).\
                     option("password",mysqlAmfPassword).\
                     option("dbtable",devAesMysqlTable).\
                     option("serverTimezone","America/New_York").\
                     load()
 devC360MysqlMsi = devC360MysqlMsi.withColumn('dv_count',lit(str(devC360MysqlMsi.count())))
 devC360MysqlMsi.createOrReplaceTempView("devC360MysqlMsi_vw")
 ## Comparing the mysql qa and dev msi tables.......
 ## Taking union of two data sets in order to apply full outer join
 diffDF = spark.sql("""select a.lexid as q_lx_id,
                             a.account_number as q_acc_no,
                             b.lexid as dv_lx_id,
                             b.account_number as dv_acc_no,
            concat(cast(a.tran_amt_mean_last_30_days as char(255)),'|',
                       cast(a.tran_amt_std_last_30_days as char(255)),'|',
                       cast(a.tran_cnt_per_day_mean_last_30_days as char(255)),'|',
                       cast(a.tran_cnt_per_day_std_last_30_days as char(255)),'|',
                       cast(a.tran_cnt_total_last_30_days as char(255)),'|',
                       cast(a.tran_cnt_prev_day as char(255)),'|',
                       cast(a.foreign_tran_prev_day as char(1)),'|',
                       qa_count) as q_c360_attributes,
            concat(cast(b.tran_amt_mean_last_30_days as char(255)),'|',
                       cast(b.tran_amt_std_last_30_days as char(255)),'|',
                       cast(b.tran_cnt_per_day_mean_last_30_days as char(255)),'|',
                       cast(b.tran_cnt_per_day_std_last_30_days as char(255)),'|',
                       cast(b.tran_cnt_total_last_30_days as char(255)),'|',
                       cast(b.tran_cnt_prev_day as char(255)),'|',
                       cast(b.foreign_tran_prev_day as char(1)),'|',
                       dv_count) as dv_c360_attributes
            from qaC360MysqlMsi_vw a left join devC360MysqlMsi_vw b
                      on (trim(a.lexid)=trim(b.lexid) and trim(a.account_number)=trim(b.account_number))
                      where (coalesce(a.tran_amt_mean_last_30_days,0) <> coalesce(b.tran_amt_mean_last_30_days,0) or
                             coalesce(a.tran_amt_std_last_30_days,0) <> coalesce(b.tran_amt_std_last_30_days,0) or
                             coalesce(a.tran_cnt_per_day_mean_last_30_days,0) <> coalesce(b.tran_cnt_per_day_mean_last_30_days,0) or
                             coalesce(a.tran_cnt_per_day_std_last_30_days,0) <> coalesce(b.tran_cnt_per_day_std_last_30_days,0) or
                             coalesce(a.tran_cnt_total_last_30_days,0) <> coalesce(b.tran_cnt_total_last_30_days,0) or
                             coalesce(a.tran_cnt_prev_day,0) <> coalesce(b.tran_cnt_prev_day,0) or
                             coalesce(a.foreign_tran_prev_day,'') <> coalesce(b.foreign_tran_prev_day,''))
            union
                      select b.lexid as q_lx_id,
                             b.account_number as q_acc_no,
                             a.lexid as dv_lx_id,
                             a.account_number as dv_acc_no,
            concat(cast(b.tran_amt_mean_last_30_days as char(255)),'|',
                       cast(b.tran_amt_std_last_30_days as char(255)),'|',
                       cast(b.tran_cnt_per_day_mean_last_30_days as char(255)),'|',
                       cast(b.tran_cnt_per_day_std_last_30_days as char(255)),'|',
                       cast(b.tran_cnt_total_last_30_days as char(255)),'|',
                       cast(b.tran_cnt_prev_day as char(255)),'|',
                       cast(b.foreign_tran_prev_day as char(1)),'|',
                       qa_count) as q_c360_attributes,
            concat(cast(a.tran_amt_mean_last_30_days as char(255)),'|',
                       cast(a.tran_amt_std_last_30_days as char(255)),'|',
                       cast(a.tran_cnt_per_day_mean_last_30_days as char(255)),'|',
                       cast(a.tran_cnt_per_day_std_last_30_days as char(255)),'|',
                       cast(a.tran_cnt_total_last_30_days as char(255)),'|',
                       cast(a.tran_cnt_prev_day as char(255)),'|',
                       cast(a.foreign_tran_prev_day as char(1)),'|',
                       dv_count) as dv_c360_attributes
            from devC360MysqlMsi_vw a left join qaC360MysqlMsi_vw b
                      on (trim(a.lexid)=trim(b.lexid) and trim(a.account_number)=trim(b.account_number))
                      where (coalesce(a.tran_amt_mean_last_30_days,0) <> coalesce(b.tran_amt_mean_last_30_days,0) or
                             coalesce(a.tran_amt_std_last_30_days,0) <> coalesce(b.tran_amt_std_last_30_days,0) or
                             coalesce(a.tran_cnt_per_day_mean_last_30_days,0) <> coalesce(b.tran_cnt_per_day_mean_last_30_days,0) or
                             coalesce(a.tran_cnt_per_day_std_last_30_days,0) <> coalesce(b.tran_cnt_per_day_std_last_30_days,0) or
                             coalesce(a.tran_cnt_total_last_30_days,0) <> coalesce(b.tran_cnt_total_last_30_days,0) or
                             coalesce(a.tran_cnt_prev_day,0) <> coalesce(b.tran_cnt_prev_day,0) or
                             coalesce(a.foreign_tran_prev_day,'') <> coalesce(b.foreign_tran_prev_day,''))
                      """).withColumn('load_ts',lit(current_timestamp()))
 print("Total differences between qa: " + aesC360AttrMYSQLTable + " and dev: " + devAesMysqlTable + " scripts output are: " + str(diffDF.count()))
 print("Finding the differences between qa and dev scripts output: Complete")
 print("#################################################################################################")
 return diffDF
 
def loadDiff(diffDF,mysqlAesDiffTable,jdbcUrl,mySQLDriver,mysqlAmfUserId,mysqlAmfPassword):
 print("Loading differences into mysql aes diff table: Starts")
 diffDF.write.format('jdbc').\
                     options(
                             url=jdbcUrl,
                             driver=mySQLDriver,
                             dbtable=mysqlAesDiffTable,
                             user=mysqlAmfUserId,
                             password=mysqlAmfPassword).\
                             option("truncate","true").\
                             option("fetchsize","10000").\
                             option("batchsize","10000").\
                             option("serverTimezone","America/New_York").\
                             mode('overwrite').\
                             save()
 print("Loading differences into mysql aes diff table: Complete")
 print("#################################################################################################")


## Define main funtion for pyspark script
def main():
 configFileName, sqlOpts = validateArg()
 sqlParams,sqlFiles = parseSQLOpts(sqlOpts)
 inputParams = validateConfigFile(configFileName)
 spark = createSparkSession(inputParams['protegrityJar'],inputParams['mySQLJarFile'],inputParams['mySQLAesJar'],inputParams['sparkJar'])
 spark.sparkContext.setLogLevel("ERROR")
 sqlParams = parseJceksFile(spark, sqlParams, sqlFiles['mySQLJCEKSPath'])
 jdbcUrl = buildJdbcUrl(sqlParams['mysqlAmfHostname'], sqlParams['mysqlAmfJdbcPort'], sqlParams['mysqlAmfDbname'],sqlParams['mysqlAmfUserId'], sqlParams['mysqlAmfPass'])
 hive = readHiveContext(spark)
 dlzAESTokDF = readC360AttrHiveTable(inputParams['c360AmfQA'],inputParams['c360AttrPtyDetokQry'],inputParams['c360AttrAesTokQry'],inputParams['mySQLDriver'],jdbcUrl,sqlParams['mysqlAmfUserId'],sqlParams['mysqlAmfPass'],spark)
 loadC360MsiMysqlTable(dlzAESTokDF,jdbcUrl,inputParams['mySQLDriver'],inputParams['aesC360AttrMYSQLTable'],sqlParams['mysqlAmfUserId'],sqlParams['mysqlAmfPass'])
 diffDF = findDiff(inputParams['aesC360AttrMYSQLTable'],inputParams['devAesMysqlTable'],jdbcUrl,inputParams['mySQLDriver'],sqlParams['mysqlAmfUserId'],sqlParams['mysqlAmfPass'],spark)
 loadDiff(diffDF,inputParams['mysqlAesDiffTable'],jdbcUrl,inputParams['mySQLDriver'],sqlParams['mysqlAmfUserId'],sqlParams['mysqlAmfPass'])
 
if __name__ == "__main__":
    main()