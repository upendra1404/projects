#!/bin/bash

########################################################
# Script Name: amf_edl_one_time_mysql_ddl.sh           #
# Purpose : To create mysql tables for c360 atributes  #
# Created By: Upendra Kumar                            #
# Date Created : 03-Sept-2020                          #
# Date Last Modified :                                 #
########################################################

echo "Start running the c360 attribute mysql onetime table creation shell script#######################" 
## Creating the log directory
mkdir -p ${log_file_dir}
log_time=`date '+%Y%m%d%H%m%s'`
start_time=`date '+%Y-%m-%d %H:%M:%S'` 
echo "Starting creating the local log file directory and c360 attributes mysql tables at ${start_time}" >> ${log_file_dir}/amf_edl_one_time_mysql_ddl_${log_time}.log
echo "################################################################################################################################################################"
## Defining the function for creating loacl log directory
function create_local_directory()
{
 dir_name=$1
 ls ${dir_name}
 if [ $? -ne 0 ]
  then
   mkdir -p ${dir_name}
    if [ $? -eq 0 ]
	  then
	    echo "Successfully created local log directory: ${dir_name}"
	else
	    echo "ERROR: Failed creating directory:${dir_name}"
	fi
else
    echo "Skipping creating a directory: ${dir_name} already exists"
fi
}
## Defining a function for running ddl sql script to create mysql pty tables
function create_c360_mysql_pty_tables()
{
sql_file=$1
mysql --host=${hostPty} --user=${user} --password=${password} --port=${port} --database=${databasePty} --enable-cleartext-plugin < ${sql_file}
if [ $? -eq 0 ]
then
echo "Tables created successfully" >> ${log_file_dir}/amf_edl_one_time_mysql_ddl_${log_time}.log
else
echo "Something went wrong!!. Please check the log for more details...."
exit 99
fi
}
## Defining a function for running ddl sql script to create mysql aes tables
function create_c360_mysql_aes_tables()
{
sql_file=$1
mysql --host=${hostAes} --user=${user} --password=${password} --port=${port} --database=${databaseAes} --enable-cleartext-plugin < ${sql_file}
if [ $? -eq 0 ]
then
echo "Tables created successfully" >> ${log_file_dir}/amf_edl_one_time_mysql_ddl_${log_time}.log
else
echo "Something went wrong!!. Please check the log for more details...."
exit 99
fi
}
## Calling create local log directory function
create_local_directory ${log_file_dir} >> ${log_file_dir}/amf_edl_one_time_mysql_ddl_${log_time}.log
echo "End creating local log directory" >> ${log_file_dir}/amf_edl_one_time_mysql_ddl_${log_time}.log
## Calling the create c360 mysql ddl function
create_c360_mysql_pty_tables ${mysql_script_dir}/createC360MySqlPtyTables.sql >> ${log_file_dir}/amf_edl_one_time_mysql_ddl_${log_time}.log
create_c360_mysql_aes_tables ${mysql_script_dir}/createC360MySqlAesTables.sql >> ${log_file_dir}/amf_edl_one_time_mysql_ddl_${log_time}.log
echo "End creating c360 attributes mysql tables" >> ${log_file_dir}/amf_edl_one_time_mysql_ddl_${log_time}.log
echo "Finished running the c360 attribute mysql onetime table creation shell script#######################"


