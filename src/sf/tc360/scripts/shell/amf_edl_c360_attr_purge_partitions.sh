#!/bin/bash

########################################################################
# Script Name: amf_edl_c360_attr_feature_purge_partitions.sh           #
# Purpose : To trigger c360 attribute purge partitions pyspark script  #
# Created By: Upendra  Kumar                                           #
# Date Created : 06-Jan-2021                                           #
# Date Last Modified :                                                 #
########################################################################

#Setting up email notification
if [ -f ${edlcommon_shell_libraries}/send_email.sh ]
    then 
        . ${edlcommon_shell_libraries}/send_email.sh
else 
    echo "ERROR: The ${edlcommon_shell_libraries}/send_email.sh is missing"
    exit 99    
fi

log_time=`date '+%Y%m%d%H%M%S'`
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "Starting the data flow process for Account Management Fraud C360 Attribute purge partitions process at ${start_time}" > ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
echo "##################################################################################"

#Step-3 - HIVE CSPA Load Ready Table
#Calculating Start Date Time
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "Step-3: Starting C360 Attribute purge partitions process at ${start_time}" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
echo "Step-3: Executing C360 Attribute purge partitions script..." >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
#Trigger HIVE Load Ready Table Script
spark-submit ${deploy_mode_c360_attributes} ${spark_resources_c360_attributes} ${python_spark_dir}/amfServiceC360PurgePartitions.py -f ${config_file_dir}/${config_file}>>${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log

#Check the status of spark-submit command
if [ $? -ne 0 ]
 then 
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: End c360 Attribute purge partitions process at ${end_time}" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
    echo "Step-3: c360 Attribute purge partitions script failed. Please refer to log for more details" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
    send_email -t "${recipient_c360_attr_purge_partitions}" -c "${cc_c360_attr_purge_partitions}" -s "${subject_c360_attr_purge_partitions_fail}" -b "${body_c360_attr_purge_partitions_fail}" -a "${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
         exit 999
    fi
    exit 99
    exit 99
else
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: c360 Attribute purge partitions process at ${end_time}" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
    echo "Step-3: c360 Attribute purge partitions process completed successfully" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
    send_email -t "${recipient_c360_attr_purge_partitions}" -c "${cc_c360_attr_purge_partitions}" -s "${subject_c360_attr_purge_partitions_success}" -b "${body_c360_attr_purge_partitions_success}" -a "${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log
         exit 999
    fi
fi
echo "##################################################################################" >> ${log_file_dir}/amf_service_hive_c360_attr_purge_partitions_${log_time}.log

