#!/bin/bash

###########################################################
# Script Name: amf_edl_tc360_timeboxed_attr_falcon_load.sh #
# Purpose : To trigger falcon attribute load python script #
# Created By: Upendra  Kumar                               #
# Date Created : 21-Sep-2021                               #
# Date Last Modified :                                     #
###########################################################

runDay=$1
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
# Passing the date arguments
if [ $# -eq 0 ]
  then
     runDay=`date -d 'today' '+%Y-%m-%d'`
else
     runDay=$runDay
fi
echo "Starting the data flow process for Account Management Fraud new tc360 timeboxed attribute load process at ${start_time}" > ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
echo "##################################################################################"

#Step-3 - HIVE CSPA Load Ready Table
#Calculating Start Date Time
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "Step-3: Starting new tc360 timeboxed attribute HIVE Load Ready Table Load ${start_time}" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
echo "Step-3: Executing new tc360 timeboxed attribute HIVE Load Ready Table Load script..." >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
#Trigger HIVE Load Ready Table Script
spark-submit ${deploy_mode_tc360_attributes_onetime} ${spark_resources_tc360_attributes_onetime} ${python_spark_dir}/amtfServiceTc360TimeBoxedAttrLoad.py -f ${config_file_dir}/${config_file} -r ${runDay} >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log

#Check the status of spark-submit command
if [ $? -ne 0 ]
 then 
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: End new tc360 timeboxed attribute Data Load at ${end_time}" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
    echo "Step-3: new tc360 timeboxed Attribute Data Load Table script failed. Please refer to log for more details" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
    send_email -t "${recipient_tc360_attr_timeboxed}" -c "${cc_recipient_tc360_attr_timeboxed}" -s "${subject_recipient_tc360_attr_timeboxed_fail}" -b "${body_tc360_attr_timeboxed_fail}" -a "${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
         exit 999
    fi
    exit 99
    exit 99
else
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: new tc360 timeboxed Attribute Data Load script at ${end_time}" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
    echo "Step-3: new tc360 timeboxed Attribute Data Load script completed successfully" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
    send_email -t "${recipient_tc360_attr_timeboxed}" -c "${cc_recipient_tc360_attr_timeboxed}" -s "${subject_tc360_attr_timeboxed_success}" -b "${body_tc360_attr_timeboxed_success}" -a "${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log
         exit 999
    fi
fi
echo "##################################################################################" >> ${log_file_dir}/amf_service_hive_timeboxed_attr_${log_time}.log

