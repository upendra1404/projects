#!/bin/bash

###########################################################
# Script Name: amtfServiceGibberish.sh                    #
# Purpose : To trigger gibberish score script             #
# Created By: Upendra  Kumar                              #
# Date Created : 29-Sept-2020                             #
# Date Last Modified :                                    #
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
echo "Starting the data flow process for Account Management Fraud new customer tc360 attribute load process at ${start_time}" > ${log_file_dir}/amf_service_gibberish_${log_time}.log
echo "##################################################################################"

#Step-3 - HIVE CSPA Load Ready Table
#Calculating Start Date Time
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "Step-3: Starting gibberish score Load ${start_time}" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
echo "Step-3: Executing gibberish score load script..." >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
#Trigger HIVE Load Ready Table Script
spark-submit ${deploy_mode_tc360_attributes} ${spark_resources_tc360_attributes} ${python_spark_dir}/amtfServiceGibberish.py -f ${config_file_dir}/${config_file} -r ${runDay}>>${log_file_dir}/amf_service_gibberish_${log_time}.log

#Check the status of spark-submit command
if [ $? -ne 0 ]
 then 
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: End gibberish score Data Load at ${end_time}" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
    echo "Step-3: Gibberish score load table script failed. Please refer to log for more details" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
    send_email -t "${recipient_gibberish_score}" -c "${cc_recipient_gibberish_score}" -s "${subject_recipient_gibberish_score_fail}" -b "${body_gibberish_score_fail}" -a "${log_file_dir}/amf_service_gibberish_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
         exit 999
    fi
    exit 99
    exit 99
else
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: Gibberish score load script at ${end_time}" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
    echo "Step-3: Gibberish score load script completed successfully" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
    send_email -t "${recipient_gibberish_score}" -c "${cc_recipient_gibberish_score}" -s "${subject_gibberish_score_success}" -b "${body_gibberish_score_success}" -a "${log_file_dir}/amf_service_gibberish_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log
         exit 999
    fi
fi
echo "##################################################################################" >> ${log_file_dir}/amf_service_gibberish_${log_time}.log

