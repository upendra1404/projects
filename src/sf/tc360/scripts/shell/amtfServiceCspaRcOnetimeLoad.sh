#!/bin/bash

###########################################################
# Script Name: amf_edl_cspa_rc_hive_ldr.sh                #
# Purpose : To load cspa rc one time view                 #
# Created By: Upendra  Kumar                              #
# Date Created : 15-May-2021                              #
# Date Last Modified :                                    #
###########################################################

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
echo "Starting the data flow process of cspa rc onetime load for Account Management Fraud process at ${start_time}" > ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
echo "##################################################################################"

#Step-3 - HIVE CSPA Load Ready Table
#Calculating Start Date Time
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "Step-3: Starting cspa rc one time hive table load at ${start_time}" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
echo "Step-3: Executing cspa rc one time script..." >> ${log_file_dir}/amf_service_hive_cspa_rc_load__${log_time}.log
#Trigger HIVE Load Ready Table Script
spark-submit ${deploy_mode_c360_attributes} ${spark_resources_c360_attributes} ${python_spark_dir}/amtfServiceCspaRcOnetimeLoad.py -f ${config_file_dir}/${config_file}>>${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log

#Check the status of spark-submit command
if [ $? -ne 0 ]
 then 
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: End cspa rc one time load at ${end_time}" >> ${log_file_dir}/amf_service_hive_new_tc360_attr_cust_${log_time}.log
    echo "Step-3: cspa rc one time load script failed. Please refer to log for more details" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
    send_email -t "${recipient_cspa_rc_load}" -c "${cc_recipient_cspa_rc_load}" -s "${subject_recipient_cspa_rc_load_fail}" -b "${body_cspa_rc_load_fail}" -a "${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
         exit 999
    fi
    exit 99
    exit 99
else
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: cspa rc one time load script at ${end_time}" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
    echo "Step-3: cspa rc one time load script completed successfully" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
    send_email -t "${recipient_cspa_rc_load}" -c "${cc_recipient_cspa_rc_load}" -s "${subject_cspa_rc_load_success}" -b "${body_cspa_rc_load_success}" -a "${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log
         exit 999
    fi
fi
echo "##################################################################################" >> ${log_file_dir}/amf_service_hive_cspa_rc_load_${log_time}.log

