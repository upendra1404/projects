#!/bin/bash

###########################################################
# Script Name: amf_edl_c360_attr_feature_ldr.sh           #
# Purpose : To run the c360 attribute load job            #
# Created By: Upendra  Kumar                              #
# Date Created : 21-Oct-2020                              #
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
echo "Starting the data flow process for Account Management Fraud C360 Attribute load process at ${start_time}" > ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
echo "##################################################################################"

#Step-3 - HIVE CSPA Load Ready Table
#Calculating Start Date Time
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "Step-3: Starting C360 Attribute HIVE Load Ready Table Load ${start_time}" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
echo "Step-3: Executing C360 Attribute HIVE Load Ready Table Load script..." >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
#Trigger HIVE Load Ready Table Script
spark-submit ${deploy_mode_c360_attributes} ${spark_resources_c360_attributes} ${python_spark_dir}/amf_c360_attributes_script.py -f ${config_file_dir}/${config_file}>>${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log

#Check the status of spark-submit command
if [ $? -ne 0 ]
 then 
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: End c360 Attribute Data Load at ${end_time}" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
    echo "Step-3: c360 Attribute Data Load Table script failed. Please refer to log for more details" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
    send_email -t "${recipient_c360_attr_ldr}" -c "${cc_c360_attr_ldr}" -s "${subject_c360_attr_ldr_fail}" -b "${body_c360_attr_ldr_fail}" -a "${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
         exit 999
    fi
    exit 99
    exit 99
else
    #Calculating End Date Time
    end_time=`date '+%Y-%m-%d %H:%M:%S'`
    echo "Step-3: c360 Attribute Data Load script at ${end_time}" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
    echo "Step-3: c360 Attribute Data Load script completed successfully" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
    send_email -t "${recipient_c360_attr_ldr}" -c "${cc_c360_attr_ldr}" -s "${subject_c360_attr_ldr_success}" -b "${body_c360_attr_ldr_success}" -a "${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log"
    if [ $? -eq 0 ]
        then
         echo "Email notification send successfully" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
    else 
         echo "Email notification failed" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log
         exit 999
    fi
fi
echo "##################################################################################" >> ${log_file_dir}/amf_service_hive_c360_attr_ldr_${log_time}.log

