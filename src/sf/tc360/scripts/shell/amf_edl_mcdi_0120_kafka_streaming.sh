#!/bin/bash

###########################################################
# Script Name: amf_edl_tc360_fct_attr_ldr.sh              #
# Purpose : To load mcdi 0120 kafka message to edl        #
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

curr_date=`date '+%m%d%Y'`
curr_time=`date '+%H:%M:%S'`
this_script_name=$(readlink --canonicalize --no-newline $BASH_SOURCE)
shell_wrapper_kafka_log=${log_file_dir}/"amf_edl_mcdi_0120_kafka_streaming-"${curr_date}"-"${curr_time}"".log

trigger_spark_submit_kafka_ingestion_script()
{
$spark_submit \
--master yarn \
--deploy-mode cluster \
--queue $tez_queue \
 $spark_resources_mcdi0120 \
 ${python_spark_dir}/amtf_mcdi0120_kafka_streaming.py
}

command_status_chk_func(){
 status=$1
 message=$2
 if [ $status -eq 0 ]
 then
     echo "INFO" -s "$this_script_name" -m "Success - $message"
     send_email -t "${rcpt_mcdi0120msg_kafka}" -c "${cc_rcpt_mcdi0120msg_kafka}" -s "${subject_mcdi0120msg_kafka_success}" -b "${body_mcdi0120msg_kafka_success}"
 else
     echo "ERROR:" -s "$this_script_name" -m "Error - $message"
	 send_email -t "${rcpt_mcdi0120msg_kafka}" -c "${cc_rcpt_mcdi0120msg_kafka}" -s "${subject_mcdi0120msg_kafka_fail}" -b "${body_mcdi0120msg_kafka_fail}"
   exit 1
 fi
 }

mkdir -p ${shell_wrapper_kafka_log}
# Trigger mcdi 0120 kafka streaming job
echo "Starting kafka mdci 0120 streaming job....." >> ${shell_wrapper_kafka_log}
echo "###########################################################################"
trigger_spark_submit_kafka_ingestion_script
command_status_chk_func $?
echo "##################################################################################" >> ${shell_wrapper_kafka_log}
echo "Finished running the mcdi 0120 kafka streaming job....."
