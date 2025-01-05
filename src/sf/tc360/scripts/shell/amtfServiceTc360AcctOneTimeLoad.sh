#!/bin/bash

project_dir=/data1/EDL/domains/amf_qa/edl_amf_qa
. $project_dir/set_env.env
echo ""
###########################################################
# Script Name: amtfTc360AcctOneTimeLoad.sh                #
# Purpose : To run the c360 attribute load job            #
# Created By: Upendra  Kumar                              #
# Date Created : 21-Oct-2020                              #
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
# Passing the date arguments
if [ $# -eq 0 ]
  then
     runDay=`date -d 'today' '+%Y-%m-%d'`
else
     runDay=$runDay
fi
declare -a email_array_string=("${emailLAXref}" "${emailFcc}" "${emailCust}" "${emailBin6}" "${emailFalcon}" "${emailAOTL}")
#Function definition to trigger multi pyspark scripts
function trigger_multi_scripts(){
declare -a acctOTLArr=($1)
declare -a logAOTL=($2)
shift 2
declare -a emailArr=("$@")
echo "Total elements in pyspark array: ${#acctOTLArr[@]}"
echo "Total elements in email array: ${#emailArr[@]}"
for ((i=0;i<${#acctOTLArr[@]};i++))
do 
 start_time=`date '+%Y-%m-%d %H:%M:%S'` ## Initialising start date Time
 log_time=`date '+%Y%m%d%H%M%S'` ## Initialising log date time
 echo "Started running pyspark script ${acctOTLArr[$i]} at : ${start_time}" > "${log_file_dir}"/"${logAOTL[i]}"${log_time}.log
 IFS=';' read -r -a arrESR <<< ${emailArr[$i]} # Creating array of email and spark resource parameters
 spark-submit ${arrESR[6]} ${arrESR[7]} ${python_spark_dir}/${acctOTLArr[i]} -f ${config_file_dir}/${config_file} -r ${runDay} >> "${log_file_dir}"/"${logAOTL[i]}"${log_time}.log
 check_status $? "Script ${acctOTLArr[$i]} run" # Function to check spark-submit status
done
}

# Function definition to check status of spark-submit command
function check_status(){
 status=$1
 message=$2
 if [ $status -eq 0 ]
  then
    end_time=`date '+%Y-%m-%d %H:%M:%S'` ## Calculating end date time
	echo "INFO" "${acctOTLArr[$i]}" "Success - $message"
    echo "Finished running pyspark script ${acctOTLArr[$i]} at :${end_time}" >> "${log_file_dir}"/"${logAOTL[i]}"${log_time}.log
	send_email -t "${arrESR[0]}" -c "${arrESR[1]}" -s "${arrESR[2]}" -b "${arrESR[3]}" -a "${log_file_dir}"/"${logAOTL[i]}"${log_time}.log
 else
    end_time=`date '+%Y-%m-%d %H:%M:%S'` ## Calculating end date time
	echo "ERROR" "${acctOTLArr[$i]}" "Error - $message"
    echo "Finished running pyspark script ${acctOTLArr[$i]} at :${end_time}" >> "${log_file_dir}"/"${logAOTL[i]}"${log_time}.log
	send_email -t "${arrESR[0]}" -c "${arrESR[1]}" -s "${arrESR[4]}" -b "${arrESR[5]}" -a "${log_file_dir}"/"${logAOTL[i]}"${log_time}.log
    exit 1
 fi
}
# Calling function to trigger multi pyspark scripts
trigger_multi_scripts "${acctOTL[@]}" "${logAOTL[@]}" "${email_array_string[@]}"

