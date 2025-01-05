#!/bin/bash

###########################################################
# Script Name: amtfServiceTc360AcctLoad.sh                #
# Purpose : To load Tc360 account hive load               #
# Created By: Upendra  Kumar                              #
# Date Created : 22-Oct-2021                              #
# Date Last Modified :                                    #
###########################################################

## sourcing set_env file variables
project_dir=/data1/EDL/domains/amf_qa/edl_amf_qa
. $project_dir/set_env.env
echo ""
runDay=$1
#Setting up email notification
if [ -f ${edlcommon_shell_libraries}/send_email.sh ]
    then 
        . ${edlcommon_shell_libraries}/send_email.sh
else 
    echo "ERROR: The ${edlcommon_shell_libraries}/send_email.sh is missing"
    exit 99    
fi

# Passing the date arguments
if [ $# -eq 0 ]
  then
     runDay=`date -d 'today' '+%Y-%m-%d'`
else
     runDay=$runDay
fi
declare -a email_array_string=("${emailLAXref}" "${emailFcc}" "${emailCust}" "${emailBin6}" "${emailTB}" "${emailADL}" "${emailAL}")
#Function definition to trigger multi pyspark scripts
function trigger_multi_scripts(){
declare -a acctArr=($1)
declare -a logAL=($2)
shift 2
declare -a emailArr=("$@")
echo "Total elements in pyspark array: ${#acctArr[@]}"
echo "Total elements in email array: ${#emailArr[@]}"
for ((i=0;i<${#acctArr[@]};i++))
do
 start_time=`date '+%Y-%m-%d %H:%M:%S'` ## Initialising start date Time
 log_time=`date '+%Y%m%d%H%M%S'` ## Initialising log date time
 echo "Started running pyspark script ${acctArr[$i]} at : ${start_time}" > "${log_file_dir}"/"${logAL[i]}"${log_time}.log
 IFS=';' read -r -a arrESR <<< ${emailArr[$i]} # Creating array of email and spark resource parameters
 spark-submit ${arrESR[6]} ${arrESR[7]} ${python_spark_dir}/${acctArr[$i]} -f ${config_file_dir}/${config_file} -r ${runDay} >> "${log_file_dir}"/"${logAL[i]}"${log_time}.log
 check_status $? "Script ${acctArr[$i]} run"
done
 }

function check_status(){
 status=$1
 message=$2
 if [ $status -eq 0 ]
  then
    end_time=`date '+%Y-%m-%d %H:%M:%S'` ## Calculating end date time
	echo "INFO" "${acctArr[$i]}" "Success - $message"
	echo "Finished running pyspark script ${acctArr[$i]} at :${end_time}" >> "${log_file_dir}"/"${logAL[i]}"${log_time}.log
    send_email -t "${arrESR[0]}" -c "${arrESR[1]}" -s "${arrESR[2]}" -b "${arrESR[3]}" -a "${log_file_dir}"/"${logAL[i]}"${log_time}.log
 else
    end_time=`date '+%Y-%m-%d %H:%M:%S'` ## Calculating end date time
	echo "ERROR" "${acctArr[$i]}" "Error - $message"
	echo "Finished running pyspark script ${acctArr[$i]} at :${end_time}" >> "${log_file_dir}"/"${logAL[i]}"${log_time}.log
    send_email -t "${arrESR[0]}" -c "${arrESR[1]}" -s "${arrESR[4]}" -b "${arrESR[5]}" -a "${log_file_dir}"/"${logAL[i]}"${log_time}.log
	exit 1
 fi
}
# Function call to trigger multi pyspark scripts
trigger_multi_scripts "${acctArr[@]}" "${logAL[@]}" "${email_array_string[@]}"


