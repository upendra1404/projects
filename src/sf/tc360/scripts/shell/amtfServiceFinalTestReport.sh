#!/bin/bash
project_dir=/data1/EDL/domains/amf_qa/edl_amf_qa
. $project_dir/set_env.env
echo ""
###########################################################
# Script Name: amtfServiceFinalTestReport.sh              #
# Purpose : To send html test report for data validations #
# Created By: Upendra  Kumar                              #
# Date Created : 19-Nov-2021                              #
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

# Passing the date arguments
if [ $# -eq 0 ]
  then
     runDay=`date -d 'today' '+%Y-%m-%d'`
else
     runDay=$runDay
fi
#Function definition to trigger multi pyspark scripts
function trigger_multi_scripts(){
declare -a trArr=($1)
declare -a logETR=($2)
declare -a emailArr=("$3")
q_tab_arr=`echo $4 | tr ' ' ','`
d_tab_arr=` echo $5 | tr ' ' ','`
echo "Total elements in pyspark array: ${#trArr[@]}"
echo "Total elements in email array: ${#emailArr[@]}"
for ((i=0;i<${#trArr[@]};i++))
do 
 start_time=`date '+%Y-%m-%d %H:%M:%S'` ## Initialising start date Time
 log_time=`date '+%Y%m%d%H%M%S'` ## Initialising log date time
 echo "Started running pyspark script ${trArr[$i]} at : ${start_time}" > "${log_file_dir}"/"${logETR[i]}"${log_time}.log
 IFS=';' read -r -a arrESR <<< ${emailArr[$i]} # Creating array of email and spark resource parameters
 spark-submit ${arrESR[6]} ${arrESR[7]} ${python_spark_dir}/${trArr[i]} -f ${config_file_dir}/${config_file} -r ${q_tab_arr} -u ${d_tab_arr} -x ${is_hdfs_path_read} 2>&1 | tee -a "${log_file_dir}"/"${logETR[i]}"${log_time}.log
 check_status $? "Script ${trArr[$i]} run" # Function to check spark-submit status
done
}

# Function definition to check status of spark-submit command
function check_status(){
 status=$1
 message=$2
 if [ $status -eq 0 ]
  then
    end_time=`date '+%Y-%m-%d %H:%M:%S'` ## Calculating end date time
	echo "INFO" "${trArr[$i]}" "Success - $message"
    echo "Finished running pyspark script ${trArr[$i]} at :${end_time}" >> "${log_file_dir}"/"${logETR[i]}"${log_time}.log
	echo "Email report sent successfully" >> "${log_file_dir}"/"${logETR[i]}"${log_time}.log
 else
    end_time=`date '+%Y-%m-%d %H:%M:%S'` ## Calculating end date time
	echo "ERROR" "${trArr[$i]}" "Error - $message"
    echo "Finished running pyspark script ${trArr[$i]} at :${end_time}" >> "${log_file_dir}"/"${logETR[i]}"${log_time}.log
	send_email -t "${arrESR[0]}" -c "${arrESR[1]}" -s "${arrESR[4]}" -b "${arrESR[5]}" -a "${log_file_dir}"/"${logETR[i]}"${log_time}.log
    exit 1
 fi
}
# Calling function to trigger multi pyspark scripts
if [ $is_hdfs_path_read -eq 0 ]
 then
      trigger_multi_scripts "${trArr[@]}" "${logETR[@]}" "${emailTR}" "${q_tab_arr}" "${d_tab_arr}"
else
      trigger_multi_scripts "${trArr[@]}" "${logETR[@]}" "${emailTR}" "${q_hdfs_arr}" "${d_hdfs_arr}"
fi

