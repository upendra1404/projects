#!/bin/bash

##############################################################
# Script Name: amtfServiceGenericProcessedDataValidation.sh #
# Purpose : To process generic processed data validatins     #
# Created By: Upendra  Kumar                                 #
# Date Created : 07-Nov-2020                                 #
# Date Last Modified :                                       #
##############################################################

project_dir=/data1/EDL/domains/amf_qa/edl_amf_qa
. $project_dir/set_env.env
echo '\n'
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
# creating local dir
mkdir -p "${dupe_inbound_edge}"/amf_qa/schema/qa
mkdir -p "${dupe_inbound_edge}"/amf_qa/schema/dev
# creating hdfs dir
hdfs dfs -mkdir -p "${hdfs_inbound}"/schema/qa
hdfs dfs -mkdir -p "${hdfs_inbound}"/schema/dev

#Function definition to trigger multi pyspark scripts
function trigger_multi_scripts(){
gpdv_pty_str=$1
gpdv_cols_str=$2
gpdv_email_str=$3
declare -a qTabArr=($4)
declare -a dTabArr=($5)
declare -a keysArr=($6)
declare -a ipArr=($7)
declare -a etrArr=($8)
echo "Total elements in qTab array: ${#qTabArr[@]}"
echo "Total elements in dTab array: ${#dTabArr[@]}"
IFS=';' read -r -a arrColStr <<< ${cols_str}
for ((i=0;i<${#qTabArr[@]};i++))
do 
 start_time=`date '+%Y-%m-%d %H:%M:%S'` ## Initialising start date Time
 log_time=`date '+%Y%m%d%H%M%S'` ## Initialising log date time
 if [ $is_hdfs_path_read -eq 0 ]
  then 
      dev_table_name="$(cut -d'.' -f2 <<< ${dTabArr[i]})"
      qa_table_name="$(cut -d'.' -f2 <<< ${qTabArr[i]})"
      hive -e "desc ${dTabArr[i]};" > "${dupe_inbound_edge}"/amf_qa/schema/dev/schema_temp.csv
      hive -e "desc ${qTabArr[i]};" > "${dupe_inbound_edge}"/amf_qa/schema/qa/schema_temp.csv
      cat "${dupe_inbound_edge}"/amf_qa/schema/dev/schema_temp.csv | sed '1,3d' | head -n -1 > "${dupe_inbound_edge}"/amf_qa/schema/dev/"${dev_table_name}".csv
      cat "${dupe_inbound_edge}"/amf_qa/schema/qa/schema_temp.csv | sed '1,3d' | head -n -1 > "${dupe_inbound_edge}"/amf_qa/schema/qa/"${qa_table_name}".csv
      hdfs dfs -copyFromLocal -f "${dupe_inbound_edge}"/amf_qa/schema/qa/"${qa_table_name}".csv "${hdfs_inbound}"/schema/qa
      hdfs dfs -copyFromLocal -f "${dupe_inbound_edge}"/amf_qa/schema/dev/"${dev_table_name}".csv "${hdfs_inbound}"/schema/dev
 fi	 
 echo "Started running pyspark script ${gpdv_pty_str} for ${qTabArr[i]} table at : ${start_time}" > "${log_file_dir}"/"${logGPDV}""(${qTabArr[i]})_"${log_time}.log
 IFS=';' read -r -a arrESR <<< ${gpdv_email_str} # Creating array of email and spark resource parameters
 spark-submit ${arrESR[6]} ${arrESR[7]} ${python_spark_dir}/${gpdv_pty_str} -f ${config_file_dir}/${config_file} -r ${runDay} -n ${keysArr[i]} -s ${qTabArr[i]} -u ${dTabArr[i]} -w ${arrColStr[i]} -x ${ipArr[i]} -o ${hdfs_processed_qa} -p ${hdfs_processed_dev} -q ${is_hdfs_path_read} -c ${etrArr[i]} >> "${log_file_dir}"/"${logGPDV}""(${qTabArr[i]})_"${log_time}.log
 check_status $? "Script ${gpdv_pty_str} run" # Function to check spark-submit status
done
}

# Function definition to check status of spark-submit command
function check_status(){
 status=$1
 message=$2
 if [ $status -eq 0 ]
  then
    end_time=`date '+%Y-%m-%d %H:%M:%S'` ## Calculating end date time
	echo "INFO" "${gpdv_pty_str}" "Success - $message"
    echo "Finished running pyspark script ${gpdv_pty_str} for ${qTabArr[i]} table at :${end_time}" >> "${log_file_dir}"/"${logGPDV}""(${qTabArr[i]})_"${log_time}.log
	send_email -t "${arrESR[0]}" -c "${arrESR[1]}" -s "${arrESR[2]}" -b "${arrESR[3]}" -a "${log_file_dir}"/"${logGPDV}""(${qTabArr[i]})_"${log_time}.log
 else
    end_time=`date '+%Y-%m-%d %H:%M:%S'` ## Calculating end date time
	echo "ERROR" "${gpdv_pty_str}" "Error - $message"
    echo "Finished running pyspark script ${gpdv_pty_str} for ${qTabArr[i]} table at :${end_time}" >> "${log_file_dir}"/"${logGPDV}""(${qTabArr[i]})_"${log_time}.log
	send_email -t "${arrESR[0]}" -c "${arrESR[1]}" -s "${arrESR[4]}" -b "${arrESR[5]}" -a "${log_file_dir}"/"${logGPDV}""(${qTabArr[i]})_"${log_time}.log
    exit 1
 fi
}

# Calling function to trigger multi pyspark scripts
if [ $is_hdfs_path_read -eq 0 ]
 then 
      trigger_multi_scripts "${gpdvPyStr}" "${cols_str}" "${emailGPDV}" "${q_tab_arr[@]}" "${d_tab_arr[@]}" "${n_unique_arr[@]}" "${is_partitioned_arr[@]}" "${is_ets_control[@]}"
else
      trigger_multi_scripts "${gpdvPyStr}" "${cols_str}" "${emailGPDV}" "${q_hdfs_arr[@]}" "${d_hdfs_arr[@]}" "${n_unique_arr[@]}" "${is_partitioned_arr[@]}" "${is_ets_control[@]}"
fi