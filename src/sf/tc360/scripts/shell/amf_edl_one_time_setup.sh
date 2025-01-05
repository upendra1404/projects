#!/bin/bash

######################################################
# Script Name: amf_edl_one_time_setup.sh             #
# Purpose : To setup the EDL HDFS and Local Dir      #
# Created By: Upendra Kumar                          #
# Date Created : 03-Sept-2020                        #
# Date Last Modified :                               #
######################################################
	
#Setting up beeline_runsql function
if [ -f $edlcommon_shell_libraries/hive.sh ]
	then 
		. $edlcommon_shell_libraries/hive.sh
else 
	echo "ERROR: The current folder is missing the file hive.sh"
	exit 99	
fi

#Create the Log File Directory
mkdir -p ${log_file_dir}

#Defining the database 
edlstg_db=edl_amf_qa

log_time=`date '+%Y%m%d%H%M%S'`
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "Starting the EDL HDFS and HIVE environment setup for Account management fraud services at ${start_time}" > ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
echo "##################################################################################"

ldr_c360_attr_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_amf_c360_attributes
ldr_new_tc360_customer_attr_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_amf_tc360_customer
ldr_new_tc360_account_attr_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_amf_tc360_account
ldr_new_tc360_fcc_attr_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_tc360_fraud_claim_case
ldr_new_tc360_fct_attr_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_tc360_fraud_claim_tran
tc360_risk_merchant_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_tc360_risk_merchant_id
tc360_risk_merchant_nm_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_tc360_risk_merchant_name
tc360_risk_mcc_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_tc360_risk_mcc
tc360_risk_bin12_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_tc360_risk_bin12
tc360_risk_bin6_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_tc360_risk_bin6_merchant_id
mcdi0120_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_mastercard_0120_details
mcdi0120ChkPtLoc=${hdfs_processed}/amf_service/chkPt0120
mcdi0110_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/mastercard_di_details
cspa_rc_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_cspa_rc
inbound_edge_dir=${hdfs_inbound}/input_files/c360_attr_file
ldr_pc_acct_lexid_xref_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_amf_pc_ad_lexid_xref
ldr_pc_stmt_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_amf_pc_stmt
ldr_pc_app_fact_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_amf_pc_app_fact
ldr_new_tc360_falcon_attr_location=${hdfs_processed}/amf_service/hive/${edlstg_db}/ext_amf_tc360_falcon

create_hdfs_directory()
{
	dir_name=$1
	hadoop fs -ls ${dir_name}
	if [ $? -ne 0 ] 
	then	
		hadoop fs -mkdir -p ${dir_name}
		if [ $? -eq 0 ]
			then 
				echo "Successfully created HDFS Directory: ${dir_name}"
		else
			echo "ERROR: Failed creating directory: ${dir_name}"
		fi
	else	
		echo "Skipping as directory: ${dir_name} exists"
	fi
	echo "##############################################################################"
}

create_local_directory()
{
	dir_name=$1
	ls ${dir_name}
	if [ $? -ne 0 ] 
	then	
		mkdir -p ${dir_name}
		if [ $? -eq 0 ]
			then 
				echo "Successfully created local Directory: ${dir_name}"
		else
			echo "ERROR: Failed creating directory: ${dir_name}"
		fi
	else	
		echo "Skipping as directory: ${dir_name} exists"
	fi
	echo "##############################################################################"
}

# Precursor Step : Creating the Log directory:

create_local_directory ${log_file_dir}
# Create local inbound edge directory
create_local_directory ${amf_inbound_edge}/amf_qa/input_files/c360_attr_file

#Step-1 - HDFS Directory Setup
echo "Starting: Creating HDFS directories" >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log

# Creating the HDFS Directories for the hive tables:

create_hdfs_directory ${ldr_c360_attr_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${ldr_new_tc360_customer_attr_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${ldr_new_tc360_account_attr_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${ldr_new_tc360_fcc_attr_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${ldr_new_tc360_fct_attr_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${mcdi0120_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${mcdi0110_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${inbound_edge_dir} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${mcdi0120ChkPtLoc} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${cspa_rc_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${tc360_risk_merchant_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${tc360_risk_merchant_nm_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${tc360_risk_mcc_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${tc360_risk_bin12_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${tc360_risk_bin6_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${ldr_pc_acct_lexid_xref_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${ldr_pc_stmt_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${ldr_pc_app_fact_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
create_hdfs_directory ${ldr_new_tc360_falcon_attr_location} >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
echo "End: Creating HDFS directories" >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log

###################################################################################################################


#Step-3 - Create HIVE Tables
echo "Starting: Creating HIVE tables"  >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log

beeline_runsql -f ${hive_ddl_dir}/hiveTablesAmf.sql -v ldr_c360_attr_location=${ldr_c360_attr_location}\
                                                        -v ldr_new_tc360_customer_attr_location=${ldr_new_tc360_customer_attr_location}\
														-v ldr_new_tc360_account_attr_location=${ldr_new_tc360_account_attr_location}\
														-v ldr_new_tc360_fcc_attr_location=${ldr_new_tc360_fcc_attr_location}\
														-v ldr_new_tc360_fct_attr_location=${ldr_new_tc360_fct_attr_location}\
														-v mcdi0120_location=${mcdi0120_location}\
														-v mcdi0110_location=${mcdi0110_location}\
														-v cspa_rc_location=${cspa_rc_location}\
														-v tc360_risk_merchant_location=${tc360_risk_merchant_location}\
														-v tc360_risk_merchant_nm_location=${tc360_risk_merchant_nm_location}\
														-v tc360_risk_mcc_location=${tc360_risk_mcc_location}\
														-v tc360_risk_bin12_location=${tc360_risk_bin12_location}\
														-v tc360_risk_bin6_location=${tc360_risk_bin6_location}\
														-v ldr_pc_acct_lexid_xref_location=${ldr_pc_acct_lexid_xref_location}\
														-v ldr_pc_stmt_location=${ldr_pc_stmt_location}\
														-v ldr_pc_app_fact_location=${ldr_pc_app_fact_location}\
														-v ldr_new_tc360_falcon_attr_location=${ldr_new_tc360_falcon_attr_location}\
														-v db=${edlstg_db}\
														>> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log

if [ $? -eq 0 ]
    then
        echo "Completed: Creating HIVE tables" >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
else
    echo "Error: Failed to Create the Hive Tables" >> ${log_file_dir}/amf_edl_one_time_setup_${log_time}.log
fi
