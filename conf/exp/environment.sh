#!/bin/bash

#################################################################################
#																			   	#
#--   Create Date: 5/20/2020                                                   	#
#--   Modification log:                                                        	#
#--     Date         Name (SSO)                  History                       	#
#--   --------      ----------------------    ----------------------           	#
#--   5/20/2020      Zach Moore (290002542)      create                        	#
#																			   	#
#################################################################################

# Project Related Directories.
export project_home_path=$( dirname "$BASH_SOURCE" )
export scripts_path=$project_home_path/scripts/
export conf_path=$project_home_path/conf/
export sql_path=$project_home_path/sql/
export hdfs_processed_path=/EDL/src_data/processed/
export hdfs_protected_path=/EDL/src_data/protected/
export log_path=/data1/EDL/logs/

# Source generic sqoop environment.
export generic_sqoop_dir=/data1/EDL/domains/foundation/generic_sqoop_ingestion/
. $generic_sqoop_dir/environment.sh
