#!/bin/bash
## shell scripts run command
nohup sh /grid/1/data1/EDL/domains/amf_qa/edl_amf_qa/set_env.env;${project_home_directory}/scripts/shell/amf_edl_c360_attr_mysql_pty_load.sh &
nohup sh /grid/1/data1/EDL/domains/amf_qa/edl_amf_qa/set_env.env;${project_home_directory}/scripts/shell/amf_edl_c360_attr_mysql_aes_load.sh &