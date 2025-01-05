#!/bin/bash
email_html_output_file=/home/lh004970/email_html_output_file
## Sending HTML output as Email
from_email=$1
to_email=$2
subject=$3
html_report=$4
(
  echo To: $to_email
  echo From: $from_email
  echo "Content-Type: text/html; "
  echo "MIME-Version: 1.0";
  echo Subject:$subject
  echo
  echo $html_report
) | sendmail -t