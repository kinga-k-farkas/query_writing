This project's focal point is the query_writer.R function, which writes specific queries for the MLab data stored in Google BigQuery

To run the script:

Make sure R is installed.

In the terminal navigate to the QueryWriting Folder.

Then run:

Rscript query_writer.R "argument 1" "argument 2" "argument 3" "argument 4" "argument 5"


Where 

"argument 1" is the value of the metric. The choices for the argument 1 are: "dtp", "rtt", and "prt" for download throughput, round trip time and packet retransmission respectively.

"argument 2" is the M-Lab server location.  See MLabServers.csv file for possible values.

"argument 3" is the AS number, enter it without using quotation marks.  See MLabServers.csv file for possible values. 

"argument 4" is the start time.   It should be entered in the 'mm/dd/yy' format


"argument 5" is the end time.   It should be entered in the 'mm/dd/yy' format


After a successful run, the output is written to a text file: query.txt



Example:

Rscript query_writer.R "rtt" "New York" 174 "06/15/14" "05/13/15"
