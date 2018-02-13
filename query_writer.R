

query_writer <- function(metric, mlab_location, AS, start_time, end_time,
                         country = 'US' ) 
      {
      # This function takes as input the metric, mlab_location, AS number, 
      # start_time, end_time and the optional country (the default 
      # country is set to US)
      # Check out the MLabServers.csv file to look up possible values for the
      # mlab_location and AS variables.  The mlab_location should be entered using 
      # quotation marks, the AS should be entered as an integer.
      # The choices for the metric are: "dtp", "rtt", and "prt" for download 
      # throughput, round trip time and packet retransmission respectively
      # The start_time, end_time info should be entered in the 'mm/dd/yy' format
      # The output of the function, when successful, is a text file, called
      # query.txt
      
     
      #DEFINING THE BASIC QUERIES FOR EACH METRIC
      
      #The basic query for download throughput
      dtp_basic_query<-
      "SELECT
      web100_log_entry.connection_spec.remote_ip AS remote_ip,
      web100_log_entry.connection_spec.local_ip AS local_ip,
      8 * (web100_log_entry.snap.HCThruOctetsAcked /
      (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd)) AS download_Mbps
      FROM
      [plx.google:m_lab.ndt.all]
      WHERE
      IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd)
      AND project = 0
      AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction)
      AND connection_spec.data_direction = 1
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry)
      AND web100_log_entry.is_last_entry = True
      AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
      AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd) >= 9000000
      AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +  
      web100_log_entry.snap.SndLimTimeSnd) < 3600000000
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CongSignals)
      AND web100_log_entry.snap.CongSignals > 0
      AND (web100_log_entry.snap.State == 1
      OR (web100_log_entry.snap.State >= 5
      AND web100_log_entry.snap.State <= 11))"
      
      
      #The basic query for finding round trip time 
      rtt_basic_query <-
      "SELECT
      web100_log_entry.connection_spec.remote_ip AS remote_ip,
      web100_log_entry.connection_spec.local_ip AS local_ip,
      web100_log_entry.snap.MinRTT AS min_rtt
      FROM
      [plx.google:m_lab.ndt.all]
      WHERE
      IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd)
      AND project = 0
      AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction)
      AND connection_spec.data_direction = 1
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry)
      AND web100_log_entry.is_last_entry = True
      AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
      AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd) >= 9000000
      AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +  
      web100_log_entry.snap.SndLimTimeSnd) < 3600000000
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.MinRTT)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CountRTT)
      AND web100_log_entry.snap.CountRTT > 10
      AND (web100_log_entry.snap.State == 1
      OR (web100_log_entry.snap.State >= 5
      AND web100_log_entry.snap.State <= 11))"
      
      
      #The basic query for packet retransmission 
      prt_basic_query <-
      "SELECT
      web100_log_entry.connection_spec.remote_ip AS remote_ip,
      web100_log_entry.connection_spec.local_ip AS local_ip,
      (web100_log_entry.snap.SegsRetrans / web100_log_entry.snap.DataSegsOut) AS packet_retransmission_rate
      FROM
      [plx.google:m_lab.ndt.all]
      WHERE
      IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd)
      AND project = 0
      AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction)
      AND connection_spec.data_direction = 1
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry)
      AND web100_log_entry.is_last_entry = True
      AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
      AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd) >= 9000000
      AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +  
      web100_log_entry.snap.SndLimTimeSnd) < 3600000000
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SegsRetrans)
      AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.DataSegsOut)
      AND web100_log_entry.snap.DataSegsOut > 0
      AND (web100_log_entry.snap.State == 1
      OR (web100_log_entry.snap.State >= 5
      AND web100_log_entry.snap.State <= 11))"
      
      #SELECTING THE RIGHT BASIC QUERY
      
      if (metric == "dtp") {
            basic <- dtp_basic_query
      }
      else if (metric == "rtt") {
            basic <- rtt_basic_query
      }
      else if (metric == "prt") {
            basic <- prt_basic_query
      }
      else {
            print("The metric entered is invalid!")
            return()
      }
      
       #FINDING MLAB SERVER IPS
      servers <- read.csv("MLabServers.csv")
      cond<- (servers$City == mlab_location & servers$AS == AS)
      if (nrow(na.omit(servers[cond,])) == 0 ){
            print("There are no MLab servers satisfying the conditions entered.")
            return()
      }  
      else {
           ips <- as.character(na.omit(servers[cond,])$IP)
      }
      
      #WRITING THE MLAB SERVER CONDITION
      mlab_serv_var <- "web100_log_entry.connection_spec.local_ip"
      mlab_ips_cond <- paste("\n", "AND (", collapse = "")
      for (x in ips[1:(length(ips)-1)]) {
            mlab_ips_cond <- paste( mlab_ips_cond, mlab_serv_var, "==", "\'", x, 
                         "\'","\n", "OR ", sep = "", collapse = "" )
      }
      mlab_ips_cond <- paste( mlab_ips_cond, mlab_serv_var, "==", "\'", 
                  ips[length(ips)], "\'",  ")", sep = "", collapse = "" )
      
      #CONVERTING DATE TO UNIX TIMESTAMP
      end_time_unix <-
            as.numeric(as.POSIXct(as.Date(end_time, "%m/%d/%y")))
      start_time_unix <- 
            as.numeric(as.POSIXct(as.Date(start_time, "%m/%d/%y")))
      if (is.na(end_time_unix)) {
            print("The end_time entered is invalid!")
            return()
      }
      if (is.na(start_time_unix)) {
            print("The start_time entered is invalid!")
            return()
      }
      
      #WRITING THE TIME CONDITION
      tstamp_var <- "web100_log_entry.log_time"
      tframe_cond <- paste("\n", "AND ", tstamp_var, "<=", end_time_unix,"\n",
                  "AND ", tstamp_var, ">=", start_time_unix, collapse="")
      
      #WRITING THE COUNTRY CONDITION
      country_string<- paste("\'", country, "\'", sep="", collapse = "")
      country_var <- "connection_spec.client_geolocation.country_code"
      country_cond <- 
            paste("\n", "AND ", country_var, "==", country_string, collapse="")
      
      #WRITING THE QUERY
      
      the_query <- 
      paste(basic ,country_cond, mlab_ips_cond, tframe_cond, collapse = "")
      write(the_query, "query.txt")
      
     
      
}

args <- commandArgs(trailingOnly = TRUE)
#cat(typeof(args[3]), "\n")
query_writer(args[1], args[2], as.integer(args[3]), args[4], args[5])
