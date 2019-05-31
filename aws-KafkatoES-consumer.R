###############################################
# *****************CONSUMER******************
# Overview:
# Create a test data pipeline: 
# "Producer" Step 1: Stream "transactional sales order data"
# to AWS's Kafka Managed Service
# "Consumer" Step 2: Pull from the Kafka stream, transform the data
# and store in ElasticSearch on AWS using R
#
# *****************CONSUMER******************
# High Level Description:
# This code reads from the test Kafka stream (AWS Service - 
# Managed Streams for Kafka (MSK)) transforms the data to enrich with text
# and stores into Elasticsearch for AWS (ES).  

# Description for this code file:
# This file assumes:
# 1.) An AWS MSK cluster is created and you have the connection details 
# necessary for the producer connection and consumer connection. 
# 2.) An AWS Elasticsearch instance is created and you have the connection
# details from AWS.
# If you are creating your own resources, please
# see the companion "awscli" file in github, <name-of-this-file>-awscli.R

##############################################
# Setup the R environment
#install.packages("rkafka")
#install.packages("jsonlite")
#install.packages("elastic")
#install.packages("dplyr")
#install.packages("promises")
#install.packages("future")

library(rkafka) # wrapper for the Apache Kafka API's
library(jsonlite) # streaming data format is in json
library(elastic) # wrapper for the ElasticSearch API's
library(dplyr) # data wrangling

library(promises) # for asyinc processing for stream insertion
library(future) # for async processing (stream insertion)
##############################################
# Function 

# writetolog

# function to create a new log file and dump cached logs 
writetolog <- function(wlogES_df) {

logfilename <- paste0("LogES", format(Sys.time(), "%Y%M%d-%H-%M-%S"),".csv")
write.csv(wlogES_df, file=logfilename)
}
##############################################
# Function 

# initlog

# function to initialize the log dataframe 
initlog <- function() {
  flogES_df <- NA
  flogES_df <- as.data.frame(matrix(c("initialized", format(Sys.time(), "%Y%M%d-%H-%M-%S")), 
                                   nrow=1, dimnames = list(NULL, c("kafka-in", "es-out"))))
  return(flogES_df)
}
##############################################
# Function 

# grab_andsend

# function to send grab data from to kafka, transform and send to ES
grab_andsend <- function(estring, cstring, ftopic_name, esindex, ixstart, ixend, logrowlimit) {

  # open the consumer
  #  consumer1=rkafka.createConsumer(cstring, topic_name, consumerTimeoutMs = 20000)
  consumer1=rkafka.createConsumer(cstring, ftopic_name)
  
  # initialize log file
  logES_df <- initlog()
  # initialize return value
  result <- NA
  
  # loop through the kafka messages
  for(i in ixstart:ixend) { 

    # Wait so we don't overrun the producer
    Sys.sleep(10)
    
    # consume the message (i.e. grab a message off the Kafka queue)
    msg <- rkafka.read(consumer1)
    
    if(msg == "") {result <- "Kafka connectivity FAILED"; return(result) }
    
    ##########################################
    # Next Step: Transform our data based on the message from the Kafka queue
    #   1.) Extend the data by querying the Data Warehouse (DW) using the Customer.ID
    #   2.) Send the enriched text stream to our search engine ES
    
    # grab the Customer.ID field from with the message by converting the
    # JSON Stream to a df 
    msg_df <- fromJSON(msg)
    
    # "Connect" to DW and get further customer data for this sales order 
    #     via the SQL statement 
    sqlstmt <- stringr::str_remove_all(paste0(
      'SELECT "First.Name", "Last.Name", AddressLine1, AddressLine2,
    City, Name, "State.Province.Code", "Country.Region.Code", "Customer.ID" 
    FROM AdvenSalesDW WHERE 
    "Customer.ID" = "', msg_df$Customer.ID, '"'),
      "[\n]")
    
    #     and execute
    msg_ext_df <- sqldf::sqldf(sqlstmt)
    
    # Check SQL Query result - ensure customer found in the DW
    no_customerdets <- !length(.subset2(msg_ext_df, 1L)) > 0L
    
    # Still send the msg but without additional text
    if(no_customerdets) {
      msg_ext_df <- c("Missing Customer Details for ", msg_df$Customer.ID)
      }
    
    # send data to ES
    logES <- tryCatch( 
        docs_create(index=esindex, type='deprecated', 
                body=as.list(msg_ext_df[1,])),
        warning=function(w) {result <- "Elasticsearch connectivity FAILED"; return(result)}
        )
    
    # log the successful writes to ES into a dataframe
    logES_df <- add_row(logES_df, "kafka-in" = toString(toJSON(msg_ext_df[1,])),
          "es-out" = toString(toJSON(logES))) 
    
    # write log file to disk if we've hit the filesize line constraint
    if(nrow(logES_df) > logrowlimit) {
      writetolog(logES_df)
      LogES_df <- initlog()
    }
    
    #check for index end value
    if(ixend == i) {
      result <- "all records processed"}
  
  }
  # close connection
  rkafka.closeConsumer(consumer1)
  
  #  call function to write the rest of logES dataframe to disk - even if no successes
  writetolog(logES_df)
  
  # return 
  return(result)
}

###############################################
# Main Block

# Initialize

# set our future plan to multisession so we can continue with processing 
plan(multisession) 

# set topic name and index name - elasticsearch index name must be lowercase
topic_name <- "test-sales-topic-1C"
index_name <- "test-sales-index-1c"

# set connection strings
es_string <- "127.0.0.1"
#es_string <- "search-esv190503-jeodnaa566dp6eluw47rqophfy.ap-southeast-1.es.amazonaws.com"
con_string <- "127.0.0.1:2181"
#con_string <- "b-3.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092,b-2.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092,b-1.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092"

# open connection to ES
esconnect <- elastic::connect(es_host = es_string, es_port = 9200, path = "", es_transport_schema  = "http")

# set the range of data we will "stream"
rowstart <- 1
rowend <- 10
rowend <- nrow(AdvenSales) 

# set the size of the logfile by rows
logrowlimit <- 200


# call the function 
result <- NA

result <- grab_andsend(es_string, con_string, topic_name, index_name, rowstart, rowend, logrowlimit)

# End of Main Block
##############################################

###################################
#  Optional and Cleanup 

###
# If interactive - uncomment to check the progress/results using
# resolved (this step is optional)

# check for resolved, this should not block
# if(!resolved(result)) print("not resolved") else print("resolved")

# force the future to block until resolved
# value(result)

###
# To debug issues with kafka corruption  - where msg returns as "" and no error
# the high level consumer relies on kafka offset
# simple consumer reads work when offset fails (imho) 
#  (optional debug step)

#consumer1=rkafka.createSimpleConsumer("127.0.0.1","9092","10000","100000", topic_name)
#rkafka.receiveFromSimpleConsumer(consumer1,topic_name,"0","2","500")
#msg <- rkafka.readFromSimpleConsumer(consumer1)


# DON'T FORGET TO DELETE THE AWS RESOURCES!!! 
# (use the corresponding "awscli" file or the AWS console) 
