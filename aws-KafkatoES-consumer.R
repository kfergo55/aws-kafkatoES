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
# Function grab_andsendES
# function to send grab data from to kafka, transform and send to ES
grab_andsend <- function(topic, esindex, ixstart, ixend) {

  # no harm in ensuring we close the consumer if we are running batches interactively
  rkafka.closeConsumer(consumer1)
  
  # open the consumer
  #  consumer1=rkafka.createConsumer("127.0.0.1:2181",topic_name, consumerTimeoutMs = 20000)
  consumer1=rkafka.createConsumer("b-3.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092,b-2.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092,b-1.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092","kf-txhouse")
  
  # open connection to ES
  esconnect <- elastic::connect(es_host = "search-esv190503-jeodnaa566dp6eluw47rqophfy.ap-southeast-1.es.amazonaws.com", es_port = 80, path = "", es_transport_schema  = "http")
  
  # if index doesn't exist, create it
  if(!elastic::index_exists(esconnect, index=esindex)) {
     elastic::index_create(esconnect, index=esindex) 
  }
  
  # loop through the kafka messages
  for(i in ixstart:ixend) { 
    # consume the message (i.e. grab a message off the Kafka queue)
    msg <- rkafka.read(consumer1)
    
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
    if (no_customerdets) {msg_ext_df <- c("Missing Customer Details for ",msg_df$Customer.ID)}
    
    # send data to ES
    # todo - extend to a tryCatch and include error processing
    logES[i] <- docs_create(awsconnect, index=esindex, type='deprecated', id=i, 
                body=as.list(msg_ext_df))
    
  }
  # close connection
  rkafka.closeConsumer(consumer1)
  # write logES file to disk
  logfilename <- paste0("LogES", format(Sys.time(), "%Y%M%d-%H-%M-%S"),".csv")
  write.csv(logES, file=logfilename)
  # return the result list
  return(res)
}

###############################################
# set our future plan to multisession so we can continue with processing 
# 
plan(multisession) 

# set topic name
topic_name <- "test-sales-topic4"
index_name <- "test-sales-index"

# call the producer with an index range of messages to send to kafka 
result <- as.list(NA)
result %<-% grab_andsend(topic_name,index_name,1,10)


# checking the progress/results (optional)

if(!resolved(result)) print("not resolved") else print("resolved")

# force the future to block until resolved
value(result)

unlist(result)


# DON'T FORGET TO DELETE THE AWS RESOURCES!!! 
# (use the corresponding "awscli" file or the AWS console) 
