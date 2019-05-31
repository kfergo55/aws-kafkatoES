###############################################
# Overview:
# Create a test data pipeline: 
# "Producer" Step 1: Stream "transactional sales order data"
# to AWS's Kafka Managed Service
# "Consumer" Step 2: Pull from the Kafka stream, transform the data
# and store in ElasticSearch on AWS using R
#
# *****************PRODUCER******************
# High Level Description:
# This code creates a test stream using the AWS Service - 
# Managed Streams for Kafka (MSK) and Elasticsearch for AWS (ES).  

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
# Get Data

# I've treaked the publicly available Adventure Works Bicycle Company data to
# create a sample "streaming" dataset of Sale Orders and to create a mock DW 
# for this data pipeline
AdvenSales <- read.csv("https://raw.githubusercontent.com/kfergo55/aws-kafkatoES/master/AdvenSales_demo.csv")
AdvenSalesDW <- read.csv("https://raw.githubusercontent.com/kfergo55/aws-kafkatoES/master/AdvenSalesDW_demo.csv")

# order the data frame by modified date to simulate order inserts/updates from an 
# enterprise order system 
AdvenSales <- AdvenSales %>% arrange(Modified.Date)

############################################
# Initiate the async stream of "Sales Data" to the kafka cluster

# Warning: This test stream does not currently encrypt the stream data.

# If needed: This test stream can be run on an EC2 server within the VPC
# that hosts the Kafka cluster and ElasticSearch instance, but in this 
# example we are generating data locally and sending the data externally 
# unmodified.

# Overview:
# Using the futures package, this code block will be executing asynchronously
# by call the function below as a future using %<-%. It opens the Producer
# connection, then loops through sending a message after 10 seconds until
# all data is sent from the data frame AdvenSales

##############################################
# Function send_andwait
# function to send data to kafka every 10 seconds
send_andwait <- function(cstring, topic, data, ixstart, ixend) {
  
  # open a producer on AWS Managed Streaming Kafka
  # force the logger error by running twice
  prod1=rkafka.createProducer(cstring)
  prod1=rkafka.createProducer(cstring)
  
  res <- NA
  
  for(i in ixstart:ixend) { 
    res <- message(rkafka.send(prod1, topic, cstring, toJSON(data[i,])))
    Sys.sleep(10)
    
  }
  # close the producer
  rkafka.closeProducer(prod1)
  # return the result list
  return(res)
}

###############################################
# set our future plan to multisession so we can continue with processing 
# 
plan(multisession) 

# set topic name - create topic below as broker errors on autocreate of topic
topic_name <- "test-sales-topic-1C"

# set connection string
con_string <- "127.0.0.1:9092"
#con_string <- "b-3.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092,b-2.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092,b-1.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092"

# set the range of data we will get from the "stream"
rowstart <- 1
rowend <- nrow(AdvenSales) 
# rowend <- 10

# call the producer with an index range of messages to send to kafka 
result <- as.list(NA)
result %<-% send_andwait(con_string, topic_name, AdvenSales, rowstart, rowend)


########################################
# END MAIN BLOCK

###
# checking the progress (optional)

# ifelse(resolved(result), print("resolved"), print("not resolved"))

# force the future to block until resolved
# value(result)

###
# list the topics for a local install of kafka and create a topic
#oldwd <- getwd()
#setwd("C:/Users/Nigel/kafkaNODE2_2.12-2.2.0/bin/windows")
#shell('kafka-topics.bat --list --zookeeper localhost:2181')
#shell('kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-sales-topic-1C')
#setwd(oldwd)

