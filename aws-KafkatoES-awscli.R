###############################################
# *****AWS CLI Addendum 

# This file has a corresponding "source code" file which is the 
# name of this file but with "-awscli" removed.



# Objective of this "awscli" file:
# This "awscli" file will help create the necessary AWS services by using 
# the AWS CLI.
# If you don't have the AWS CLI installed you can install from here: 
# https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html

# If you are relatively new to AWS, it may be better to manually create the 
# services using a GUI, so use the AWS Management Console here:
# (https://aws.amazon.com/console/). 
# You will need to have a basic understanding of VPC's, subnets, and 
# security groups but you don't really need experience in MSK or 
# Elasticsearch Service. You can use the parameters provided in the awscli
# commands below to create the services using the AWS console.

#******************Very Important********************
# NOTE: Please ensure you delete the Kafka cluster
# at the end of your testing. At the time of writing, the MSK service is not
# included in the free tier. The ES Domain should be deleted as well 
# even though free hours for an ES Domain are included in the free tier account.
# 


##############################################
# Setup the AWS environment 

# AWS Pre-requisites: 
# 1. Confirm you are in the correct region first and that MSK service is available (not
# currently availabile in all regions. 
# 2. you need 3 subnets within a VPC
# 3. you need a security group that is open to http/https traffic (ports 80 and 8080)
# and ports for ES and MSK

# NOTE: AWS internal resource id's will not match what is displayed in the 
# AWS Managemtent console. You can't copy the subnet id from the console and use 
# in the aws cli, you have to query the resource id with the cli to use 
# within the cli.

# ALTERNATE OS: If you aren't using Windows and the shell command is not working:
# you can open a terminal in RStudio (bottom left) or a native terminal window
# and run the aws cli commands without the enclosing quotes 

# AWS Pre-requisite 1:

# confirm you are in the correct region
shell("aws configure get default.region")
shell("aws kafka") 
# (You don't want to) find in output: "Invalid choice: 'kafka'" *try a valid region 

# AWS Pre-requisite 2:

# run the aws describe subnet command below and paste the SubnetId from the output 
# into the "aws kafka create..." command below where ClientSubnets="your subnet id"
shell("aws ec2 describe-subnets") 
# find in output: "SubnetId": "subnet-2cc8f165" (find three!)

# AWS Pre-requisite 3:

# run the aws command below and past the security group ID into the 
# "aws kafka create..." command below where SecurityGroups="you security group id"
shell("aws ec2 describe-security-groups --group-names launch-wizard-1")
# find in output:  "GroupId": "sg-5c94e724"
# if multiple security groups, you will need to find the one for the correct VPC
# and inbound/outbound access

##############################################
# Setup the AWS environment - Kafka and ES

# STEP 1 OF 2 - Create Kafka Cluster in AWS
# Feel free to change your cluster-name and ensure you have replaced the values
# for the subnets and the security group.
# NOTE: for readability this command has new lines but when passed to the shell
# they need to be removed. 
shell(stringr::str_remove_all('aws kafka create-cluster 
        --cluster-name "Kafkav190506" 
        --kafka-version "1.1.1" 
        --number-of-broker-nodes 3 
        --enhanced-monitoring "DEFAULT" 
        --broker-node-group-info 
            "ClientSubnets="subnet-2cc8f165",
              "subnet-84666ee3",
              "subnet-2e63a777",
            InstanceType="kafka.m5.large",
            SecurityGroups="sg-5c94e724",
            StorageInfo={EbsStorageInfo={VolumeSize=1}}"', "[\n]"))

# QUERIES:
# While the cluster is starting you can grab the arn from the output 
# and copy into the delete-cluster command near the end of this script.
# to list the details of the kafka clusters again or check whether the cluster is 
# active use the list-clusters command.
#
shell("aws kafka list-clusters")
#
# find in output:  "State": "ACTIVE"

# After cluster is "ACTIVE", grab the connection string
#
shell(stringr::str_remove_all('aws kafka get-bootstrap-brokers --cluster-arn 
  "arn:aws:kafka:ap-southeast-1:308573694809:cluster/Kafkav190506/e0db2c9d-dd29-4f54-a75f-bd6be7293683-2"', "[\n]"))
#
# find in output: "BootstrapBrokerString": "b-3.kafkav190503.nqh12a.c2.kafka.ap-southeast-1
# .amazonaws.com:9092,b-2.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092,
# b-1.kafkav190503.nqh12a.c2.kafka.ap-southeast-1.amazonaws.com:9092"
# copy this string to the CreateProducer and the CreateConsumer strings in the
# "source file"


# STEP 2 OF 2 - Create ES domain in AWS
shell(stringr::str_remove_all('aws es create-elasticsearch-domain 
      --domain-name "esv190503"
      --elasticsearch-version "6.5"
      --elasticsearch-cluster-config 
        "InstanceType=t2.small.elasticsearch,
        InstanceCount=2"
      --ebs-options 
        "EBSEnabled=true,
        VolumeType=standard,
        VolumeSize=10"', "[\n]"))
# note: if you changed the domain-name setting, ensure you copy the 
# new name to the delete-elasticsearch command below

# QUERIES:
# after the domain is created, you can query for the endpoint and  
# paste/overwrite into the es connection in the source code/file  
#
shell('aws es describe-elasticsearch-domain --domain-name "esv190503"')
#
# look for the following line in the output (near the beginning, near the ARN)
#     "Endpoint": "search-esv190503-jeodnaa566dp6eluw47rqophfy.ap-southeast-1.es.amazonaws.com",
# copy the string within the quotes, i.e. "search<identifyingstring>amazonaws.com"
# search in the "source code" file for elastic::connect and replace


###########################################
# Cleaning up - Deleting Resources


# Don't forget to come back here and delete the resources!!! 
shell('aws kafka delete-cluster --cluster-arn "arn:aws:kafka:ap-southeast-1:308573694809:cluster/Kafkav190506/e0db2c9d-dd29-4f54-a75f-bd6be7293683-2"')

shell('aws es delete-elasticsearch-domain --domain-name "esv190503"')

# make sure they are gone!
shell("aws kafka list-clusters")
#
shell('aws es list-domain-names')
#