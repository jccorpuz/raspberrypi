# Developed by: Sebastian Maurice, PhD
# Date: 2021-01-18 
# Toronto, Ontario Canada

# TML python library
import maadstml

# Uncomment IF using Jupyter notebook 
import nest_asyncio

import json
import random
from joblib import Parallel, delayed
import sys
import multiprocessing
import pandas as pd
import asyncio
import datetime
import time
import os

# Ensure Jupyter compatibility
nest_asyncio.apply()

# Global paths and environment variables
basedir = os.environ['userbasedir']
VIPERHOST = ''
VIPERPORT = ''
HTTPADDR = 'https://'


#############################################################################################################
#                                      STORE VIPER TOKEN
def getparams():
    global VIPERHOST, VIPERPORT, HTTPADDR
    with open(basedir + "/Viper-preprocess/admin.tok", "r") as f:
        VIPERTOKEN = f.read()

    if VIPERHOST == "":
        with open(basedir + '/Viper-preprocess/viper.txt', 'r') as f:
            output = f.read()
            VIPERHOST = HTTPADDR + output.split(",")[0]
            VIPERPORT = output.split(",")[1]

    return VIPERTOKEN

VIPERTOKEN = getparams()
if VIPERHOST == "":
    print("ERROR: Cannot read viper.txt: VIPERHOST is empty or HPDEHOST is empty")


#############################################################################################################
#                                     CREATE TOPICS IN KAFKA

def datasetup(maintopic, preprocesstopic):
    companyname = "OTICS"
    myname = "Sebastian"
    myemail = "Sebastian.Maurice"
    mylocation = "Toronto"

    replication = 1  # Replication factor for Kafka redundancy
    numpartitions = 3  # Number of partitions for Kafka topic
    enabletls = 1  # Enable SSL/TLS communication
    brokerhost = ''  # Broker host
    brokerport = -999  # Broker port
    microserviceid = ''  # Microservice ID if needed

    description = "TML Use Case"

    # Create the main topic in Kafka
    result = maadstml.vipercreatetopic(VIPERTOKEN, VIPERHOST, VIPERPORT, maintopic, companyname,
                                       myname, myemail, mylocation, description, enabletls,
                                       brokerhost, brokerport, numpartitions, replication,
                                       microserviceid)

    try:
        y = json.loads(result, strict='False')
    except Exception as e:
        y = json.loads(result)

    for p in y:  # Loop through the JSON and grab the topic and producer IDs
        pid = p['ProducerId']
        tn = p['Topic']

    # Create the preprocess topic in Kafka
    result = maadstml.vipercreatetopic(VIPERTOKEN, VIPERHOST, VIPERPORT, preprocesstopic, companyname,
                                       myname, myemail, mylocation, description, enabletls,
                                       brokerhost, brokerport, numpartitions, replication,
                                       microserviceid)

    return tn, pid


def sendtransactiondata(maintopic, mainproducerid, VIPERPORT, index, preprocesstopic):
    maxrows = 500
    offset = -1
    topic = maintopic
    producerid = mainproducerid
    brokerhost = ''
    brokerport = -999
    microserviceid = ''

    preprocessconditions = ''
    preprocesslogic = 'MIN,MAX,COUNT,VARIANCE,OUTLIERS,ANOMPROB'  # Define preprocessing logic

    # Define the JSON criteria for the preprocessing
    jsoncriteria = 'uid=metadata.dsn,filter:allrecords~\
    subtopics=metadata.property_name~\
    values=datapoint.value~\
    identifiers=metadata.display_name~\
    datetime=datapoint.updated_at~\
    msgid=datapoint.id~\
    latlong=lat:long'     

    tmlfilepath = ''
    usemysql = 1

    streamstojoin = "" 
    identifier = "IoT device performance and failures"

    pathtotmlattrs = 'oem=n/a,lat=n/a,long=n/a,location=n/a,identifier=n/a'

    try:
        # Debug: print the logic being passed
        print(f"Sending preprocessing with logic: {preprocesslogic}")

        # Send the request to preprocess the data
        result = maadstml.viperpreprocesscustomjson(VIPERTOKEN, VIPERHOST, VIPERPORT, topic, producerid, offset,
                                                    jsoncriteria, 1, maxrows, 1, 70, brokerhost, brokerport,
                                                    microserviceid, -999, streamstojoin, preprocesslogic,
                                                    preprocessconditions, identifier, preprocesstopic, 0, 1, 0, 120,
                                                    usemysql, tmlfilepath, pathtotmlattrs)

        # Check and log the result to see if the request was processed successfully
        print(f"Preprocessing result: {result}")

        # Try to interpret the result as JSON if it's valid JSON
        try:
            result_json = json.loads(result)
            print(f"Preprocessing Result JSON: {result_json}")
        except json.JSONDecodeError:
            print("Error: Response from VIPER is not a valid JSON")
        
        return result

    except Exception as e:
        print(f"ERROR during preprocessing: {e}")
        return e
     

#############################################################################################################
# SETUP THE TOPIC DATA STREAMS FOR EXAMPLE

maintopic = 'iot-mainstream'
preprocesstopic = 'iot-preprocess'
maintopic, producerid = datasetup(maintopic, preprocesstopic)
print(f"Topic setup complete. Main topic: {maintopic}, Producer ID: {producerid}")

async def startviper():
    print("Start Preprocess-iot-monitor-customdata Request:", datetime.datetime.now())
    while True:
        try:
            # Call function to send transaction data and start preprocessing
            sendtransactiondata(maintopic, producerid, VIPERPORT, -1, preprocesstopic)
            time.sleep(1)
        except Exception as e:
            print("ERROR:", e)
            continue

async def spawnvipers():
    loop.run_until_complete(startviper())

# Initialize an event loop for asynchronous execution
loop = asyncio.new_event_loop()
loop.create_task(spawnvipers())
asyncio.set_event_loop(loop)

loop.run_forever()
