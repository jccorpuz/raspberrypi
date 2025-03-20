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
#from joblib import parallel_backend
import sys
import multiprocessing
import pandas as pd
#import concurrent.futures
import asyncio
# Uncomment IF using Jupyter notebook
nest_asyncio.apply()
import datetime
import time
import os

basedir = os.environ['userbasedir'] 

# Set Global Host/Port for VIPER - You may change this to fit your configuration
VIPERHOST=''
VIPERPORT=''
HTTPADDR='https://'


#############################################################################################################
#                                      STORE VIPER TOKEN
def getparams():
     global VIPERHOST, VIPERPORT, HTTPADDR
     with open(basedir + "/Viper-preprocess/admin.tok", "r") as f:
        VIPERTOKEN=f.read()

     if VIPERHOST=="":
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

     # Replication factor for Kafka redundancy
     replication = 1
     # Number of partitions for joined topic
     numpartitions = 3
     # Enable SSL/TLS communication with Kafka
     enabletls = 1
     # If brokerhost is empty then this function will use the brokerhost address in your
     # VIPER.ENV in the field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
     brokerhost = ''
     # If this is -999 then this function uses the port address for Kafka in VIPER.ENV in the
     # field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
     brokerport = -999
     # If you are using a reverse proxy to reach VIPER then you can put it here - otherwise if
     # empty then no reverse proxy is being used
     microserviceid = ''

     description = "TML Use Case"

     # Create the 4 topics in Kafka concurrently - it will return a JSON array
     result = maadstml.vipercreatetopic(VIPERTOKEN, VIPERHOST, VIPERPORT, maintopic, companyname,
                                        myname, myemail, mylocation, description, enabletls,
                                        brokerhost, brokerport, numpartitions, replication,
                                        microserviceid)

     # Load the JSON array in variable y
     try:
         y = json.loads(result, strict='False')
     except Exception as e:
         y = json.loads(result)

     for p in y:  # Loop through the JSON and grab the topic and producerids
         pid = p['ProducerId']
         tn = p['Topic']

     result = maadstml.vipercreatetopic(VIPERTOKEN, VIPERHOST, VIPERPORT, preprocesstopic, companyname,
                                        myname, myemail, mylocation, description, enabletls,
                                        brokerhost, brokerport, numpartitions, replication,
                                        microserviceid)

     return tn, pid


def sendtransactiondata(maintopic, mainproducerid, VIPERPORT, index, preprocesstopic):
    #############################################################################################################
    #                                    PREPROCESS DATA STREAMS

    maxrows = 500
    offset = -1
    topic = maintopic
    producerid = mainproducerid
    brokerhost = ''
    brokerport = -999
    microserviceid = ''

    preprocessconditions = ''
    
    # This is where we specify the logic for preprocessing
    preprocesslogic = 'MIN,MAX,COUNT,VARIANCE,OUTLIERS,ANOMPROB'

    # If your preprocess logic includes certain streams, make sure they are specified here
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
        # Print debug info to check the preprocess logic
        print(f"Sending preprocessing with logic: {preprocesslogic}")

        # Send the data to preprocess
        result = maadstml.viperpreprocesscustomjson(VIPERTOKEN, VIPERHOST, VIPERPORT, topic, producerid, offset,
                                                    jsoncriteria, 1, maxrows, 1, 70, brokerhost, brokerport,
                                                    microserviceid, -999, streamstojoin, preprocesslogic,
                                                    preprocessconditions, identifier, preprocesstopic, 0, 1, 0, 120,
                                                    usemysql, tmlfilepath, pathtotmlattrs)

        # Print the result to verify if preprocessing was successful
        print(f"Preprocessing result: {result}")
        return result

    except Exception as e:
        print(f"ERROR during preprocessing: {e}")
        return e
     

#############################################################################################################
# SETUP THE TOPIC DATA STREAMS FOR WALMART EXAMPLE

maintopic = 'iot-mainstream'
preprocesstopic = 'iot-preprocess'
maintopic, producerid = datasetup(maintopic, preprocesstopic)
print(f"Topic setup complete. Main topic: {maintopic}, Producer ID: {producerid}")

async def startviper():
    print("Start Preprocess-iot-monitor-customdata Request:", datetime.datetime.now())
    while True:
        try:
            sendtransactiondata(maintopic, producerid, VIPERPORT, -1, preprocesstopic)
            time.sleep(1)
        except Exception as e:
            print("ERROR:", e)
            continue
   

async def spawnvipers():
    loop.run_until_complete(startviper())
  
loop = asyncio.new_event_loop()
loop.create_task(spawnvipers())
asyncio.set_event_loop(loop)

loop.run_forever()
