import elasticsearch
import csv
import json
import commands
import logging
from logging import handlers

#
# Initialization of variables
# Default output directory will be current directory
#
INDEX = None
SOURCE = None
ES_HOST = None
QUERY = None
FROM = None
SIZE = None
paginationEnabled = None
TIMEOUT = None
DELIMITER = None
OUTPUT_DIR = commands.getstatusoutput("pwd")[1]

# Logs are written in "`PWD`/es_csv.log"
logger = None

# ElasticSearch connection
es = None

def getESConnection():
    global es
    global TIMEOUT
    es = elasticsearch.Elasticsearch(ES_HOST, sniff_on_start=True,
					      sniff_on_connection_fail=True,
					      sniffer_timeout=60)
					      #timeout=TIMEOUT)
    
# End of getESConnection

#
## Module to query elasticsearch and store in CSV
#
def getDatatoCSV():

    global INDEX
    global SOURCE
    global ES_HOST
    global QUERY
    global FROM
    global SIZE
    global DELIMITER
    global OUTPUT_DIR
	
    print "index:", INDEX
    print "_source:", SOURCE
    print "elasticsearch:", ES_HOST
    print "DSL Query:", QUERY
    print "from:", FROM
    print "size:", SIZE
    print "output_dir:", OUTPUT_DIR
    print "CSV delimiter:", DELIMITER

    es_body = {
	"from": FROM,
	"size": SIZE,
    }
    if SOURCE != None:
	es_body['_source'] = SOURCE

    QUERY=json.loads("{"+QUERY+"}")
    es_body.update(QUERY)

    result = es.search(index=INDEX,
          	           body=es_body
                  )
    
    es_data = result['hits']['hits']
    #print source
    total_hits = result['hits']['total']
    
    logger.info('Total hits received :: %s', total_hits)
    
    # CsvWriter:
    csvfile = open(OUTPUT_DIR + "/es_csv.csv", "wb")
    if DELIMITER == "tab":
    	filewriter = csv.writer(csvfile, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    elif DELIMITER == "comma":
	filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    
    # Header
    if SOURCE == None:
	SOURCE = es_data[0]['_source'].keys()
    filewriter.writerow(SOURCE)

    # Write Contents/Rows from ES output to CSV
    for hit in es_data:
    	if len(hit['_source'].keys()) != len(SOURCE):
    		missed_keys = list(set(SOURCE) - set(hit['_source'].keys()))
    		for key in missed_keys:
    			hit['_source'][key] = "key_missed"
    
    	filewriter.writerow([hit['_source'][key] for key in SOURCE])
# End of getDatatoCSV

# Logger
def enableLog():
    global logger
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.INFO)

    es_tracer = logging.getLogger('elasticsearch.trace')
    es_tracer.setLevel(logging.INFO)

    logger = logging.getLogger('mainLog')
    logger.setLevel(logging.DEBUG)

    # create file handler
    fileHandler = logging.handlers.RotatingFileHandler(OUTPUT_DIR + '/es_csv.log',
                                                       maxBytes=10**6,
                                                       backupCount=7)
    fileHandler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileHandler.setFormatter(formatter)

    logger.addHandler(fileHandler)
    es_logger.addHandler(fileHandler)
    es_tracer.addHandler(fileHandler)
# End of enableLog


def readConfig():
    global INDEX
    global SOURCE
    global ES_HOST
    global QUERY
    global FROM
    global SIZE
    global DELIMITER
    global OUTPUT_DIR
    global TIMEOUT
    global paginationEnabled

    host = None
    port = None
    file = open('config.yml', 'r')
    for line in file.readlines():
	if line.rstrip().startswith('#'):
	    continue
	else:
	    temp=line.rstrip().split('=')
	    if temp[0] == "INDEX":
		INDEX = temp[1]
            elif temp[0] == "SOURCE":
                SOURCE = temp[1]
            elif temp[0] == "ES_HOST":
                host = temp[1]
            elif temp[0] == "ES_PORT":
                port = temp[1]
            elif temp[0] == "QUERY":
                QUERY = temp[1]
            elif temp[0] == "FROM":
                FROM = temp[1]
            elif temp[0] == "SIZE":
                SIZE = temp[1]
            elif temp[0] == "OUTPUT_DIR":
                OUTPUT_DIR = temp[1]
            elif temp[0] == "TIMEOUT":
                TIMEOUT = temp[1]
            elif temp[0] == "DELIMITER":
                DELIMITER = temp[1]
            elif temp[0] == "EnablePagination":
                paginationEnabled = temp[1]

    ES_HOST = host + ":" + port
    if SOURCE != None:
	SOURCE = list(SOURCE.strip().split(','))

# End of readConfig()


def main():
    global logger

    enableLog()
    logger.debug('Script started')

    readConfig()
    logger.debug('Config parameters are read')
    if INDEX == None or INDEX == "":
	logger.error('Index is mandatory paramter')
	logger.info('Provide Index in config.yml and then re-run')
	print "INDEX IS MANDATORY PARAMTER\nProvide Index in config.yml and then re-run\n"
	exit()
    
    getESConnection()
    logger.debug('Connected to ElasticSearch')

    getDatatoCSV()
    logger.debug('Data is exported to CSV')

    logger.debug('Script Finished')
# End of main

if __name__ == "__main__":
	main()

#
## End of Script
#
