import configparser
import os 

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

CLUSTER_DB_HOST=config.get('CLUSTER','DB_HOST')
CLUSTER_DB=config.get('CLUSTER','DB_NAME')
CLUSTER_DB_USER=config.get('CLUSTER','DB_USER')
CLUSTER_DB_PASSWORD=config.get('CLUSTER','DB_PASSWORD')
CLUSTER_PORT=config.get('CLUSTER','DB_PORT')
CLUSTER_TYPE=config.get('CLUSTER','TYPE')
CLUSTER_NODE_TYPE=config.get('CLUSTER','NODE_TYPE')
CLUSTER_IDENTIFIER=config.get('CLUSTER','IDENTIFIER')
CLUSTER_NUM_NODES=config.get('CLUSTER','NUM_NODES')
CLUSTER_ENDPOINT='ENDPOINT'

S3_LOG_DATA=config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA=config.get('S3', 'SONG_DATA')

IAM_ROLE_ARN=config.get('IAM_ROLE','ARN')

def set_cluster_endpoint(endpoint):
    CLUSTER_ENDPOINT=endpoint