import configparser
import os 

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY='KEY'
SECRET='SECRET'

CLUSTER_DB_HOST=config.get('CLUSTER','DB_HOST')
CLUSTER_DB=config.get('CLUSTER','DB_NAME')
CLUSTER_DB_USER=config.get('CLUSTER','DB_USER')
CLUSTER_DB_PASSWORD=config.get('CLUSTER','DB_PASSWORD')
CLUSTER_PORT=config.get('CLUSTER','DB_PORT')
CLUSTER_ENDPOINT=config.get('CLUSTER','ENDPOINT')

S3_LOG_DATA=config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA=config.get('S3', 'SONG_DATA')

IAM_ROLE_ARN=config.get('IAM_ROLE','ARN')


def connect_redshift():
    conn_string="postgresql://{}:{}@{}:{}/{}".format(
        CLUSTER_DB_USER, CLUSTER_DB_PASSWORD, CLUSTER_ENDPOINT, CLUSTER_PORT,CLUSTER_DB
    )
    print(conn_string)
    # %sql $conn_string
