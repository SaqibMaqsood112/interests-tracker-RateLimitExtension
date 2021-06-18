
import boto3
import json
from sqlalchemy import create_engine
import yaml


## Reading in Config File
def read_config(file_path= 'config.yaml'):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

## Building Aurora Connection
# Get DB Creds
def get_db_creds(ssm_param):
    ssm = boto3.client('ssm')
    response = ssm.get_parameter(Name=ssm_param, WithDecryption=True)
    creds = json.loads(response['Parameter']['Value'])
    return creds

# Get DB Connection String
def get_db_connection(ssm_param):
    creds = get_db_creds(ssm_param)
    connect = "postgresql+psycopg2://{}:{}@{}:5432/{}".format(
        creds['user'],
        creds['password'],
        creds['host'],
        creds['database'])
    conn = create_engine(connect)
    return conn

# Establish connection
def establish_conn(config):
    try:
        param = config['DB Connection']['param_path']
        #param = "/dev/ds/aurora/postgres_ratelimits_only"
        conn = get_db_connection(param) # Establish Conn
        print("Aurora Connection Successfully Established")
        return conn
    except Exception as e:
        print("Failed to establish Aurora Connection")
        print(e)
        return ("Failed to establish Aurora Connection")

# Function to add extension record in table
def add_ext(conn, event):
    try:
        add_ext_query = """ insert into ds.ds_sources_ratelimits_extensions (app_name, env_type, data_source,
                            daily_rate_limit_ext, valid_from_UTC, valid_to_UTC) values ('{}','{}', '{}', {}, '{}',
                            '{}')""".format(event['app_name'], event['env_type'], event['data_source'],
                            event['daily_rate_limit_ext'], event['from_date'], event['to_date'])
        conn.execute(add_ext_query)
        status = "Specified rate limit extensions has been successfully registered"
        print("Specified rate limit extensions has been successfully registered")
    except Exception as e:
        status = "Failed to register the specified rate limit extension"
        print("Failed to register the specified rate limit extension", exc_info = True)
        print(e)
    return status


# Function to do request validations
def input_data_validation(event):
    available_apps = {
        "interest tracker": {"data_source": ["PeopleDataLab"], "env_type": ["prod", "dev"]}
        }
    # Check App Name
    if event["app_name"] not in list(available_apps.keys()):
        allowed_apps = ' and'.join(', '.join(list(available_apps.keys())).rsplit(',',1))
        hv = 'application is' if len(available_apps)==1 else 'applications are'
        val_status = "Invalid application name. Allowed {} {}".format(hv, allowed_apps)
        return val_status
    # Check data source
    if event["data_source"] not in available_apps[event["app_name"]]["data_source"]:
        available_ds = available_apps[event["app_name"]]["data_source"]
        allowed_ds = ' and'.join(', '.join(available_ds).rsplit(',',1))
        hv = 'data source is' if len(available_ds)==1 else 'data sources are'
        val_status = "Invalid data source name. Allowed {} {}".format(hv, allowed_ds)
        return val_status
    # Check environment name
    if event["env_type"] not in available_apps[event["app_name"]]["env_type"]:
        available_env = available_apps[event["app_name"]]["env_type"]
        allowed_env = ' and'.join(', '.join(available_env).rsplit(',',1))
        hv = 'environment type is' if len(available_env)==1 else 'environment types are'
        val_status = "Invalid envrionment type name. Allowed {} {}".format(hv, allowed_env)
        return val_status
    return 200


# Calling the function
def lambda_handler(event, context):
    # To handle lambda proxy based payload
    try:
        print("Event Received")
        print(event)
        if 'body' in event:
            request = json.loads(event['body'])
        else:
            request = event
    except Exception as e:
        return {
            'statusCode': 400,
            #'isBase64Encoded':False,
            'body': 'Call Failed {0}'.format(e)
            }
    
    # Rest of the function
    try:
        if input_data_validation(request) == 200:
            config = read_config()
            conn = establish_conn(config)
            status = add_ext(conn, request)
            return {
                'statusCode': 200,
                #'isBase64Encoded':False,
                'body': status
                }
        else:
            return {
                'statusCode': 422,
                #'isBase64Encoded':False,
                'body': input_data_validation(request)
                }
    except Exception as err:
        return {
            'statusCode': 400,
            #'isBase64Encoded':False,
            'body': 'Call Failed {0}'.format(err)
            }




# =============================================================================
# For local testing

# event = {"app_name": "interest tracker",
#  	"env_type": "prod1",
#  	"data_source": "PeopleDataLab",
#  	"daily_rate_limit_ext": 1132,
#  	"from_date": "2021-05-01",
#  	"to_date": "2021-05-30"}
# context = ''
# lambda_handler(event, context)

# =============================================================================

# =============================================================================
# For Lambda Proxy based Payload

# event = {'resource': '/', 'path': '/', 'httpMethod': 'POST', 'headers': None, 'multiValueHeaders': None, 'queryStringParameters': None, 'multiValueQueryStringParameters': None, 'pathParameters': None, 'stageVariables': None, 'requestContext': {'resourceId': 'qn3iort26g', 'resourcePath': '/', 'httpMethod': 'POST', 'extendedRequestId': 'BE4NgF-vvHcFsiA=', 'requestTime': '17/Jun/2021:15:43:05 +0000', 'path': '/', 'accountId': '664813711721', 'protocol': 'HTTP/1.1', 'stage': 'test-invoke-stage', 'domainPrefix': 'testPrefix', 'requestTimeEpoch': 1623944585475, 'requestId': '75513268-78b4-41ba-bd0a-70b211d27087', 'identity': {'cognitoIdentityPoolId': None, 'cognitoIdentityId': None, 'apiKey': 'test-invoke-api-key', 'principalOrgId': None, 'cognitoAuthenticationType': None, 'userArn': 'arn:aws:sts::664813711721:assumed-role/sendoso-datateam-admin-all/saqib.maqsood', 'apiKeyId': 'test-invoke-api-key-id', 'userAgent': 'aws-internal/3 aws-sdk-java/1.11.1030 Linux/5.4.102-52.177.amzn2int.x86_64 OpenJDK_64-Bit_Server_VM/25.292-b10 java/1.8.0_292 vendor/Oracle_Corporation cfg/retry-mode/legacy', 'accountId': '664813711721', 'caller': 'AROAZVSP4GFUUP52JUSB5:saqib.maqsood', 'sourceIp': 'test-invoke-source-ip', 'accessKey': 'ASIAZVSP4GFU3VIHWD3A', 'cognitoAuthenticationProvider': None, 'user': 'AROAZVSP4GFUUP52JUSB5:saqib.maqsood'}, 'domainName': 'testPrefix.testDomainName', 'apiId': 'yqza956v43'}, 'body': '{\n  "app_name": "interest tracker",\n  "env_type": "prod1",\n  "data_source": "PeopleDataLab",\n  "daily_rate_limit_ext": 1125,\n  "from_date": "2021-05-01",\n  "to_date": "2021-05-30"\n}', 'isBase64Encoded': False}

# =============================================================================
