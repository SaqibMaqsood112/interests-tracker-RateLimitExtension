
import boto3
import json
from sqlalchemy import create_engine



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
def establish_conn():
    try:
        param = "/dev/ds/aurora/postgres_ratelimits_only"
        conn = get_db_connection(param) # Establish Conn
        print("Aurora Connection Successfully Established")
    except Exception as e:
        print("Failed to establish Aurora Connection")
        print(e)
    return conn

# Function to add extension record in table
def add_ext(conn, event):
    try:
        add_ext_query = """ insert into ds.ds_sources_ratelimits_extensions (app_name, env_type, data_source,
                            daily_rate_limit_ext, valid_from_UTC, valid_to_UTC) values ('{}','{}', '{}', {}, '{}',
                            '{}')""".format(event['app_name'], event['env_type'], event['data_source'],
                            event['daily_rate_limit_ext'], event['from_data'], event['to_date'])
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
    try:
        if input_data_validation(event) == 200:
            conn = establish_conn()
            status = add_ext(conn, event)
            return {
                'statusCode': 200,
                #'isBase64Encoded':False,
                'body': status
                }
        else:
            return input_data_validation(event)
    except Exception as err:
        return {
            'statusCode': 400,
            #'isBase64Encoded':False,
            'body': 'Call Failed {0}'.format(err)
            }


# =============================================================================
## For local testing 
#event = {	"app_name": "interest tracker",
#	"env_type": "prod",
#	"data_source": "PeopleDataLab",
#	"daily_rate_limit_ext": 160,
#	"from_data": "2021-05-01",
#	"to_date": "2021-05-30"}
#context = ''
#lambda_handler(event, context)
# =============================================================================

