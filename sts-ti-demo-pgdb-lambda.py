import boto3
import pg8000
import os
import time
import ssl

connection = None
assumed_role_object = None
rds_client = None

def assume_role(event):
    global assumed_role_object
    try:
        RolePrefix  = os.environ.get("RolePrefix")
        LoginTenant = event['login_tenant_id']
    
        # create an STS client object that represents a live connection to the STS service
        sts_client      = boto3.client('sts')
        # Prepare input parameters
        role_to_assume  = 'arn:aws:iam::' + sts_client.get_caller_identity()['Account'] + ':role/' + RolePrefix + '-' + LoginTenant
        RoleSessionName = 'AssumeRoleSession' + str(time.time()).split(".")[0] + str(time.time()).split(".")[1]
    
        # Call the assume_role method of the STSConnection object and pass the role ARN and a role session name.
        assumed_role_object = sts_client.assume_role(
            RoleArn         =   role_to_assume, 
            RoleSessionName =   RoleSessionName,
            DurationSeconds =   900) #15 minutes 
        
        return assumed_role_object['Credentials']
    except Exception as e:
        print({'Role assumption failed!': {'role': role_to_assume, 'Exception': 'Failed due to :{0}'.format(str(e))}})
        return None

def get_connection(event):
    global rds_client
    creds = assume_role(event)

    try:
        # create an RDS client using assumed credentials
        rds_client = boto3.client('rds',
            aws_access_key_id       = creds['AccessKeyId'],
            aws_secret_access_key   = creds['SecretAccessKey'],
            aws_session_token       = creds['SessionToken'])

        # Read the environment variables and event parameters
        DBEndPoint   = os.environ.get('DBEndPoint')
        DatabaseName = os.environ.get('DatabaseName')
        DBUserName   = event['dbuser']

        # Generates an auth token used to connect to a db with IAM credentials.
        pwd = rds_client.generate_db_auth_token(
            DBHostname=DBEndPoint, Port=5432, DBUsername=DBUserName, Region='us-west-2'
        )

        ssl_context             = ssl.SSLContext()
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations('rds-ca-2019-root.pem')

        # create a database connection
        conn = pg8000.connect(
            host        =   DBEndPoint,
            user        =   DBUserName,
            database    =   DatabaseName,
            password    =   pwd,
            ssl_context =   ssl_context)
        
        return conn
    except Exception as e:
        print ({'Database connection failed!': {'Exception': "Failed due to :{0}".format(str(e))}})
        return None

def execute_sql(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [str(desc[0]) for desc in cursor.description]
        results = []
        for res in cursor:
            results.append(dict(zip(columns, res)))
        cursor.close()
        retry = False
        return results    
    except Exception as e:
        print ({'Execute SQL failed!': {'Exception': "Failed due to :{0}".format(str(e))}})
        return None


def lambda_handler(event, context):
    global connection
    try:
        connection = get_connection(event)
        if connection is None:
            return {'statusCode': 400, "body": "Error in database connection!"}

        response = {'statusCode':200, 'body': {
            'db & user': execute_sql(connection, 'SELECT CURRENT_DATABASE(), CURRENT_USER'), \
            'data from tenant_metadata': execute_sql(connection, 'SELECT * FROM tenant_metadata')}}
        return response
    except Exception as e:
        try:
            connection.close()
        except Exception as e:
            connection = None
        return {'statusCode': 400, 'statusDesc': 'Error!', 'body': 'Unhandled error in Lambda Handler.'}
