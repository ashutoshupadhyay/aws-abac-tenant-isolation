import json
import os
import time 

def lambda_handler(event, context):
    import boto3
    bucket_name     =   os.environ['s3_bucket_name']

    try:
        login_tenant_id =   event['login_tenant_id']
        data_tenant_id  =   event['s3_tenant_home']
    except:
        return {
            'statusCode': 400,
            'body': 'Error in reading parameters'
        }

    prefix_of_role  =   'assumeRole'
    file_name       =   'tenant.info' + '-' + data_tenant_id

    # create an STS client object that represents a live connection to the STS service
    sts_client = boto3.client('sts')
    account_of_role = sts_client.get_caller_identity()['Account']
    role_to_assume  =   'arn:aws:iam::' + account_of_role + ':role/' + prefix_of_role + '-' + login_tenant_id

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    RoleSessionName = 'AssumeRoleSession' + str(time.time()).split(".")[0] + str(time.time()).split(".")[1]
    try:
        assumed_role_object = sts_client.assume_role(
            RoleArn         = role_to_assume, 
            RoleSessionName = RoleSessionName, 
            DurationSeconds = 900) #15 minutes

    except:
        return {
            'statusCode': 400,
            'body': 'Error in assuming the role ' + role_to_assume + ' in account ' + account_of_role
        }

    # From the response that contains the assumed role, get the temporary 
    # credentials that can be used to make subsequent API calls
    credentials=assumed_role_object['Credentials']
    
    # Use the temporary credentials that AssumeRole returns to make a connection to Amazon S3  
    s3_resource=boto3.resource(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

    try:
        obj = s3_resource.Object(bucket_name, data_tenant_id + "/" + file_name)
        return {
            'statusCode': 200,
            'body': obj.get()['Body'].read()
        }
    except:
        return {
            'statusCode': 400,
            'body': 'error in reading s3://' + bucket_name + '/' + data_tenant_id + '/' + file_name
        }
