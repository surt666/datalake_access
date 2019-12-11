import json
import psycopg2
import boto3


def get_item(ds, client):
    return client.get_item(TableName='dsrelations', Key={'pk': {'S': ds['pk']['S']}, 'sk': {'S': ds['pk']['S']}})['Item']


def get_sources(perms, type, client):
    filtered_perms = list(filter(lambda x: x['reltype']['S'] == type, perms))
    return list(map(lambda ds: get_item(ds, client), filtered_perms))


def create_policy(perms, client):
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
             "Effect": "Allow",
             "Action": [
                    "s3:GetBucketLocation",
                    "s3:ListAllMyBuckets"
             ],
             "Resource": "*"
            }
        ]
    }
    s3_list_policy = {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": []
    }
    s3_write_policy = {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject"
      ],
      "Resource": []
    }
    s3_read_policy = {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
      ],
      "Resource": []
    }
    read_sources = list(filter(lambda x: x['type'] == {'S': 's3'}, get_sources(perms, 'reads', client)))
    write_sources = list(filter(lambda x: x['type'] == {'S': 's3'}, get_sources(perms, 'writes', client)))
    s3_list_policy['Resource'] = list(set(list(map(lambda x: x['bucket']['S'], read_sources)) + list(map(lambda x: x['bucket']['S'], write_sources))))
    s3_read_policy['Resource'] = list(map(lambda x: x['bucket']['S'] + x['prefix']['S'] + '/*', read_sources))
    s3_write_policy['Resource'] = list(map(lambda x: x['bucket']['S'] + x['prefix']['S'] + '/*', write_sources))
    s3_policy['Statement'].append(s3_list_policy)
    if read_sources:
        s3_policy['Statement'].append(s3_read_policy)
    if write_sources:
        s3_policy['Statement'].append(s3_write_policy)
    return s3_policy
    
    
def set_redshift_grants(dbuser, dbpw, dbhost, dbname, dbport, perms):
    con = psycopg2.connect(dbname=dbname, host=dbhost, port=dbport, user=dbuser, password=dbpw)
    con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT 1 FROM pg_user WHERE usename='" + perms['user'] + "';")
    r = cur.fetchall()
    if r[0][0] != 1:
        cur.execute("create user " + perms['user'] + " password disable;") 
    cur.execute("grant usage on schema " + perms['schema'] + " to " + perms['user'] + ";")
    cur.execute("grant " + perms['grant'] + " on table " + perms['schema'] + "." + perms['table'] + " to " + perms['user'] + ";")
    cur.close()
    con.close()
    
def purge(m):
    m['creds'].pop('ResponseMetadata')
    m['creds'].pop('Expiration')
    return m
    
def create_redshift_access(perms, ddb, redshift, dbuser, dbpw, user):
    read_sources = list(filter(lambda x: x['type'] == {'S': 'redshift'}, get_sources(perms, 'reads', ddb)))
    write_sources = list(filter(lambda x: x['type'] == {'S': 'redshift'}, get_sources(perms, 'writes', ddb)))    
    clusters = list(set(list(map(lambda x: (x['clusterid']['S'], x['dbname']['S']), read_sources)) + list(map(lambda x: (x['clusterid']['S'], x['dbname']['S']), write_sources))))
    map(lambda x: set_redshift_grants(dbuser, dbpw, x['endpoint']['S'], x['dbname']['S'], x['port']['S'], {'user': user, 'schema': x['schema']['S'], 'table': x['table']['S'], 'grant': 'select'}), read_sources)
    map(lambda x: set_redshift_grants(dbuser, dbpw, x['endpoint']['S'], x['dbname']['S'], x['port']['S'], {'user': user, 'schema': x['schema']['S'], 'table': x['table']['S'], 'grant': 'all'}), write_sources)
    res = list(map(lambda x: {'creds': redshift.get_cluster_credentials(
                                                        DbUser=user,
                                                        DbName=x[1],
                                                        ClusterIdentifier=x[0],
                                                        DurationSeconds=3600,
                                                        AutoCreate=False),
                              'cluster': x}, clusters))
    r = list(map(purge, res))  
    return r                                                  
    

def lambda_handler(event, context):
    cognito_client = boto3.client('cognito-idp')
    ssm = boto3.client('ssm')
    params = ssm.get_parameters_by_path(Path='/nnedl/', Recursive=True)['Parameters']
    role = list(filter(lambda x: x['Name'] == '/nnedl/role', params))[0]['Value']
    dbuser = list(filter(lambda x: x['Name'] == '/nnedl/dbuser', params))[0]['Value']
    dbpw = list(filter(lambda x: x['Name'] == '/nnedl/dbpw', params))[0]['Value']
    clientid = list(filter(lambda x: x['Name'] == '/nnedl/cognito_client_id', params))[0]['Value']
    token = cognito_client.initiate_auth(
        AuthFlow='USER_PASSWORD_AUTH',
        AuthParameters={
            'USERNAME': event['username'],
            'PASSWORD': event['password']
        },
        ClientId=clientid
    )['AuthenticationResult']['AccessToken']
    user = cognito_client.get_user(AccessToken=token)['Username']
    ddb_client = boto3.client('dynamodb')
    response = ddb_client.query(
        TableName='dsrelations',
        IndexName='sk-reltype-index',
        Select='ALL_ATTRIBUTES',
        KeyConditionExpression='sk= :sk',
        ExpressionAttributeValues={
            ':sk': {
                'S': user,
            }
        }
    )['Items']
    policy = create_policy(response, ddb_client)
    p = json.dumps(policy)
    sts_client = boto3.client('sts')
    response2 = sts_client.assume_role(
        RoleArn=role,
        RoleSessionName='datalake',
        Policy=p)
    redshift_client = boto3.client('redshift')
    redshift_creds = create_redshift_access(response, ddb_client, redshift_client, dbuser, dbpw, user)
    resp = {}
    resp['s3'] = response2['Credentials']
    resp['redshift'] = redshift_creds
    return json.dumps(resp, default=str)
