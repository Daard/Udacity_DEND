import pandas as pd
import boto3
import json
import psycopg2

from botocore.exceptions import ClientError
import configparser


def create_IAM_role(iam, DWH_IAM_ROLE_NAME):
    '''
    Create IAM role for Redshift
    '''
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print('1.2 Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print('1.3 Get the IAM role ARN')
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    
    return roleArn

def create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, 
                   DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    '''
    create Redshift cluster
    '''

    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)
        
        
def describe(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    return simple and pretty Redshift description
    '''
    
    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName",
                      "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
#     DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
#     DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
#     print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
#     print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    
    return myClusterProps, prettyRedshiftProps(myClusterProps)


def open_ports(ec2, myClusterProps, DWH_PORT):
    '''
    Update clusters security group to allow access through redshift port
    '''
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

