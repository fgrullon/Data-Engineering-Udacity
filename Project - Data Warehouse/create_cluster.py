import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError
import psycopg2
import time

def create_iam_role(iam, DWH_IAM_ROLE_NAME):

    try:
        print('Creating a new IAM Role')
        dwhRole=iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Allow Redshift cluster to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement':[{'Action':'sts:AssumeRole',
                              'Effect':'Allow',
                              'Principal':{'Service':'redshift.amazonaws.com'}}],
                'Version':'2012-10-17'})
        )


    except Exception as e:
        print(e)

    print('Attach Read Only Access Policy to Role')

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                          PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
                          )['ResponseMetadata']['HTTPStatusCode']
    
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    return roleArn

def create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    try:
        response = redshift.create_cluster(        
            ClusterType= DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn]

        )
        print('Cluster Created')
    except Exception as e:
        print(e)
        

    
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = [
        "ClusterIdentifier", 
        "NodeType", 
        "ClusterStatus", 
        "MasterUsername", 
        "DBName", 
        "Endpoint", 
        "NumberOfNodes", 
        'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    Retrieve Redshift clusters properties
    '''


    while True:
        ClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_properties = prettyRedshiftProps(ClusterProps)
        cluster_status = cluster_properties.iloc[2]['Value']
        if cluster_status == 'available':
            print('Cluster Available')
            break
        # If is not available we wait 30 seconds
        print('Waiting for cluster to be available')
        time.sleep(30)
    
    return ClusterProps


def open_ports(ec2, myClusterProps, DWH_PORT):
    '''
    Open an incoming  TCP port to access the cluster ednpoint
    '''

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
        
def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('IAM_ROLE','KEY')
    SECRET                 = config.get('IAM_ROLE','SECRET')

    DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("CLUSTER_DB","DWH_DB")
    DWH_DB_USER            = config.get("CLUSTER_DB","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER_DB","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER_DB","DWH_PORT")
    

    DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({
        "Param": [
            "DWH_CLUSTER_TYPE", 
            "DWH_NUM_NODES", 
            "DWH_NODE_TYPE", 
            "DWH_CLUSTER_IDENTIFIER", 
            "DWH_DB", 
            "DWH_DB_USER", 
            "DWH_DB_PASSWORD", 
            "DWH_PORT", 
            "DWH_IAM_ROLE_NAME"
        ],
        "Value": [
            DWH_CLUSTER_TYPE, 
            DWH_NUM_NODES, 
            DWH_NODE_TYPE, 
            DWH_CLUSTER_IDENTIFIER, 
            DWH_DB, 
            DWH_DB_USER, 
            DWH_DB_PASSWORD, 
            DWH_PORT, 
            DWH_IAM_ROLE_NAME
        ]
    })
    

    ec2 = boto3.resource('ec2',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

    s3 = boto3.resource('s3',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

    iam = boto3.client('iam',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)


    redshift = boto3.client('redshift',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
    
    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

    create_cluster(redshift, 
                   roleArn, 
                   DWH_CLUSTER_TYPE, 
                   DWH_NODE_TYPE, 
                   DWH_NUM_NODES, 
                   DWH_DB, 
                   DWH_CLUSTER_IDENTIFIER, 
                   DWH_DB_USER, 
                   DWH_DB_PASSWORD
                  )

    myClusterProps = get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)

    open_ports(ec2, myClusterProps, DWH_PORT)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER_DB'].values()))

    cur = conn.cursor()

    print('Connected')

    conn.close()


if __name__ == "__main__":
    main()