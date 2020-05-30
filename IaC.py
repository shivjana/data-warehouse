import boto3
import configparser
import json
import time
import pandas as pd

def create_iam_role(KEY,SECRET,DWH_IAM_ROLE_NAME):
    """This module creates the IAM role and attaches policy"""
    
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    try:
        print("Creating a new IAM Role") 
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
        
    print("Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    print("Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    return roleArn,iam

def create_redshift_cluster(KEY,SECRET,roleArn,**config):
    """This module creates redshift cluster and opens the incoming TCP port to access the cluster endpoint"""
    
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    
    try:
        print("Creating Redshift Cluster")
        response = redshift.create_cluster(        
            ClusterType=config['DWH_CLUSTER_TYPE'],
            NodeType=config['DWH_NODE_TYPE'],
            NumberOfNodes=int(config['DWH_NUM_NODES']),

            DBName=config['DWH_DB'],
            ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=config['DWH_DB_USER'],
            MasterUserPassword=config['DWH_DB_PASSWORD'],

            IamRoles=[roleArn]  
        )
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
        while myClusterProps['ClusterStatus'] != 'available':
            time.sleep(120)
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
    except Exception as e:
        print(e)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
    
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['DWH_PORT']),
            ToPort=int(config['DWH_PORT'])
        )
    except Exception as e:
        print(e)
        
    return myClusterProps,redshift

def delete_redshift_cluster(KEY,SECRET,**config):
    """This Module deletes the cluster and IAM roles created"""
    
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    print("Deleting redshift cluster")
    iam.detach_role_policy(RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME'], PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
    iam.delete_role(RoleName=config['CLUSTER']['DWH_IAM_ROLE_NAME'])
    myClusterProps = redshift.delete_cluster(ClusterIdentifier=config['CLUSTER']['DWH_CLUSTER_IDENTIFIER'],SkipFinalClusterSnapshot=True)
    time.sleep(300)
    
    
def main():
    """This is the main module that loads and reads the config file and passes key and secret as values for creating the IAM role and eventually the redshift cluster"""
    
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('dwh.cfg')
    KEY=config.get('AWS','KEY')
    SECRET= config.get('AWS','SECRET')
    
    delete_redshift_cluster(KEY,SECRET,**config)
    
if __name__ == "__main__":
    main()    

