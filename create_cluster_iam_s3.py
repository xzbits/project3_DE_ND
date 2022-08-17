import configparser
from aws_identity_redshift_cluster_config import AWSIdentity, AWSCluster
import json
import time
import boto3


if __name__ == "__main__":
    aws_access_info = AWSIdentity('phong_aws_credentials.cfg')
    cluster_info = AWSCluster('dwh.cfg')
    session = boto3.Session(aws_access_key_id=aws_access_info.aws_identity['aws_key'],
                            aws_secret_access_key=aws_access_info.aws_identity['aws_secret'],
                            aws_session_token=aws_access_info.aws_identity['aws_token'])

    s3 = session.resource('s3', region_name=cluster_info.cluster_info['dwh_region'])
    ec2 = session.resource('ec2', region_name=cluster_info.cluster_info['dwh_region'])
    iam = session.client('iam', region_name=cluster_info.cluster_info['dwh_region'])
    redshift = session.client('redshift', region_name=cluster_info.cluster_info['dwh_region'])

    # Generate IAM role
    # 1.1 Create the role,
    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(Path='/',
                                  RoleName=cluster_info.cluster_info['dwh_iam_role_name'],
                                  Description="Allows Redshift clusters to call AWS services on your behalf.",
                                  AssumeRolePolicyDocument=json.dumps(
                                      {'Statement': [{'Action': 'sts:AssumeRole',
                                                      'Effect': 'Allow',
                                                      'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                       'Version': '2012-10-17'})
                                  )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    attaching = iam.attach_role_policy(RoleName=cluster_info.cluster_info['dwh_iam_role_name'],
                                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                       )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=cluster_info.cluster_info['dwh_iam_role_name'])['Role']['Arn']
    print('Finish create IAM role: {}'.format(roleArn))

    # Generate cluster
    print('2.1 Request to AWS to create Cluster')
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=cluster_info.cluster_info['dwh_cluster_type'],
            NodeType=cluster_info.cluster_info['dwh_node_type'],
            NumberOfNodes=int(cluster_info.cluster_info['dwh_num_nodes']),

            # Identifiers & Credentials
            DBName=cluster_info.cluster_info['dwh_db'],
            ClusterIdentifier=cluster_info.cluster_info['dwh_cluster_identifier'],
            MasterUsername=cluster_info.cluster_info['dwh_db_user'],
            MasterUserPassword=cluster_info.cluster_info['dwh_db_password'],

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)
    print('Request Done!!!')

    # Checking Cluster available status
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_info.cluster_info['dwh_cluster_identifier']
                                                )['Clusters'][0]
    print("3.1 Checking cluster status ...")
    while myClusterProps['ClusterStatus'] != 'available':
        print('Current Cluster status is {}'.format(myClusterProps['ClusterStatus']))
        print("Checking status again after 10 seconds")
        time.sleep(10)
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=cluster_info.cluster_info['dwh_cluster_identifier']
        )['Clusters'][0]
    print("Current cluster status is {}".format(myClusterProps['ClusterStatus']))

    # Update Cluster ENDPOINT and role ARN
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

    config_update = configparser.ConfigParser()
    config_update.read('dwh.cfg')
    config_update.set('CLUSTER', 'HOST', DWH_ENDPOINT)
    config_update.set('IAM_ROLE', 'ARN', DWH_ROLE_ARN)

    with open('dwh.cfg', 'w') as conf:
        config_update.write(conf)

    # Open an incoming TCP port to access the cluster endpoint
    print('4.1 Open an incoming TCP port to access the cluster endpoint')
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(cluster_info.cluster_info['dwh_port']),
            ToPort=int(cluster_info.cluster_info['dwh_port'])
        )
    except Exception as e:
        print(e)

    print('CLUSTER IS CREATED SUCCESSFULLY!!!')
