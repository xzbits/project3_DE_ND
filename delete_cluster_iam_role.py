import boto3
from aws_identity_redshift_cluster_config import AWSIdentity, AWSCluster


if __name__ == "__main__":
    aws_identity = AWSIdentity('aws_credentials.cfg')
    aws_cluster = AWSCluster('dwh.cfg')
    session = boto3.Session(aws_access_key_id=aws_identity.aws_identity['aws_key'],
                            aws_secret_access_key=aws_identity.aws_identity['aws_secret'],
                            aws_session_token=aws_identity.aws_identity['aws_token'])

    iam = session.client('iam', region_name=aws_cluster.cluster_info['dwh_region'])
    redshift = session.client('redshift', region_name=aws_cluster.cluster_info['dwh_region'])

    # Delete cluster
    redshift.delete_cluster(ClusterIdentifier=aws_cluster.cluster_info['dwh_cluster_identifier'],
                            SkipFinalClusterSnapshot=True)

    # Delete IAM role
    iam.detach_role_policy(RoleName=aws_cluster.cluster_info['dwh_iam_role_name'],
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=aws_cluster.cluster_info['dwh_iam_role_name'])

