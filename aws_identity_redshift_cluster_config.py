import configparser


class AWSIdentity:
    def __init__(self, config_file_path):
        """
        Get AWS KEY, SECRET, and TOKEN in config file
        :param config_file_path: Path lead to AWS credential config file
        """
        self.config = configparser.ConfigParser()
        self.config.read_file(open(config_file_path))
        self.aws_identity = dict()

        self.__get_aws_identity()

    def __get_aws_identity(self):
        self.aws_identity['aws_key'] = self.config.get('AWS', 'KEY')
        self.aws_identity['aws_secret'] = self.config.get('AWS', 'SECRET')
        self.aws_identity['aws_token'] = self.config.get('AWS', 'TOKEN')


class AWSCluster:
    def __init__(self, config_file_path):
        """
        Get all required info for AWS S3, Redshift Cluster, and Database in config file
        :param config_file_path: Path lead to AWS S3, Redshift Cluster, and Database config file
        """
        self.config = configparser.ConfigParser()
        self.config.read_file(open(config_file_path))
        self.cluster_info = dict()

        self.__get_cluster_info()

    def __get_cluster_info(self):
        # Cluster configuration
        self.cluster_info['dwh_cluster_identifier'] = self.config.get("DWH", "CLUSTER_IDENTIFIER")
        self.cluster_info['dwh_cluster_type'] = self.config.get("DWH", "CLUSTER_TYPE")
        self.cluster_info['dwh_num_nodes'] = self.config.get("DWH", "NUM_NODES")
        self.cluster_info['dwh_node_type'] = self.config.get("DWH", "NODE_TYPE")

        # Database info
        self.cluster_info['dwh_db'] = self.config.get("CLUSTER", "DB_NAME")
        self.cluster_info['dwh_db_user'] = self.config.get("CLUSTER", "DB_USER")
        self.cluster_info['dwh_db_password'] = self.config.get("CLUSTER", "DB_PASSWORD")
        self.cluster_info['dwh_port'] = self.config.get("CLUSTER", "DB_PORT")
        self.cluster_info['dwh_host'] = self.config.get("CLUSTER", "HOST")
        self.cluster_info['dwh_region'] = self.config.get("DWH", "REGION")

        # S3 info
        self.cluster_info['s3_log_data'] = self.config.get('S3', 'log_data')
        self.cluster_info['s3_log_jsonpath'] = self.config.get('S3', 'log_jsonpath')
        self.cluster_info['s3_song_data'] = self.config.get('S3', 'song_data')

        # IAM role
        self.cluster_info['dwh_iam_role_name'] = self.config.get("DWH", "IAM_ROLE_NAME")
        self.cluster_info['dwh_iam_arn'] = self.config.get("IAM_ROLE", "ARN")

