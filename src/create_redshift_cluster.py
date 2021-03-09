import configparser
import psycopg2
import sys
import boto3
import pandas as pd


def read_config_file():
    """Reads the credentials file and returns a list with it's content
       
       Input Arguments: no args
    """
    config = configparser.ConfigParser()
    config.read_file(open('./aws/credentials.cfg'))

    HOST = config.get("CLUSTER","HOST")
    CLUST_NAME = config.get("OTHER","CLUST_NAME")
    DB_NAME = config.get("CLUSTER","DB_NAME")
    DB_USER = config.get("CLUSTER","DB_USER")
    DB_PASS = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT = config.get("CLUSTER","DB_PORT")
    ARN = config.get("IAM_ROLE","ARN")
    KEY = config.get("AWS","AWS_ACCESS_KEY_ID")
    SECRET = config.get("AWS","AWS_SECRET_ACCESS_KEY")
    
    return [HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT, CLUST_NAME, ARN, KEY, SECRET]


def redshift_client(key, secret):
    redshift = boto3.client('redshift',
                            region_name = "us-west-2",
                            aws_access_key_id = key,
                            aws_secret_access_key = secret)
    return redshift


def ec2_resource(key, secret):
    ec2 = boto3.resource('ec2',
                         region_name = "us-west-2",
                         aws_access_key_id = key,
                         aws_secret_access_key = secret)
    return ec2


def prettyRedshiftProps(props):
    """Shows the cluster properties in a fancy way.
       
       Input Arguments: props - cluster properties
    """
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", 
                  "Endpoint", "NumberOfNodes", 'VpcId']
    
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def create_cluster():
    """Creates a redshift cluster according to the credentials file info.
       
       Input Arguments: no args
    """
    cfg_lst = read_config_file() #return a list with credentials
    ec2 = ec2_resource(cfg_lst[7], cfg_lst[8]) #create a ec2 resource
    redshift = redshift_client(cfg_lst[7], cfg_lst[8]) #create a redshift client
    
    # creating the cluster
    try:
        response = redshift.create_cluster(
            ClusterIdentifier = cfg_lst[5],
            ClusterType = "multi-node",
            NodeType = "dc2.large",
            NumberOfNodes = 3,
            #Identifiers & Credentials
            DBName = cfg_lst[1],        
            MasterUsername = cfg_lst[2],
            MasterUserPassword = cfg_lst[3],
            #Roles (for s3 access)
            IamRoles = [cfg_lst[6]]  
        )
        print("#### Cluster Created!!! ####")
    except Exception as e:
        print(e)
        
    # Opening an incoming TCP port to access the cluster endpoint  
    myClusterProps = redshift.describe_clusters(ClusterIdentifier = cfg_lst[5])['Clusters'][0]
    
    try:
        vpc = ec2.Vpc(id = myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName = defaultSg.group_name,
            CidrIp = '0.0.0.0/0',
            IpProtocol = 'TCP',
            FromPort = int(cfg_lst[4]),
            ToPort = int(cfg_lst[4])
        )
    except Exception as e:
        print(e)
    
    # showing cluster properties
    print(prettyRedshiftProps(myClusterProps))
    

def delete_cluster():
    """Deletes a redshift cluster according to the credentials file info.
       
       Input Arguments: no args
    """
    cfg_lst = read_config_file() #return a list with credentials
    redshift = redshift_client(cfg_lst[7], cfg_lst[8]) #create a redshift client
    
    try:
        redshift.delete_cluster(ClusterIdentifier=cfg_lst[5], SkipFinalClusterSnapshot=True)
        print("#### Cluster Deleted!!! ####")
    except Exception as e:
        print(e)
        

def main():
    args = sys.argv[1:]
    
    if(len(args)== 1 and args[0]=='create'):
        create_cluster()
    elif(len(args)==1 and args[0]=='delete'):
        delete_cluster()
    else:
        print(f"### ERROR: Invalid option '{args[0]}' ####")
    

if __name__ == "__main__":
    main()