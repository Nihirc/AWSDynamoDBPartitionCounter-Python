import boto3
from AWSDynamoDBPartitionCounter import AWSDynamoDBPartitionCounter


def setRegion():
    """Sets a region by reading from ~/.aws/config.
    If not available, prompts the user to enter a region"""
    try:
        botoSession = boto3.session.Session()
        dynamodb_regions = botoSession.get_available_regions("dynamodb")
        AWSRegion = botoSession.region_name
        if AWSRegion is None:
            print ("Could not retrieve region from ~/.aws/config file. Please ensure that ~/.aws/config and ~/.aws/credentials files exists.")
            region = raw_input("Please provide a valid AWS region: ")
            if region not in dynamodb_regions:
                print ("Invalid region provided.")
                print ("Available regions for dynamodb service are: ")
                for r in dynamodb_regions:
                    print (r)
                return None
            else:
                return region
        else:
            return AWSRegion
    except Exception as e:
        print e.message
        return None

if __name__ == '__main__':
    region = setRegion()
    print (region)
    if region is not None:
        dbClient = boto3.client("dynamodb", region_name=region)
        dbStreamsClient = boto3.client("dynamodbstreams", region_name=region)
        tableName = raw_input("Please provide a table name: ")
        counter = AWSDynamoDBPartitionCounter(dbClient, dbStreamsClient, tableName)
        counter.GetPartitionCount()
