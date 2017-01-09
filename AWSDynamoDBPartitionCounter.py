import botocore


class AWSDynamoDBPartitionCounter(object):
    """Counter class to count the number of partitions for a dynamodb table in a specific region"""
    def __init__(self, dbClient, dbStreamsClient, TableName):
        """Returns a counter object that creates dynamodb and dynamodbstreams client object"""
        self.dbClient = dbClient
        self.dbStreamsClient = dbStreamsClient
        self.tableName = TableName

    def TableStreamsValidator(self):
        """Checks whether the table exists and validates if the streams has been enabled on the
        table. If so, returns the stream ARN"""
        try:
            if self.dbClient is not None:
                response = self.dbClient.describe_table(TableName=self.tableName)
                streamArn = response.get("Table").get("LatestStreamArn")
                if streamArn is None:
                    print ("Please enable streams on the table.")
                    return None
                return streamArn
        except botocore.exceptions.ClientError as clientError:
            if clientError.response['Error']['Code'] == "ResourceNotFoundException":
                print ("Table with name " + self.tableName + " does not exist.")
                print ("Request ID: " + clientError.response['ResponseMetadata']['RequestId'])
                return None
            elif clientError.response['Error']['Code'] == "AccessDeniedException":
                print ("Access denied. You do not have access.")
                print ("Request ID: " + clientError.response['ResponseMetadata']['RequestId'])
                return None
            elif clientError.response['Error']['Code'] == "LimitExceededExceptin":
                print ("The number of times you can describe table has been exceeded.")
                print ("Request ID: " + clientError.response['ResponseMetadata']['RequestId'])
                return None
        except Exception as e:
            print (e.message)
            return None

    def GetStreamDescription(self):
        """Gets the information regarding the DynamoDB stream"""
        try:
            streamArn = self.TableStreamsValidator()
            if streamArn is not None:
                response = self.dbStreamsClient.describe_stream(StreamArn=streamArn)
                if response.get("StreamDescription").get("StreamStatus") == "ENABLED":
                    return response.get("StreamDescription").get("Shards")
                else:
                    print ("Stream is not enabled.")
                    return None
            else:
                return None
        except Exception as e:
            print (e.message)
            return None

    def GetPartitionCount(self):
        """Gets the number of partitions by counting the number of shards for dynamodb table"""
        try:
            partitionCount = 0
            Shards = self.GetStreamDescription()
            if Shards is not None:
                for shard in Shards:
                    endingShard = shard.get("SequenceNumberRange").get("EndingSequenceNumber")
                    if endingShard is None:
                        partitionCount += 1
                print ("Total partitions are: " + str(partitionCount))
        except Exception as e:
            print (e.message)
