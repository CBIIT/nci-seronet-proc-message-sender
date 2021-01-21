import boto3
import json
import logging
from seronetCopyFiles import *
from seronetdBUtilities import *
from seronetSnsMessagePublisher import *

print('Loading function')

# boto3 S3 initialization
s3_client = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ssm = boto3.client("ssm")
sns = boto3.client('sns')





def lambda_handler(event, context):

 try:
  host=ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
  user=ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
  dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
  password=ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
  destination_bucket_name = ssm.get_parameter(Name="file_destination_bucket", WithDecryption=True).get("Parameter").get("Value")
  job_table_name='table_file_remover'
  
  

  
  
  #read the message from the event
  message=event['Records'][0]['Sns']['Message']
  #convert the message to json style
  messageJson=json.loads(message)
   
  source_bucket_name =messageJson['bucketName']
  
  print('Source Bucket: '+source_bucket_name)
  message=event['Records'][0]['Sns']['Message']
  #convert the message to json style
  messageJson=json.loads(message)
  source_bucket_name =messageJson['bucketName']
    
      # determining which cbc bucket the file came from
  prefix='' 
    
      # Filename of object (with path) and Etag
  file_key_name = messageJson['key']
  
  print('Source Bucket:'+ source_bucket_name)
  print('Key file: '+ file_key_name)
  source_etag = s3_client.head_object(Bucket=source_bucket_name,Key=file_key_name)['ETag'][1:-1]  
  print('Source Etag:'+ source_etag)
  if "guid" in messageJson:
   if messageJson['scanResult']=="Clean": 
    # defining constants for CBCs
    CBC01='cbc01'
    CBC02='cbc02'
    CBC03='cbc03'
    CBC04='cbc04'
   
  
    
   
   
    # determining which cbc bucket the file came from
    prefix='' 
    #result=[]
   
   
       
    if CBC01 in source_bucket_name:
        prefix=CBC01
    elif  CBC02 in source_bucket_name:
        prefix=CBC02
    elif  CBC03 in source_bucket_name:
        prefix=CBC03
    elif  CBC04 in source_bucket_name:
        prefix=CBC04        
    else:
        prefix='UNMATCHED'
   
    print('Prefix is: '+prefix)
        # Copy Source Object
    if(prefix != 'UNMATCHED'):
        #connect to RDS
        mydb=connectToDB(user, password, host, dbname)
        #call the function to copy file
        result=fileCopy(s3_client, event, destination_bucket_name)
        resultTuple=(result['file_name'], result['file_location'], result['file_added_on'], result['file_last_processed_on'], result['file_status'], result['file_origin'], result['file_type'], result['file_action'], result['file_submitted_by'], result['updated_by'])
        
        #record the copy file result 
        excution2="INSERT INTO "+ job_table_name+" VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" %resultTuple
        executeDB(mydb,excution2)
        
        #publish message to sns topic
        result['previous_function']="filecopy"
        TopicArn_Success=ssm.get_parameter(Name="TopicArn_Success", WithDecryption=True).get("Parameter").get("Value")
        TopicArn_Failure = ssm.get_parameter(Name="TopicArn_Failure", WithDecryption=True).get("Parameter").get("Value")
        response=sns_publisher(result,TopicArn_Success,TopicArn_Failure)
        print(response)
        
        
        statusCode=200
        message='File Processed'
       
    else:
        statusCode=400
        message='Desired CBC prefix not found'
        

  
 except Exception as err:
  raise err
  
    #print(message)      

 return {
       'statusCode': statusCode,
       'body': json.dumps(message)
 }  
  
  
  
  
  
  
  
  
  
  
  
  

   
 
