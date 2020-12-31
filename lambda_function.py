import json
import logging
import urllib3
import boto3
from seronetdBUtilities import *

# boto3 S3 initialization
s3_client = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ssm = boto3.client("ssm")



def lambda_handler(event, context):
    host=ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user=ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    password=ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    job_table_name='table_file_remover'
    
    http=urllib3.PoolManager()
    #print(event)
    
    
    failure=ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
    success=ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
    
    message = event['Records'][0]['Sns']['Message']
    messageJson=json.loads(message)
    
    #connect to database
    mydb=connectToDB(user, password, host, dbname)
    exe="SELECT * FROM "+job_table_name+" WHERE file_name="+messageJson['file_name']+" AND "+"file_added_on="+messageJson['file_added_on']
    mydbCursor=mydb.cursor()
    mydbCursor.execute(exe)
    sqlresult = mydbCursor.fetchone()
    if(sqlresult[5]=="COPY_SUCCESSFUL"):
        file_status="copy successfully"
    elif(sqlresult[5]=="COPY_UNSUCCESSFUL"):
        file_status="copy unsuccessfully"
        
    
    file_name=str(sqlresult[0])
    file_submitted_by=str(sqlresult[9])
    file_location=str(sqlresult[2])
    file_added_on=str(sqlresult[3])
    
    
    message_slack=str(sqlresult[1])+"(Job ID: "+file_name+", CBC ID: "+file_submitted_by+") "+file_status+" to "+file_location+" at "+file_added_on
    
    
    
    
    data={"text": message_slack}
    if(messageJson['file_status']=="'COPY_SUCCESSFUL'"):
        r=http.request("POST",
                        success, 
                        body=json.dumps(data),
                        headers={"Content-Type":"application/json"})
                        
    elif(messageJson['file_status']=="'COPY_UNSUCCESSFUL'"):
        r=http.request("POST",
                        failure, 
                        body=json.dumps(data),
                        headers={"Content-Type":"application/json"})

    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
