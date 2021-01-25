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
    
    
    #set up success and failure slack cahnnel
    failure=ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
    success=ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
    
    #get the message from the event
    message = event['Records'][0]['Sns']['Message']
    #print(event)
    messageJson=eval(message)

    #if the message is passed by the filecopy function
    if(messageJson['previous_function']=='filecopy'):
        
        #connect to database to get information needed for slack
        mydb=connectToDB(user, password, host, dbname)
        exe="SELECT * FROM "+job_table_name+" WHERE file_name="+messageJson['file_name']+" AND "+"file_added_on="+messageJson['file_added_on']
        mydbCursor=mydb.cursor()
        mydbCursor.execute(exe)
        sqlresult = mydbCursor.fetchone()
        #determine which slack channel to send the message to
        if(sqlresult[5]=="COPY_SUCCESSFUL"):
            file_status="copy successfully"
        elif(sqlresult[5]=="COPY_UNSUCCESSFUL"):
            file_status="copy unsuccessfully"
        
        #construct the slack message 
        file_name=str(sqlresult[1])
        file_id=str(sqlresult[0])
        file_submitted_by=str(sqlresult[9])
        file_location=str(sqlresult[2])
        file_added_on=str(sqlresult[3])
        message_slack=file_name+"(Job ID: "+file_id+", CBC ID: "+file_submitted_by+") "+file_status+" to "+file_location+" at "+file_added_on
    
    
    
        #send the message to slack channel
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

    #if the message is passed by the prevalidator function
    elif(messageJson['previous_function']=='prevalidator'):
        #print(messageJson)
        #connect to database to get the file name
        #mydb=connectToDB(user, password, host, dbname)
        #exe="SELECT * FROM "+job_table_name+" WHERE file_id="+messageJson['org_file_id']
        #mydbCursor=mydb.cursor()
        #mydbCursor.execute(exe)
        #sqlresult = mydbCursor.fetchone()
        
        #file_name=sqlresult[1]
        file_name=messageJson['org_file_name']
        validation_date=messageJson['validation_date']
        file_submitted_by=messageJson['file_submitted_by'][1:len(messageJson['file_submitted_by'])-1]
        file_status="processed"
        org_file_id=messageJson['org_file_id']
        error_Message=messageJson['Error_Message']
        
        #collect pass and fail files from the message
        content_pass=[]
        content_fail=[]
        length=len(messageJson['validation_status_list'])
        for i in range(0,length):
            if messageJson['validation_status_list'][i]=="FILE_VALIDATION_SUCCESS":
                content_pass.append(messageJson['full_name_list'][i])
            elif messageJson['validation_status_list'][i]=="FILE_VALIDATION_Failure":
                content_fail.append(messageJson['full_name_list'][i])
        
        #get the files that pass the validation
        passString='NA'
        #get the files that do not pass the validation
        failString='NA'
        if(len(content_pass)>0):
            passString = ', '.join(content_pass)
        if(len(content_fail)>0):
            failString = ', '.join(content_fail)
        
        #construct the slack message 
        message_slack=file_name+"(Job ID: "+org_file_id+" CBC ID: "+file_submitted_by+" Validation pass: "+"_" + passString+"_"+" Validation fail: "+"*"+failString+"*"+") " + "is "+file_status+" at "+validation_date+". "+error_Message
        data={"type": "mrkdwn","text": message_slack}
        if(len(content_fail)>0):
            r=http.request("POST",
                            failure, 
                            body=json.dumps(data),
                            headers={"Content-Type":"application/json"})
        else:                
            r=http.request("POST",
                            success, 
                            body=json.dumps(data),
                            headers={"Content-Type":"application/json"})
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
