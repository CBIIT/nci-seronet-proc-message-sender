import smtplib  
import email.utils
import boto3
import urllib3
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from jinja2 import Environment, BaseLoader
from seronetdBUtilities import *
import datetime
import dateutil.tz





def lambda_handler(event, context):
    # boto3 S3 initialization
    s3 = boto3.client("s3")
    ssm = boto3.client("ssm")
    
    # Replace smtp_username with your Amazon SES SMTP user name.
    USERNAME_SMTP = ssm.get_parameter(Name="USERNAME_SMTP", WithDecryption=True).get("Parameter").get("Value")
    
    # Replace smtp_password with your Amazon SES SMTP password.
    PASSWORD_SMTP = ssm.get_parameter(Name="PASSWORD_SMTP", WithDecryption=True).get("Parameter").get("Value")
    
    # If you're using Amazon SES in an AWS Region other than US West (Oregon), 
    # replace email-smtp.us-west-2.amazonaws.com with the Amazon SES SMTP  
    # endpoint in the appropriate region.
    HOST = "email-smtp.us-east-1.amazonaws.com"
    PORT = 587
  
    host = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    JOB_TABLE_NAME = 'table_file_remover'
    
    
    http = urllib3.PoolManager()
    
    
    #set up success and failure slack cahnnel
    failure = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
    success = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
    
    #get the message from the event
    message = event['Records'][0]['Sns']['Message']
    #print(event)
    messageJson = json.loads(message)

    #if the message is passed by the filecopy function
    if(messageJson['previous_function']=='filecopy'):
        
        try:
                #remove the Apostrophe('') in the input
                file_name = messageJson['file_name']
                file_added_on = messageJson['file_added_on']
                #connect to database to get information needed for slack
                mydb = connectToDB(user, password, host, dbname)
                exe = f"SELECT * FROM {JOB_TABLE_NAME} WHERE file_name = %s AND file_added_on = %s"
                mydbCursor=mydb.cursor(buffered=True)
                mydbCursor.execute(exe, (file_name, file_added_on))
                sqlresult = mydbCursor.fetchone()
                #determine which slack channel to send the message to
                file_status_sql = sqlresult[5]
                
                if(file_status_sql == "COPY_SUCCESSFUL" or file_status_sql == "COPY_SUCCESSFUL_DUPLICATE"):
                    file_status="copy successfully"
                elif(file_status_sql == "COPY_UNSUCCESSFUL" or file_status_sql == "COPY_UNSUCCESSFUL_DUPLICATE"):
                    file_status="copy unsuccessfully"
                else:
                    print("Error: file has been processed")
                    return #comment this line for testing
                
                
                #file_status="copy successfully"#comment this line in non-prod
                #construct the slack message
                file_id = str(sqlresult[0])
                file_submitted_by = str(sqlresult[9])
                file_location = str(sqlresult[2])
                file_added_on = str(sqlresult[3])
                file_added_on_list = file_added_on.split(" ")
                file_added_on_date = file_added_on_list[0]
                file_added_on_time = file_added_on_list[1]
                file_name = str(sqlresult[1])
                file_md5=str(sqlresult[11])
                
                if(file_status_sql == "COPY_SUCCESSFUL_DUPLICATE" or file_status_sql == "COPY_UNSUCCESSFUL_DUPLICATE"):
                    exe2 = f"SELECT * FROM {JOB_TABLE_NAME} WHERE file_md5 = %s AND (file_status='COPY_SUCCESSFUL' OR file_status='FILE_Processed') "
                    mydbCursor.execute(exe2, (file_md5,))
                    sqlresult2 = mydbCursor.fetchall()
                    length = len(sqlresult2)-1
                    duplicate_file_added_on = str(sqlresult2[length][3])
                    duplicate_file_added_on_list = duplicate_file_added_on.split(" ")
                    duplicate_file_added_on_date = duplicate_file_added_on_list[0]
                    duplicate_file_added_on_time = duplicate_file_added_on_list[1]
                    
                #send the message to slack channel
                message_slack=f"{file_name}(Job ID: {file_id}, CBC ID: {file_submitted_by}) {file_status} to {file_location} at {file_added_on}."
                #send the message to slack channel
                data={"text": message_slack}
                if(file_status_sql == "COPY_SUCCESSFUL" and messageJson['send_slack'] == "yes"):
                    r=http.request("POST",
                                success, 
                                body=json.dumps(data),
                                headers={"Content-Type":"application/json"})
                elif(file_status_sql == "COPY_SUCCESSFUL_DUPLICATE" and messageJson['send_slack'] == "yes"):
                    message_slack=message_slack+f" The file has been found to be a duplicate of a previous submission made at {duplicate_file_added_on}."
                    data={"text": message_slack}
                    r=http.request("POST",
                                failure, 
                                body=json.dumps(data),
                                headers={"Content-Type":"application/json"})
                elif(file_status_sql == "COPY_UNSUCCESSFUL_DUPLICATE" and messageJson['send_slack'] == "yes"):
                    message_slack=message_slack+f" The file has been found to be a duplicate of a previous submission made at {duplicate_file_added_on}."
                    data={"text": message_slack}
                    r=http.request("POST",
                                failure, 
                                body=json.dumps(data),
                                headers={"Content-Type":"application/json"})
                elif(file_status_sql == "COPY_UNSUCCESSFUL" and messageJson['send_slack'] == "yes"):
                    r=http.request("POST",
                                failure, 
                                body=json.dumps(data),
                                headers={"Content-Type":"application/json"})
                else:
                    print("previous function does not allow to send the slack notification")

                
                
                
                
                
                
                # get the HTML template of the email from s3 bucket.
                bucket_email = ssm.get_parameter(Name="bucket_email", WithDecryption=True).get("Parameter").get("Value")
                key_email = ssm.get_parameter(Name="key_email", WithDecryption=True).get("Parameter").get("Value")
                key_email_duplicate = ssm.get_parameter(Name="key_email_duplicate", WithDecryption=True).get("Parameter").get("Value")
                SUBJECT = 'File Received'
                
                if(file_status_sql == "COPY_SUCCESSFUL"):
                    s3_response_object = s3.get_object(Bucket=bucket_email, Key=key_email)
                    body = s3_response_object['Body'].read()
                    body = body.decode('utf-8')
                    template = Environment(loader=BaseLoader).from_string(body)
                    BODY_HTML = template.render(file_added_on_date=file_added_on_date, file_added_on_time=file_added_on_time)
                elif(file_status_sql == "COPY_SUCCESSFUL_DUPLICATE"):
                    s3_response_object = s3.get_object(Bucket=bucket_email, Key=key_email_duplicate)
                    body = s3_response_object['Body'].read()
                    body = body.decode('utf-8')
                    template = Environment(loader=BaseLoader).from_string(body)
                    BODY_HTML = template.render(file_added_on_date=file_added_on_date, file_added_on_time=file_added_on_time, duplicate_file_added_on_date=duplicate_file_added_on_date, duplicate_file_added_on_time=duplicate_file_added_on_time)
                    
                #sending the email
                # Replace sender@example.com with your "From" address.
                # This address must be verified with Amazon SES.
                SENDERNAME = 'SeroNet Data Team'
                SENDER = ssm.get_parameter(Name="sender-email", WithDecryption=True).get("Parameter").get("Value")
                # Create message container - the correct MIME type is multipart/alternative.
                msg = MIMEMultipart('alternative')
                msg['Subject'] = SUBJECT
                msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
                
                # Record the MIME types of both parts - text/plain and text/html.
                part1 = MIMEText(BODY_HTML, 'html')
                # Attach parts into message container.
                # According to RFC 2046, the last part of a multipart message, in this case
                # the HTML message, is best and preferred.
                msg.attach(part1)
                
               
                 
                if(file_submitted_by in ("cbc01","cbc02","cbc03","cbc04")):
                    # Replace recipient@example.com with a "To" address. If your account 
                    # is still in the sandbox, this address must be verified.
                    recipient_email_list=file_submitted_by+"-recipient-email"
                    RECIPIENT = ssm.get_parameter(Name = recipient_email_list, WithDecryption=True).get("Parameter").get("Value")  
                    RECIPIENT_LIST = RECIPIENT.split(",")
                   
                    for recipient in RECIPIENT_LIST:
                        msg['To'] = recipient
                        
                        #record the email that sent
                        message_sender_orig_file_id = file_id
                        message_sender_cbc_id = file_submitted_by
                        message_sender_recepient = recipient
                        eastern = dateutil.tz.gettz('US/Eastern')
                        timestampDB = datetime.datetime.now(tz=eastern).strftime('%Y-%m-%d %H:%M:%S')
                        message_sender_sentdate = timestampDB
                        message_sender_sender_email = SENDER
                        MESSAGE_SENDER_TABLE_NAME = "table_message_sender"
                        
                        #if copy successfully
                        if((file_status_sql == "COPY_SUCCESSFUL" or file_status_sql == "COPY_SUCCESSFUL_DUPLICATE") and messageJson['send_email'] == "yes"):
                            try:  
                                # Try to send the message.
                                server = smtplib.SMTP(HOST, PORT)
                                server.ehlo()
                                server.starttls()
                                #stmplib docs recommend calling ehlo() before & after starttls()
                                server.ehlo()
                                server.login(USERNAME_SMTP, PASSWORD_SMTP)
                                server.sendmail(SENDER, recipient, msg.as_string())
                                server.close()
                            # Display an error message if something goes wrong.
                            except Exception as e:
                                message_sender_sent_status = "Email_Sent_Failure"
                                message_sender_tuple = (message_sender_orig_file_id, message_sender_cbc_id, message_sender_recepient, message_sender_sentdate, message_sender_sender_email, message_sender_sent_status)
                                message_sender_mysql = f"INSERT INTO {MESSAGE_SENDER_TABLE_NAME} VALUES (NULL,%s,%s,%s,%s,%s,%s)"
                                mydbCursor.execute(message_sender_mysql, message_sender_tuple)
                                raise e
                            else:
                                message_sender_sent_status = "Email_Sent_Success"
                                message_sender_tuple = (message_sender_orig_file_id, message_sender_cbc_id, message_sender_recepient, message_sender_sentdate, message_sender_sender_email, message_sender_sent_status)
                                message_sender_mysql = f"INSERT INTO {MESSAGE_SENDER_TABLE_NAME} VALUES (NULL,%s,%s,%s,%s,%s,%s)"
                                mydbCursor.execute(message_sender_mysql, message_sender_tuple)
                                print("Email sent!")
        
                                
                        else:
                            print("Copy is unsuccessful or the previous function does not allow to send email")
                else:
                    print("Can not locate the recipient email list")
        except Exception as e:
                raise e
        finally: 
                #close the connection
                mydb.commit()
                mydb.close()
        
        
    #if the message is passed by the prevalidator function
    elif(messageJson['previous_function']=='prevalidator'):
        
        file_name = messageJson['org_file_name']
        validation_date = messageJson['validation_date']
        file_submitted_by = messageJson['file_submitted_by'][1:len(messageJson['file_submitted_by'])-1]
        file_status = "processed"
        org_file_id = messageJson['org_file_id']
        error_Message = messageJson['Error_Message']
        
        #collect pass and fail files from the message
        content_pass = []
        content_fail = []
        length = len(messageJson['validation_status_list'])
        for i in range(0,length):
            if messageJson['validation_status_list'][i] == "FILE_VALIDATION_IN_PROGRESS":
                content_pass.append(messageJson['full_name_list'][i])
            elif messageJson['validation_status_list'][i] == "FILE_VALIDATION_FAILURE":
                content_fail.append(messageJson['full_name_list'][i])
        
        #get the files that pass the validation
        passString = 'NA'
        #get the files that do not pass the validation
        failString = 'NA'
        if(len(content_pass)>0):
            passString = ', '.join(content_pass)
        if(len(content_fail)>0):
            failString = ', '.join(content_fail)
        
        #construct the slack message 
        message_slack=f"{file_name}(Job ID: {org_file_id} CBC ID: {file_submitted_by} Validation pass: _{passString}_ Validation fail: *{failString}*) is {file_status} at {validation_date}. {error_Message}"
        data={"type": "mrkdwn","text": message_slack}
        if(len(content_fail)>0):
            if(messageJson['send_slack'] == "yes"):
                r=http.request("POST",
                            failure, 
                            body=json.dumps(data),
                            headers={"Content-Type":"application/json"})
        else:
            if(messageJson['send_slack'] == "yes"):
                r=http.request("POST",
                            success, 
                            body=json.dumps(data),
                            headers={"Content-Type":"application/json"})
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
