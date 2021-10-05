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

import base64
import gzip
import logging
import os
import sys


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
    if 'Records' in event.keys():
        lower_event = lower_key(event)
        event_trigger = lower_event['records'][0]["eventsource"]
        
        if "sns" in event_trigger:         ## event trigger is Sns
            message = event['Records'][0]['Sns']['Message']
            messageJson = json.loads(message)
            trigger_type = "Sns"
        elif "s3" in event_trigger:        ## event trigger is s3
            bucket_name = event['Records'][0]['s3']['bucket']['name']
            message = event['Records'][0]['s3']['object']['key']
            print(f"## bucket: {bucket_name} \n Key_Name: {message}")
            trigger_type = "S3"
        else:
             sys.exit("Function triggered by an invalid message")
        
    elif 'awslogs' in event.keys():
        trigger_type = "cloudwatch"

    else:
        sys.exit("Function triggered by an invalid message")
    
    if  trigger_type == "S3":
        try:
            RECIPIENT_LIST_RAW = ssm.get_parameter(Name="Shipping_Manifest_Recipents", WithDecryption=True).get("Parameter").get("Value")
            RECIPIENT_LIST = RECIPIENT_LIST_RAW.replace(" ", "")
            SUBJECT = 'Shipping Manifest Has Been Uploaded'
            SENDERNAME = 'SeroNet Data Team (Data Curation)'
            SENDER = ssm.get_parameter(Name="sender-email", WithDecryption=True).get("Parameter").get("Value")
            msg_text = (f"A Shipping_Manifest was uploaded to {bucket_name}\n\r " + 
                        f"The Path to the file is: {message}")
            msg_text = msg_text.replace("+", " ")
            msg = MIMEMultipart('alternative')
            msg['Subject'] = SUBJECT
            msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
            part1 = MIMEText(msg_text, "plain")
            msg.attach(part1)
            msg['To'] = RECIPIENT_LIST
            
            for recipient in RECIPIENT_LIST:
                send_email_func(HOST, PORT, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg)
        except Exception as ex:
            display_error_line(ex)
        finally:
            if 'server' in locals():
                server.close()  # server was connected but failed, close the connection


    elif trigger_type == "cloudwatch":

        error = ssm.get_parameter(Name="error_hook_url", WithDecryption=True).get("Parameter").get("Value")
        
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        def get_log_payload(event):
            logger.setLevel(logging.DEBUG)
            logger.debug(event['awslogs']['data'])
            compressed_payload = base64.b64decode(event['awslogs']['data'])
            uncompressed_payload = gzip.decompress(compressed_payload)
            log_payload = json.loads(uncompressed_payload)
            return log_payload

        def get_error_details(payload):
            error_msg = ""
            log_events = payload['logEvents']
            logger.debug(payload)
            loggroup = payload['logGroup']
            logstream = payload['logStream']
            lambda_func_name = loggroup.split('/')
            logger.debug(f'LogGroup: {loggroup}')
            logger.debug(f'Logstream: {logstream}')
            logger.debug(f'Function name: {lambda_func_name[3]}')
            logger.debug(log_events)
            for log_event in log_events:
                error_msg += log_event['message']
            logger.debug('Message: %s' % error_msg.split("\n"))
            return loggroup, logstream, error_msg, lambda_func_name

        def send_message_email(loggroup, logstream, error_msg, lambda_func_name):
            RECIPIENT_LIST_RAW = ssm.get_parameter(Name="Seronet_Error_Recipients", WithDecryption=True).get("Parameter").get("Value")
            RECIPIENT_LIST = RECIPIENT_LIST_RAW.replace(" ", "")
            SUBJECT = 'Seronet Lambda Errors Found: ' + str(lambda_func_name)
            SENDERNAME = 'SeroNet Data Team (Operations)'
            SENDER = ssm.get_parameter(Name="sender-email", WithDecryption=True).get("Parameter").get("Value")
            
            for recipient in RECIPIENT_LIST:
              try:
                msg_text = ""
                msg_text += "Lambda error  summary:" + "\n\n"
                msg_text += "LogGroup Name: " + str(loggroup) + "\n"
                msg_text += "LogStream: " + str(logstream) + "\n"
                msg_text += "\nLog Message:" + "\n"
                msg_text += "\t\t" + str(error_msg.split("\n")) + "\n"
                
                msg = MIMEMultipart('alternative')
                msg['Subject'] = SUBJECT
                msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
                part1 = MIMEText(msg_text, "plain")
                msg.attach(part1)
                msg['To'] = recipient
                send_email_func(HOST, PORT, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg)

              except Exception as e:
                logger.error("An error occured: %s" % e)
                raise e

        def send_message_slack(loggroup, logstream, error_msg, lambda_func_name):
            try:
                msg_text = ""
                msg_text += "LogGroup Name: " + str(loggroup) + "\n"
                msg_text += "LogStream: " + str(logstream) + "\n"
                msg_text += "\nLog Message:" + "\n"
                msg_text += "\t\t" + str(error_msg.split("\n")) + "\n"
                
                #send the message to the error slack channel
                message_slack=f"Seronet Lambda Errors Found: \n\n {msg_text}"
                data={"text": message_slack}
                r=http.request("POST",
                    error, 
                    body=json.dumps(data),
                    headers={"Content-Type":"application/json"})

            except Exception as e:
                logger.error("An error occured: %s" % e)
                raise e

        try:
            payload = get_log_payload(event)
            lgroup, lstream, errmessage, lambdaname = get_error_details(payload)
            send_message_email(lgroup, lstream, errmessage, lambdaname)
            send_message_slack(lgroup, lstream, errmessage, lambdaname)
         
        except Exception as e:
            logger.error("An error occured: %s" % e)
            raise e


    #if the message is passed by the filecopy function
    elif(messageJson['previous_function']=='filecopy'):
        
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
                                send_email_func(HOST, PORT, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg)
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


def send_email_func(HOST, PORT, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg):
    print("Sender: {}   Recipient: {}".format(SENDER, recipient))
    
    server = smtplib.SMTP(HOST, PORT)
    server.ehlo()
    server.starttls()
    #stmplib docs recommend calling ehlo() before & after starttls()
    server.ehlo()
    server.login(USERNAME_SMTP, PASSWORD_SMTP)

    server.sendmail(SENDER, recipient, msg.as_string())
    server.close()


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))
    

def lower_key(in_dict):
    if type(in_dict) is dict:
        out_dict = {}
        for key, item in in_dict.items():
            out_dict[key.lower()] = lower_key(item)
        return out_dict
    elif type(in_dict) is list:
        return [lower_key(obj) for obj in in_dict]
    else:
        return in_dict
