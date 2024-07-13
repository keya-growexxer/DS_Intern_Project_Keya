import base64
import boto3
import gzip
import json
import logging
import os

from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def logpayload(event):
    logger.setLevel(logging.DEBUG)
    logger.debug(event['awslogs']['data'])
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_payload = json.loads(uncompressed_payload)
    return log_payload

def error_details(payload):
    error_msg = ""
    log_events = payload['logEvents']
    logger.debug(payload)
    loggroup = payload['logGroup']
    logstream = payload['logStream']
    lambda_func_name = loggroup.split('/')[-1]  # Get last part of loggroup for function name
    logger.debug(f'LogGroup: {loggroup}')
    logger.debug(f'Logstream: {logstream}')
    logger.debug(f'Function name: {lambda_func_name}')
    logger.debug(log_events)
    for log_event in log_events:
        error_msg += log_event['message'] + '\n'  # Add each log message with new line
    logger.debug('Message:\n%s' % error_msg)
    return loggroup, logstream, error_msg, lambda_func_name

def publish_message(loggroup, logstream, error_msg, lambda_func_name):
    sns_arn = os.environ['snsARN']  # Getting the SNS Topic ARN passed in by the environment variables.
    snsclient = boto3.client('sns')
    try:
        message = "\nLambda error summary\n\n"
        message += "##########################################################\n"
        message += f"# LogGroup Name: {loggroup}\n"
        message += f"# LogStream: {logstream}\n"
        message += "# Log Message:\n"
        message += f"{error_msg}\n"
        message += "##########################################################\n"

        # Sending the notification...
        snsclient.publish(
            TargetArn=sns_arn,
            Subject=f'Execution error for Lambda - {lambda_func_name}',
            Message=message
        )
    except ClientError as e:
        logger.error("Failed to publish SNS message: %s" % e)

def lambda_handler(event, context):
    pload = logpayload(event)
    lgroup, lstream, errmessage, lambdaname = error_details(pload)
    publish_message(lgroup, lstream, errmessage, lambdaname)

