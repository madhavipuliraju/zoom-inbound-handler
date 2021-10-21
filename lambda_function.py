import json
import logging
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client("lambda")

def lambda_handler(event, context):
    # Incomming Payload routing to the Kendra Lamda
    logger.info(f"Incoming Event: {event}")
    body = json.loads(event.get("body"))
    payload = body.get("payload", {})
    # json_string = parse_qs(body)["payload"][0]
    # print(f"Json String\n{json_string}")
    # payload = json.loads(json_string)
    logger.info(f"Incoming Payload: {payload}")
    if payload.get("cmd", ""):
        userJid = payload.get("userJid","")
        message = payload.get("cmd","")
        accountId = payload.get("accountId","")
        robotJid = payload.get("robotJid","")
        kendra_handler(userJid, message, accountId, robotJid)
    else:
        logger.info("Incoming Message not found")
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Payload forwarded to L2!')
    }

def kendra_handler(userJid, message, accountId, robotJid):
    """
    Invoking the Kendra Lambda
    """
    data = {
        "userJid": userJid,
        "message": message,
        "accountId": accountId,
        "robotJid": robotJid
    }
    lambda_client.invoke(FunctionName=os.environ.get("kendra_handler_arn"),
                         InvocationType="Event",
                         Payload=json.dumps(data))
