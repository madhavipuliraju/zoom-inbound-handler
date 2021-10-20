import json
import os
import logging
import boto3
import hashlib
import traceback
from datetime import datetime
from zoom_helper import get_user_email
from profiler import profile

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')

db_service = boto3.resource('dynamodb')
user_mapping_table = db_service.Table(os.environ.get('ZoomMapping'))
clients_mapping_table = db_service.Table(os.environ.get('ClientsMapping'))
reverse_mapping_table = db_service.Table(os.environ.get('zoom_user_mapping'))

# TODO: line no: 170 -> store latest_message in user_mapping_table : update the users latest message

@profile
def lambda_handler(event, context):
    """
    Handeling incomming event from A-InboundEventHandler for Teams
    """
    logger.info(f"Incoming event: {event}")
    try:
        payload = event.get("payload")
        itsm = event.get("itsm")
        client_id = event.get("client_id")

        logger.debug(f"Incoming payload is:{payload}")
        event_type = event.get("type", "")
        if event_type not in ["message", "invoke", "bot_notification"]:
            logger.info(
                "Received unsupported event, returning success response")
            return {'statusCode': 200, "body": "Unsupported event"}

        user_name = payload.get("userName", "")
        user_id = payload.get("userId", "")
        # user_id = user_id.replace("-", "")

        client_response = clients_mapping_table.get_item(
            Key={"client_id": client_id})
        if "Item" in client_response:
            creds = {
                "client_id": client_response.get("Item", {}).get("zoom_client_id"),
                "client_secret": client_response.get("Item", {}).get("zoom_client_secret"),
                "scope": client_response.get("Item", {}).get("zoom_scope"),
                "user_details_url": client_response.get("Item", {}).get("zoom_base_url"),
                "api_key": client_response.get("Item", {}).get("zoom_api_key"),
                "private_key": client_response.get("Item", {}).get("zoom_private_key")
            }
        else:
            logger.error(f"Couldn't find entry for client id: {client_id}")
            return

        robot_jid = payload.get("robotJid")
        account_id = payload.get("accountId")
        to_jid = payload.get("toJid")
        updated_user_id = user_id.replace("-","")
        response = user_mapping_table.get_item(Key={"user_id": user_id})
        logger.info(f"response of get user from ZoomMapping DB: {response}")
        if "Item" in response:
            logger.info("User found in the DB")
            email = response.get("Item", {}).get("user_email")
        else:
            logger.info("User not found in the DB")
            email = get_user_email(robot_jid, user_id, account_id, creds)
            try:
                logger.info("Putting user in the DB")
                user_mapping_table.put_item(Item={
                    "user_id": user_id,
                    "user_email": email,
                    "user_name": user_name,
                    "robot_jid": robot_jid,
                    "account_id": account_id,
                    "to_jid":to_jid
                })
            except Exception as ex:
                logger.error(
                    f"Putting item to DB failed with exception: {ex}.\n\nTrying for the second time")
                user_mapping_table.put_item(Item={
                    "user_id": user_id,
                    "user_email": email,
                    "user_name": user_name,
                    "robot_jid": robot_jid,
                    "account_id":account_id,
                    "to_jid":to_jid
                })
        reverse_mapping_data = {
            "user_id": updated_user_id,
            "zoom_id": user_id
        }
        reverse_mapping_table.put_item(Item=reverse_mapping_data)
        # if event_type == "message":
        #     if "text" in payload:
        #         message = payload.get("text").strip()
        #         message_id = send_message_to_haptik(conversation_id, message, user_name, email, client_id, itsm, auth_id)
        #         logger.info("Invoked Haptik Oubound handler successfully!")
        #     else:
        #         message = "Received File from the user"
        #     handle_ticket_creation(message, email, itsm,conversation_id, auth_id, client_id)
        #     if "attachments" in payload:
        #         handle_attachments(
        #             payload["attachments"], user_name, email, client_id, itsm, auth_id, conversation_id)
        # elif event_type == "invoke":
        #     handle_attachment_consent()
        if event_type == "bot_notification":
            message = payload.get("cmd").strip()
            message_id = send_message_to_haptik(user_id, message, user_name, email, client_id, itsm)
            logger.info("Invoked Haptik Oubound handler successfully!")
            handle_ticket_creation(message, email, itsm, user_id, client_id)

        return {'statusCode': 200, "body": "Teams event handled successfully"}
    except Exception as ex:
        logger.error(
            f"lambda execution failed with error-{ex},traceback:{traceback.format_exc()}")
        return {'statusCode': 200, 'body': "lambda execution."}


def handle_ticket_creation(message, email, itsm, user_id, client_id):
    """
    # Triggers the Ticketing service
    """
    logger.info(f"Ticket Creation handler for ITSM: {itsm}")
    data = {
        "itsm": itsm,
        "payload": {
            "client_id": client_id,
            "event": "TICKET_CREATION",
            "user": user_id,
            "source": "zoom",
            "message": message,
            "email": email
        }
    }
    lambda_client.invoke(FunctionName=os.environ.get("ticketing_handler_arn"),
                         InvocationType="Event",
                         Payload=json.dumps(data))


def send_message_to_haptik(user_id, message, user_name, email, client_id, itsm):
    """
    Invokes Haptik Handler Lambda
    """
    logger.info("Sending Message to haptik")
    user_response = clients_mapping_table.get_item(Key={"client_id": client_id})
    logger.info(user_response)
    if "Item" in user_response:
        is_translation = user_response.get("Item", {}).get("is_translation", "")
    else:
        logger.info(f"Items not found for the client:   {client_id}")
    if is_translation:
        logger.info("is_translation is True. Translation function is called")
        transaltion_payload = {
            "message": message,
            "user_id": user_id,
            "source": "user"
        }
        response = lambda_client.invoke(FunctionName=os.environ.get("translation_service_arn"),
                                        InvocationType="RequestResponse",
                                        Payload=json.dumps(transaltion_payload))
        response = json.load(response.get("Payload"))
        logger.debug(f"Response of translation service is: {response}")
        message = response.get("translated_message")
        logger.info(f"Translation handled message: {message}")

    send_message_to_haptik = {
        "user": user_id + "_ZOOM_" + itsm + "_" + client_id,
        "message": message,
        "user_name": user_name,
        "email": email,
        "client_id": client_id
    }
    lambda_client.invoke(FunctionName=os.environ.get("haptik_handler_arn"),
                         InvocationType="Event",
                         Payload=json.dumps(send_message_to_haptik))
    store_message_in_DB(message, user_id)
    
    user_mapping_table.update_item(Key={"user_id": user_id},
                                UpdateExpression="set latest_message=:j",
                                ExpressionAttributeValues={
                                    ":j": message
                                })
    user_mapping_table.update_item(Key={"user_id": user_id},
                                UpdateExpression="set latest_message=:j",
                                ExpressionAttributeValues={
                                    ":j": message
                                })
    return


def handle_attachments(attachments, user_name, email, client_id, itsm, user_id):
    """
    Check the attachment type - Inline/download
    Send attachment to haptik
    attach the attchment to ITSM
    """
    for attachment in attachments:
        content_type = attachment["contentType"]
        if content_type == "application/vnd.microsoft.teams.file.download.info":
            file_name = attachment["name"]
            content = attachment["content"]
            file_link = content["downloadUrl"]
            file_type = content["fileType"]
        elif content_type == "image/*":
            file_name = "attachment.png"
            file_link = attachment["contentUrl"]
            file_type = "png"
        else:
            logger.info(f"invalid attachment type {content_type}")
            return
        send_file_to_haptik(auth_id, user_name, email,
                            client_id, itsm, file_type,
                            file_name, file_link)
        send_file_to_ticket(auth_id, conversation_id, email,
                            client_id, itsm, file_type,
                            file_name, file_link)
    return "handled"


def send_file_to_haptik(auth_id, user_name, email, client_id, itsm, file_type, file_name, file_link):
    """
    Sends file to haptik
    """
    logger.info("Invoking the Send File to haptik handler")
    data = {
        "user": auth_id + "_TEAMS_" + itsm + "_" + client_id,
        "is_file": True,
        "file_type": file_type,
        "file_link": file_link,
        "file_name": file_name,
        "user_name": user_name,
        "email": email,
        "source": "teams",
        "client_id": client_id
    }
    lambda_client.invoke(FunctionName=os.environ.get("haptik_handler_arn"),
                         InvocationType="Event",
                         Payload=json.dumps(data))


def send_file_to_ticket(auth_id, conversation_id, email, client_id, itsm, file_type, file_name, file_link):
    """
    Sends file to ticket
    """
    logger.info("Invoking the ticketing helper to add attachment")
    data = {
        "itsm": itsm,
        "payload": {
            "event": "TICKET_ATTACHMENT",
            "source": "teams",
            "auth_id": auth_id,
            "conversation_id": conversation_id,
            "client_id": client_id,
            "email": email,
            "file_type": file_type,
            "file_name": file_name,
            "file_link": file_link
        }
    }
    logger.debug(f"Data being passed to ticketing function is: {data}")
    lambda_client.invoke(FunctionName=os.environ.get("ticketing_handler_arn"),
                         InvocationType="Event",
                         Payload=json.dumps(data))


# def handle_attachment_consent():
#     """
#     Sends attachment if user accepts the consent
#     # Accept
#         file_name, file_type, file_consent should be fetched from DB
#         Get upload url and content url from payload
#         Upload the file
#         Send the confirmation message
#         Delete the consent message
#     # Declines
#         Delete the consent Message
#         Delete the file* entries from DB
#     """
#     # TODO Might not be required
#     logger.info(f"Received Consent URL")

def store_message_in_DB(message, user_id):
    """
    Stores the Chat message in the DB as chat_transcript.
    """
    response = user_mapping_table.get_item(Key={"user_id": user_id})
    if "Item" not in response:
        logger.error(f"User: {user_id} not found in the Table")    
        return
    chat_transcript = response.get("Item", {}).get("chat_transcript")
    formatted_time = datetime.now().strftime("%H:%M:%S %d-%m-%Y")
    message = f"{formatted_time} [User]: {message}"
    if chat_transcript:
        message = f"{chat_transcript}\n{message}"
        
    user_mapping_table.update_item(Key={"user_id": user_id},
                                UpdateExpression="set chat_transcript=:i",
                                ExpressionAttributeValues={
                                    ":i": message
                                })