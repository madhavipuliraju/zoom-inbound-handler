import requests
import logging
import boto3
import jwt
import time
import os

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


db_service = boto3.resource('dynamodb')
user_mapping_table = db_service.Table(os.environ.get('ZoomMapping'))
clients_mapping_table = db_service.Table(os.environ.get('ClientsMapping'))


def generate_jwt_token(creds):
    """
    Generate JWT Token
    """
    client_id = creds["client_id"]
    private_key = creds["private_key"]
    api_key = creds["api_key"]
    time_stamp = int(time.time())
    payload = {
       "iss":api_key,
       "exp":time_stamp+3600
    }
    encoded = jwt.encode(payload,private_key,algorithm="HS256",headers={"alg": "HS256","typ": "JWT"})
    if encoded:
        logger.info(f"JWT Token:   {encoded}")
        return encoded
    
def get_user_email(robot_jid, user_id, account_id, creds):
    """
    Gets the users email from Zoom
    """
    auth_token = generate_jwt_token(creds)
    user_details_url = f"https://api.zoom.us/v2/users/{user_id}"
    headers = {
       "Authorization":f"Bearer {auth_token}"
    }
    try:
        response =  requests.request("GET", user_details_url, headers=headers)
        if response.status_code == 200:
            return response.json().get("email")
        logger.error(f"Couldn't retrieve the user email due to status: {response.status_code} and\n\n{response.text}")
    except Exception as ex:
        logger.error(f"Raised an exception while fetching user email: {ex}")