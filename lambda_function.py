import json
import os
import time
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError
import logging
from utils import invoke_lambda, parse_event, authorize, AuthorizationError, create_response, LambdaError
from config import logger, AUTH_BP
from rate_limit_logic import process_rate_limit_request

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment Variables
DYNAMODB_TABLE = os.environ.get('RATE_LIMIT_TABLE')
TTL_S = int(os.environ.get('TTL_S', '60'))  # Default 1 minute TTL if not specified

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("RL_AI")
user_table = dynamodb.Table("Users")

class RateLimitExceeded(Exception):
    """Custom exception for rate limit exceeded"""
    pass

def get_rate_limit_info(client_id: str) -> Dict[str, Any]:
    """
    Retrieve rate limit information for a client from DynamoDB
    
    Args:
        client_id (str): Unique identifier for the client
        
    Returns:
        Dict containing rate limit information
    """
    try:
        response = user_table.get_item(Key={'id': client_id})
        return response.get('Item', {})
    except ClientError as e:
        logger.error(f"Error retrieving rate limit info: {str(e)}")
        raise

def update_rate_limit_info(client_id: str) -> None:
    """
    Update rate limit information in DynamoDB by incrementing invocation count
    or creating new record with TTL
    
    Args:
        client_id (str): Unique identifier for the client
    """
    try:
        # Calculate TTL timestamp (current time + TTL_S)
        ttl_timestamp = int(time.time()) + TTL_S
        
        # Try to update existing record
        try:
            table.update_item(
                Key={'associated_account': client_id},
                UpdateExpression='SET invocations = if_not_exists(invocations, :zero) + :inc',
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':zero': 1
                }
            )
        except Exception as e:
            # If record doesn't exist, create new one with TTL
            logger.info(f"Creating new rate limit record for {client_id}")
            try:
                table.put_item(
                    Item={
                        'associated_account': client_id,
                        'invocations': 1,
                        'ttl': ttl_timestamp
                    }
                )
            except Exception as e:
                logger.error(f"Error creating rate limit record: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Error updating rate limit record: {str(e)}")
            raise
            
    except Exception as e:
        logger.error(f"Error updating rate limit info: {str(e)}")
        raise

def lambda_handler(event, context):
    try:
        parsed_event = parse_event(event)
        
        client_id = parsed_event.get('client_id')
        session_id = parsed_event.get('session')
        
        if not client_id or not session_id:
            raise LambdaError(400, "Missing required fields: client_id and session are required.")
            
        result = process_rate_limit_request(client_id, session_id, AUTH_BP)
        
        return create_response(200, result)

    except LambdaError as e:
        return create_response(e.status_code, {"message": e.message, "error": type(e).__name__})
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        return create_response(500, {"message": "An internal server error occurred."}) 