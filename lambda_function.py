import json
import os
import time
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError
import logging
from utils import invoke, parse_event, authorize, AuthorizationError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment Variables
DYNAMODB_TABLE = os.environ.get('RATE_LIMIT_TABLE')
TTL_S = int(os.environ.get('TTL_S'))  # Default 1 hour TTL if not specified
AUTH_BP = os.environ.get('AUTH_BP', '')

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

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for rate limiting based on invocation count
    
    Expected payload (API Gateway or direct Lambda):
    {
        "client_id": "string",  # Required: Client ID to check
        "session": "string"     # Required: Session ID for authorization (can be in body or cookies)
    }
    
    Status Codes:
    200: Success - Rate limit check passed
    400: Bad Request - Missing required fields or invalid input
    401: Unauthorized - Invalid or missing session
    429: Too Many Requests - Rate limit exceeded
    500: Internal Server Error - Unexpected error
    
    Returns:
    {
        "statusCode": int,
        "body": {
            "message": str,
            "rate_limit_info": {
                "invocations": int,
                "reset_time": str,
                "client_id": str
            }
        }
    }
    """
    try:
        # Parse the event (handles both API Gateway and direct Lambda)
        parsed_event = parse_event(event)
        
        # Validate required fields - only client_id and session are needed
        missing_fields = [field for field in ['client_id', 'session'] if field not in parsed_event]
        if missing_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f"Missing required fields: {', '.join(missing_fields)}",
                    'error': 'Bad Request'
                })
            }
        
        client_id = parsed_event['client_id']
        session_id = parsed_event['session']
        
        if session_id == AUTH_BP:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Rate limit check passed',
                })
            }
        
        # Authorize the request
        try:
            authorize(client_id, session_id)
        except AuthorizationError as e:
            return {
                'statusCode': 401,
                'body': json.dumps({
                    'message': str(e),
                    'error': 'Unauthorized'
                })
            }
        
        try:
            # Get current rate limit info
            client_info = get_rate_limit_info(client_id)
            current_invocations = client_info.get('invocations', 0)
            
            # get the client's rate_limit from their user profile
            user_rate_limit = client_info.get("rl_ai", -1)
            if (user_rate_limit == -1):
                logger.error(f"User {client_id} has no rate limit set")
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': 'User has no rate limit set',
                        'error': 'Internal Server Error'
                    })
                }
            
            # Check if rate limit is exceeded
            if current_invocations > user_rate_limit:
                return {
                    'statusCode': 429,
                    'body': json.dumps({
                        'message': 'Rate limit exceeded',
                        'error': 'Too Many Requests',
                    })
                }
            
            # Update invocation count
            update_rate_limit_info(client_id)
            
            # Get updated info for response
            updated_info = get_rate_limit_info(client_id)
            reset_time = datetime.fromtimestamp(
                int(updated_info.get('ttl', time.time() + TTL_S))
            ).isoformat()
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Rate limit check passed',
                })
            }
            
        except ClientError as e:
            logger.error(f"DynamoDB error: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Database error',
                    'error': 'Internal Server Error'
                })
            }
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'An unexpected error occurred',
                'error': 'Internal Server Error'
            })
        } 