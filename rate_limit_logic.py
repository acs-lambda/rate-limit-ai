import time
import boto3
from botocore.exceptions import ClientError
from config import logger
from utils import LambdaError, authorize

# Environment Variables
TTL_S = int(os.environ.get('TTL_S', 3600))  # Default 1 hour

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("RL_AI")
user_table = dynamodb.Table("Users")

def get_user_rate_limit(client_id):
    try:
        response = user_table.get_item(Key={'id': client_id})
        item = response.get('Item')
        if not item or 'rl_ai' not in item:
            raise LambdaError(500, f"User {client_id} has no AI rate limit set.")
        return item['rl_ai']
    except ClientError as e:
        logger.error(f"Error retrieving user rate limit for {client_id}: {e}")
        raise LambdaError(500, "Database error while fetching user rate limit.")

def check_and_update_rate_limit(client_id):
    user_rate_limit = get_user_rate_limit(client_id)
    
    try:
        # Get current invocations
        response = table.get_item(Key={'associated_account': client_id})
        item = response.get('Item', {})
        current_invocations = item.get('invocations', 0)

        if current_invocations >= user_rate_limit:
            raise LambdaError(429, "Rate limit exceeded.")

        # Update or create record
        ttl_timestamp = int(time.time()) + TTL_S
        table.update_item(
            Key={'associated_account': client_id},
            UpdateExpression="SET invocations = if_not_exists(invocations, :start) + :inc, ttl = :ttl",
            ExpressionAttributeValues={
                ':inc': 1,
                ':start': 0,
                ':ttl': ttl_timestamp
            }
        )
        return {"message": "Rate limit check passed.", "current": current_invocations + 1, "limit": user_rate_limit}

    except ClientError as e:
        logger.error(f"DynamoDB error during rate limit check for {client_id}: {e}")
        raise LambdaError(500, "Database error during rate limit check.")

def process_rate_limit_request(client_id, session_id, auth_bp):
    if session_id == auth_bp:
        return {"message": "Rate limit check bypassed for admin."}
    
    authorize(client_id, session_id) # authorize is in utils
    return check_and_update_rate_limit(client_id)
