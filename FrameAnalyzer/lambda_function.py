import os
import json
import boto3
import logging
from urllib.parse import unquote_plus
import requests

#still working on this no yet ready to deploy

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Environment variables
# This assumes you have created and configured a DynamoDB table named 'FrameInfoTable'
# with a Partition Key of 'video_id' and a Sort Key of 'frame_id'.
FRAME_INFO_TABLE_NAME = os.environ['FRAME_INFO_TABLE_NAME']
VISION_LLM_API_KEY = os.environ['VISION_LLM_API_KEY']
# Note: You should replace this with the actual API endpoint for your chosen LLM.
VISION_LLM_API_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent" 

def lambda_handler(event, context):
    logger.info("Received S3 event for FrameAnalyzer: %s", json.dumps(event))

    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = event['Records'][0]['s3']['object']['key']

        # S3 keys can have URL-encoded characters, so decode them
        decoded_source_key = unquote_plus(source_key)

        # Assuming S3 key format: {video_id}/frames/{type}/{filename}.jpg
        key_parts = decoded_source_key.split('/')
        video_id = key_parts[0]
        frame_type = key_parts[2] # e.g., 'timed' or 'keyword'
        filename = key_parts[-1]

        # Extract timestamp from filename (e.g., 'frame_001.jpg' -> '001')
        frame_number = filename.split('_')[-1].split('.')[0]
        # A more robust way to get timestamp would be from the file's metadata
        # or from the pre-processor, but this works for a POC
        timestamp = int(frame_number) * 5 # Assuming 5-second intervals from pre-processor

        # Construct the publicly accessible URL for the Vision LLM (if needed)
        # Note: You may need to create a pre-signed URL if your S3 objects are not public
        frame_url = f"https://{source_bucket}.s3.amazonaws.com/{decoded_source_key}"

        logger.info(f"Analyzing frame {filename} for video_id: {video_id} from S3 URL: {frame_url}")

        # 1. Call the Vision LLM API
        llm_description = _analyze_frame_with_llm(frame_url)
        
        # 2. Save the result to DynamoDB
        _save_frame_analysis(video_id, frame_number, timestamp, frame_url, llm_description, frame_type)

        logger.info(f"Successfully analyzed frame {filename} and saved to DynamoDB.")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Frame analysis successful.')
        }

    except Exception as e:
        logger.error(f"Error analyzing frame: {e}")
        raise # Re-raise the exception for Lambda to catch and log

def _analyze_frame_with_llm(image_url):
    """
    Calls an external Vision-enabled LLM to get a description of the image.
    This is a simplified example; a real-world implementation would use a proper SDK.
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {VISION_LLM_API_KEY}" # or different auth method
    }
    
    prompt = "Analyze this image from a product video. Describe the product, its key features, color, and any actions being performed. Be concise and descriptive."

    # Construct the payload for the LLM API call
    payload = {
        "model": "your-model-name",
        "instances": [
            {
                "image": {"url": image_url},
                "text": prompt
            }
        ]
    }
    
    try:
        response = requests.post(VISION_LLM_API_ENDPOINT, headers=headers, data=json.dumps(payload))
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        
        result = response.json()
        # Extract the description from the API response (this structure varies by LLM)
        llm_description = result.get('predictions', [{}])[0].get('content', 'No description generated.')
        
        return llm_description

    except requests.exceptions.RequestException as e:
        logger.error(f"LLM API call failed: {e}")
        return "Failed to get description from LLM."

def _save_frame_analysis(video_id, frame_id, timestamp, s3_url, description, frame_type):
    """
    Saves the frame analysis data to a DynamoDB table.
    """
    table = dynamodb.Table(FRAME_INFO_TABLE_NAME)
    
    table.put_item(
        Item={
            'video_id': video_id,
            'frame_id': frame_id,
            'timestamp': timestamp,
            's3_url': s3_url,
            'description': description,
            'type': frame_type
        }
    )
    logger.info(f"Saved frame analysis to DynamoDB for frame ID: {frame_id}")