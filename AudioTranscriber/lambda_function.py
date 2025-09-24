import os
import json
import boto3
from urllib.parse import unquote_plus
import logging
import uuid

# Configure logging for better debug output in CloudWatch Logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
transcribe = boto3.client('transcribe')

# Environment variables (configured in the Lambda console)
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = event['Records'][0]['s3']['object']['key']

        # The S3 key can have URL-encoded characters, so we need to decode it
        decoded_source_key = unquote_plus(source_key)

        # Extract the unique video ID from the file path
        # Assuming the path is '{video_id}/audio/{video_id}.mp3'
        video_id = decoded_source_key.split('/')[0]

        # Construct the S3 URI for the audio file
        media_file_uri = f"s3://{source_bucket}/{decoded_source_key}"
        
        # Create a unique name for the transcription job
        transcribe_job_name = f"video-transcription-{video_id}-{uuid.uuid4()}"
        
        # Start the transcription job
        response = transcribe.start_transcription_job(
            TranscriptionJobName=transcribe_job_name,
            LanguageCode='en-US', # You can change this to your desired language
            MediaFormat='mp3',
            Media={
                'MediaFileUri': media_file_uri
            },
            # Tell Transcribe where to save the output JSON file
            OutputBucketName=OUTPUT_BUCKET,
            OutputKey=f"{video_id}/transcripts/{transcribe_job_name}.json"
        )
        
        logger.info(f"Transcription job '{transcribe_job_name}' started successfully.")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Amazon Transcribe job started',
                'transcribe_job_name': transcribe_job_name,
                'video_id': video_id,
            })
        }

    except Exception as e:
        logger.error(f"Error starting transcription job: {e}")
        raise # Re-raise the exception for Lambda to catch