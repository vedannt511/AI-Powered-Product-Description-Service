import os
import subprocess
import json
import boto3
import uuid
from urllib.parse import unquote_plus # Ensure this is at the top of your file!
import logging

# Configure logging for better debug output
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# Environment variables
INPUT_BUCKET = os.environ.get('INPUT_BUCKET')
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET')
FFMPEG_PATH = '/opt/bin/ffmpeg'

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = event['Records'][0]['s3']['object']['key']

        # CRITICAL FIX: The most reliable way to decode S3 event keys
        # It handles both plus signs and standard URL-encoded characters.
        decoded_source_key = unquote_plus(source_key)

        video_id = str(uuid.uuid4())
        download_path = f'/tmp/{video_id}_{os.path.basename(decoded_source_key)}'
        audio_output_path = f'/tmp/{video_id}.mp3'
        frame_output_dir = f'/tmp/{video_id}_frames/'

        # Check for and create directories
        if not os.path.exists(frame_output_dir):
            os.makedirs(frame_output_dir)

        logger.info(f"Processing video: s3://{source_bucket}/{decoded_source_key}")
        logger.info(f"Downloading to: {download_path}")

        # 1. Download video from S3 using the correctly decoded key
        s3.download_file(source_bucket, decoded_source_key, download_path)
        logger.info("Video downloaded successfully.")

        # 2. Extract Audio
        audio_cmd = [
            FFMPEG_PATH, '-i', download_path, '-vn', '-acodec', 'libmp3lame', '-q:a', '2', audio_output_path
        ]
        logger.info(f"Executing audio extraction: {' '.join(audio_cmd)}")
        subprocess.run(audio_cmd, check=True)
        logger.info(f"Audio extracted to: {audio_output_path}")

        # 3. Extract Frames every 5 seconds
        frame_cmd = [
            FFMPEG_PATH, '-i', download_path, '-vf', 'fps=1/5', f'{frame_output_dir}frame_%03d.jpg'
        ]
        logger.info(f"Executing frame extraction: {' '.join(frame_cmd)}")
        subprocess.run(frame_cmd, check=True)
        logger.info(f"Frames extracted to: {frame_output_dir}")

        # 4. Upload extracted audio and frames to S3 Output Bucket
        audio_s3_key = f'{video_id}/audio/{video_id}.mp3'
        s3.upload_file(audio_output_path, OUTPUT_BUCKET, audio_s3_key)
        logger.info(f"Uploaded audio to s3://{OUTPUT_BUCKET}/{audio_s3_key}")

        extracted_frame_keys = []
        for frame_file in os.listdir(frame_output_dir):
            if frame_file.endswith('.jpg'):
                frame_local_path = os.path.join(frame_output_dir, frame_file)
                frame_s3_key = f'{video_id}/frames/timed/{frame_file}'
                s3.upload_file(frame_local_path, OUTPUT_BUCKET, frame_s3_key)
                extracted_frame_keys.append(frame_s3_key)
                logger.info(f"Uploaded frame {frame_file} to s3://{OUTPUT_BUCKET}/{frame_s3_key}")

        # Clean up /tmp directory
        subprocess.run(['rm', '-rf', '/tmp/*'], check=True)
        logger.info("Cleaned /tmp directory.")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Video processed successfully',
                'video_id': video_id,
                'audio_s3_key': audio_s3_key,
                'extracted_frame_keys': extracted_frame_keys,
                'source_key': source_key,
                'source_bucket': source_bucket
            })
        }

    except Exception as e:
        logger.error(f"Error processing video: {e}")
        subprocess.run(['rm', '-rf', '/tmp/*'], check=True)
        raise