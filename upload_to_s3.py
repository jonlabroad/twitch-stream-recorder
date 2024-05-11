import boto3
import os
import asyncio

# Initialize S3 client
s3_client = boto3.client('s3')

async def upload_file(streamer, file_path):
    print(f"Uploading file for streamer: {streamer}, File Path: {file_path}")
    key = f"streams/{streamer}"

    try:
        # Read file stream
        with open(file_path, 'rb') as file_stream:
            # Upload file to S3
            response = s3_client.upload_fileobj(
                Fileobj=file_stream,
                Bucket="twitch-stream-archive",
                Key=key
            )
        print(f"File uploaded successfully: {key}")
        
        # Delete file from file system
        os.remove(file_path)
        print("File deleted from file system.")
        
    except Exception as e:
        print(f"Error uploading file: {key}", e)
        raise e

async def main(path):   
    # Process files
    files = os.listdir(path)
    print(f"Files: {files}")
    for file in files:
        try:
            full_path = os.path.join(path, file)
            await upload_file(f"{path}/{file}", full_path)
        except Exception as e:
            print(e)

def upload(path):
    asyncio.run(main(path))