import boto3
import pandas as pd
import json
from datetime import datetime

# Initialize S3 and SNS clients
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Define the S3 bucket names
source_bucket_name = 'doordash-landing-zn'
destination_bucket_name = 'doordash-target-zn'

# Define the SNS topic ARN
sns_topic_arn = 'arn:aws:sns:ap-south-1:905418304113:Doordash'

# Initialize a set to store unique identifiers of recipients who have already received notifications
already_notified = set()

def lambda_handler(event, context):
    try:
        # Get the uploaded JSON file from the event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        
        # Download the JSON file from the source S3 bucket
        response = s3.get_object(Bucket=source_bucket, Key=file_key)
        body_content = response['Body'].read().decode('utf-8')
        
        # Load JSON data
        delivery_data = json.loads(body_content)
        
        # Convert JSON data to DataFrame
        df = pd.DataFrame(delivery_data)
        
        # Filter the DataFrame based on status
        filtered_df = df[df['status'] == 'delivered']
        
        # Convert the filtered DataFrame back to JSON
        filtered_data = filtered_df.to_dict(orient='records')
        
        # Get the current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Define the destination file name
        destination_key = f'{current_date}-process_data.json'
        
        # Save the filtered data to the destination S3 bucket
        s3.put_object(Bucket=destination_bucket_name, Key=destination_key, Body=json.dumps(filtered_data))
        
        # Send notification about the processing outcome
        message = f"Input S3 File s3://{source_bucket}/{file_key} has been processed successfully!"
        response = sns.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_topic_arn, Message=message)
        
        # Record the unique identifier of the recipient
        recipient_identifier = f"{sns_topic_arn}-{event['Records'][0]['sns']['MessageId']}"
        already_notified.add(recipient_identifier)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Processing completed.')
        }
    except Exception as e:
        # Handle any exceptions
        message = f"Input S3 File s3://{source_bucket}/{file_key} processing has failed: {str(e)}"
        response = sns.publish(Subject="Failure - Daily Data Processing", TargetArn=sns_topic_arn, Message=message)
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing delivery data.')
        }