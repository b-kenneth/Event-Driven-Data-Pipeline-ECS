import json
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')

# Reference to processed files table
processed_files_table = dynamodb.Table('ProcessedFiles')

def lambda_handler(event, context):
    """
    Enhanced file detector with deduplication and archival support
    """
    
    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            print(f"📁 File detected: s3://{bucket}/{key}")
            
            # Skip if file is in processed or failed directories
            if key.startswith(('processed/', 'failed/')):
                print(f"⏭️ Skipping archived file: {key}")
                continue
            
            # Check if file was already processed
            if is_file_already_processed(key):
                print(f"⚠️ File already processed, moving to archive: {key}")
                move_to_processed_archive(bucket, key)
                continue
            
            # Check for complete batch with unprocessed files only
            batch_files = check_for_complete_batch(bucket, key)
            if batch_files:
                # Mark files as IN_PROGRESS before processing
                mark_files_in_progress(batch_files)
                trigger_pipeline(bucket, batch_files)  # ✅ This function was missing!
        
        return {'statusCode': 200, 'body': 'Processing completed'}
        
    except Exception as e:
        print(f"❌ Error processing S3 event: {str(e)}")
        raise

def is_file_already_processed(file_key):
    """Check if file was already successfully processed"""
    try:
        response = processed_files_table.get_item(
            Key={'file_key': file_key}
        )
        
        if 'Item' in response:
            status = response['Item'].get('processing_status')
            if status == 'SUCCESS':
                print(f"✅ File already processed successfully: {file_key}")
                return True
            elif status == 'IN_PROGRESS':
                print(f"🔄 File currently being processed: {file_key}")
                return True
        
        return False
        
    except Exception as e:
        print(f"❌ Error checking processed files: {str(e)}")
        return False

def mark_files_in_progress(batch_files):
    """Mark files as IN_PROGRESS to prevent reprocessing"""
    try:
        with processed_files_table.batch_writer() as batch:
            for file_type, file_list in batch_files.items():
                for file_key in file_list:
                    batch.put_item(
                        Item={
                            'file_key': file_key,
                            'processing_date': datetime.now().strftime('%Y-%m-%d'),
                            'processing_status': 'IN_PROGRESS',
                            'created_at': datetime.now().isoformat(),
                            'file_type': file_type.replace('_files', '')
                        }
                    )
        
        print(f"📝 Marked {sum(len(files) for files in batch_files.values())} files as IN_PROGRESS")
        
    except Exception as e:
        print(f"❌ Error marking files in progress: {str(e)}")
        raise

def move_to_processed_archive(bucket, file_key):
    """Move already processed file to archive"""
    try:
        # Generate archive path
        date_str = datetime.now().strftime('%Y/%m/%d')
        archive_key = f"processed/{date_str}/{file_key.replace('raw-data/', '')}"
        
        # Copy to archive location
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': file_key},
            Key=archive_key
        )
        
        # Delete from raw-data
        s3.delete_object(Bucket=bucket, Key=file_key)
        
        print(f"📦 Moved to archive: {file_key} → {archive_key}")
        
    except Exception as e:
        print(f"❌ Error moving file to archive: {str(e)}")

def check_for_complete_batch(bucket, triggered_key):
    """
    Check for complete batch, excluding already processed files
    """
    
    try:
        # List all files in raw-data folder
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix='raw-data/'
        )
        
        if 'Contents' not in response:
            return None
        
        # Categorize unprocessed files only
        files_by_type = {
            'products_files': [],
            'orders_files': [],
            'order_items_files': []
        }
        
        for obj in response['Contents']:
            key = obj['Key']
            filename = key.split('/')[-1].lower()
            
            if not filename.endswith('.csv'):
                continue
            
            # Skip if already processed
            if is_file_already_processed(key):
                continue
            
            # Categorize files
            if 'product' in filename:
                files_by_type['products_files'].append(key)
            elif 'order_item' in filename or 'orderitem' in filename:
                files_by_type['order_items_files'].append(key)
            elif 'order' in filename:
                files_by_type['orders_files'].append(key)
        
        # Check if we have at least one unprocessed file of each type
        if (files_by_type['products_files'] and 
            files_by_type['orders_files'] and 
            files_by_type['order_items_files']):
            
            print(f"✅ Complete unprocessed batch found")
            return files_by_type
        else:
            missing_types = [k.replace('_files', '') for k, v in files_by_type.items() if not v]
            print(f"⏳ Incomplete batch - missing unprocessed: {missing_types}")
            return None
            
    except Exception as e:
        print(f"❌ Error checking for complete batch: {str(e)}")
        return None

def trigger_pipeline(bucket, batch_files):
    """
    Trigger Step Functions with ALL discovered file paths
    ✅ THIS FUNCTION WAS MISSING FROM YOUR NEW CODE!
    """
    
    try:
        # Generate unique batch ID
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        batch_id = f"auto-batch-{timestamp}"
        
        # Prepare Step Functions input with ALL discovered files
        input_data = {
            'products_files': batch_files['products_files'],      # List of files
            'orders_files': batch_files['orders_files'],          # List of files
            'order_items_files': batch_files['order_items_files'], # List of files
            'batch_id': batch_id,
            'bucket_name': bucket,
            'trigger_timestamp': datetime.now().isoformat(),
            'total_files': {
                'products': len(batch_files['products_files']),
                'orders': len(batch_files['orders_files']),
                'order_items': len(batch_files['order_items_files'])
            }
        }
        
        # Start Step Functions execution
        response = stepfunctions.start_execution(
            stateMachineArn=os.environ['STEP_FUNCTIONS_ARN'],
            name=f"execution-{batch_id}",
            input=json.dumps(input_data)
        )
        
        print(f"🚀 Started Step Functions execution: {response['executionArn']}")
        print(f"📋 Processing {sum(len(files) for files in batch_files.values())} total files")
        
    except Exception as e:
        print(f"❌ Error starting Step Functions: {str(e)}")
        raise
