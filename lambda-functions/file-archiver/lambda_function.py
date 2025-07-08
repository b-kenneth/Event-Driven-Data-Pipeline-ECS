import json
import os
import boto3
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

processed_files_table = dynamodb.Table('ProcessedFiles')

def lambda_handler(event, context):
    """
    Archive files after processing and update tracking table
    """
    
    try:
        batch_id = event['batch_id']
        status = event['status']  # SUCCESS or FAILED
        bucket_name = os.environ['S3_BUCKET_NAME']
        
        # Collect all files from the batch
        all_files = []
        all_files.extend(event.get('products_files', []))
        all_files.extend(event.get('orders_files', []))
        all_files.extend(event.get('order_items_files', []))
        
        print(f"📦 Archiving {len(all_files)} files with status: {status}")
        
        # Archive files based on status
        if status == 'SUCCESS':
            archive_successful_files(bucket_name, all_files, batch_id)
        else:
            archive_failed_files(bucket_name, all_files, batch_id, event.get('error'))
        
        # Update processed files table
        update_processed_files_status(all_files, batch_id, status)
        
        return {
            'statusCode': 200,
            'body': f'Successfully archived {len(all_files)} files'
        }
        
    except Exception as e:
        print(f"❌ Error archiving files: {str(e)}")
        raise

def archive_successful_files(bucket, file_list, batch_id):
    """Move successfully processed files to processed archive"""
    
    date_str = datetime.now().strftime('%Y/%m/%d')
    
    for file_key in file_list:
        try:
            # Generate archive path
            archive_key = f"processed/{date_str}/{file_key.replace('raw-data/', '')}"
            
            # Copy to archive
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': file_key},
                Key=archive_key
            )
            
            # Delete from raw-data
            s3.delete_object(Bucket=bucket, Key=file_key)
            
            print(f"✅ Archived successful file: {file_key} → {archive_key}")
            
        except Exception as e:
            print(f"❌ Error archiving file {file_key}: {str(e)}")

def archive_failed_files(bucket, file_list, batch_id, error_info):
    """Move failed files to failed archive with error details"""
    
    date_str = datetime.now().strftime('%Y/%m/%d')
    
    for file_key in file_list:
        try:
            # Generate failed archive path
            archive_key = f"failed/{date_str}/{file_key.replace('raw-data/', '')}"
            
            # Copy to failed archive
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': file_key},
                Key=archive_key
            )
            
            # Delete from raw-data
            s3.delete_object(Bucket=bucket, Key=file_key)
            
            print(f"❌ Archived failed file: {file_key} → {archive_key}")
            
        except Exception as e:
            print(f"❌ Error archiving failed file {file_key}: {str(e)}")
    
    # Store error details
    error_log_key = f"failed/{date_str}/error-logs/{batch_id}_error.json"
    s3.put_object(
        Bucket=bucket,
        Key=error_log_key,
        Body=json.dumps({
            'batch_id': batch_id,
            'error': error_info,
            'files': file_list,
            'timestamp': datetime.now().isoformat()
        }, indent=2)
    )

def update_processed_files_status(file_list, batch_id, status):
    """Update processed files table with final status"""
    
    try:
        with processed_files_table.batch_writer() as batch:
            for file_key in file_list:
                batch.put_item(
                    Item={
                        'file_key': file_key,
                        'processing_date': datetime.now().strftime('%Y-%m-%d'),
                        'batch_id': batch_id,
                        'processing_status': status,
                        'processed_at': datetime.now().isoformat(),
                        'archive_location': f"{'processed' if status == 'SUCCESS' else 'failed'}/{datetime.now().strftime('%Y/%m/%d')}/{file_key.replace('raw-data/', '')}"
                    }
                )
        
        print(f"📝 Updated {len(file_list)} files with status: {status}")
        
    except Exception as e:
        print(f"❌ Error updating processed files table: {str(e)}")
