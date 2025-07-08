import json
import boto3
import os
from datetime import datetime, timedelta

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
stepfunctions = boto3.client('stepfunctions')
eventbridge = boto3.client('events')

# Reference to processed files table
processed_files_table = dynamodb.Table('ProcessedFiles')

# Table to track pending batches for time-based triggering
pending_batches_table = dynamodb.Table('PendingBatches')

def lambda_handler(event, context):
    """
    Enhanced file detector with batch size and time-based triggering
    """
    
    try:

        # NEW: Check if this is a delayed trigger from EventBridge
        if 'source' in event and event['source'] == 'delayed-trigger':
            print(f"⏰ Processing delayed trigger for bucket: {event['bucket']}")
            return handle_delayed_trigger(event, context)
        
        # Handle normal S3 events
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
            
            # Check batch status and trigger conditions
            batch_status = check_batch_status(bucket, key)
            
            if batch_status['should_trigger']:
                print(f"🚀 Triggering pipeline: {batch_status['reason']}")
                # Mark files as IN_PROGRESS before processing
                mark_files_in_progress(batch_status['batch_files'])
                trigger_pipeline(bucket, batch_status['batch_files'])
                
                # Clear any pending batch timer
                clear_pending_batch_timer(bucket)
            elif batch_status['has_minimum_files']:
                print(f"⏰ Minimum files present but not enough for immediate trigger. Setting 20-minute timer.")
                schedule_delayed_trigger(bucket, batch_status['batch_files'])
        
        return {'statusCode': 200, 'body': 'Processing completed'}
        
    except Exception as e:
        print(f"❌ Error processing S3 event: {str(e)}")
        raise

def check_batch_status(bucket, triggered_key):
    """
    Enhanced batch checking with size and time-based logic
    """
    
    try:
        # List all files in raw-data folder
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix='raw-data/'
        )
        
        if 'Contents' not in response:
            return {'should_trigger': False, 'has_minimum_files': False}
        
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
        
        # Check minimum requirement: at least one file in each directory
        has_minimum_files = (
            len(files_by_type['products_files']) > 0 and 
            len(files_by_type['orders_files']) > 0 and 
            len(files_by_type['order_items_files']) > 0
        )
        
        if not has_minimum_files:
            missing_types = [k.replace('_files', '') for k, v in files_by_type.items() if not v]
            print(f"⏳ Minimum requirement not met - missing files in: {missing_types}")
            return {'should_trigger': False, 'has_minimum_files': False}
        
        # Calculate total files
        total_files = sum(len(files) for files in files_by_type.values())
        
        print(f"📊 Current file count: {total_files} total ({len(files_by_type['products_files'])} products, {len(files_by_type['orders_files'])} orders, {len(files_by_type['order_items_files'])} order_items)")
        
        # Check if we should trigger immediately (10+ files)
        if total_files >= 10:
            return {
                'should_trigger': True,
                'has_minimum_files': True,
                'batch_files': files_by_type,
                'reason': f'Batch size threshold reached: {total_files} files'
            }
        
        # Check if we have a pending timer that should trigger now
        if should_trigger_pending_batch(bucket):
            return {
                'should_trigger': True,
                'has_minimum_files': True,
                'batch_files': files_by_type,
                'reason': f'20-minute timer expired with {total_files} files'
            }
        
        # Minimum files present but not enough to trigger immediately
        return {
            'should_trigger': False,
            'has_minimum_files': True,
            'batch_files': files_by_type,
            'total_files': total_files
        }
            
    except Exception as e:
        print(f"❌ Error checking batch status: {str(e)}")
        return {'should_trigger': False, 'has_minimum_files': False}

def schedule_delayed_trigger(bucket, batch_files):
    """
    Schedule a delayed trigger using EventBridge and DynamoDB tracking
    """
    
    try:
        # Check if we already have a pending batch
        existing_batch = get_pending_batch(bucket)
        
        if existing_batch:
            print(f"⏰ Timer already set for {existing_batch['trigger_time']}")
            return
        
        # Set trigger time to 20 minutes from now
        trigger_time = datetime.now() + timedelta(minutes=20)
        
        # Store pending batch info in DynamoDB
        pending_batches_table.put_item(
            Item={
                'bucket_name': bucket,
                'created_at': datetime.now().isoformat(),
                'trigger_time': trigger_time.isoformat(),
                'total_files': sum(len(files) for files in batch_files.values()),
                'file_counts': {
                    'products': len(batch_files['products_files']),
                    'orders': len(batch_files['orders_files']),
                    'order_items': len(batch_files['order_items_files'])
                },
                'status': 'PENDING'
            }
        )
        
        # Schedule EventBridge rule to trigger this Lambda after 20 minutes
        rule_name = f"delayed-trigger-{bucket}-{int(datetime.now().timestamp())}"
        
        # Create EventBridge rule
        eventbridge.put_rule(
            Name=rule_name,
            ScheduleExpression=f"at({trigger_time.strftime('%Y-%m-%dT%H:%M:%S')})",
            State='ENABLED',
            Description=f'Delayed trigger for batch processing in {bucket}'
        )
        
        # Add Lambda as target
        eventbridge.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': context.invoked_function_arn,
                    'Input': json.dumps({
                        'source': 'delayed-trigger',
                        'bucket': bucket,
                        'rule_name': rule_name
                    })
                }
            ]
        )
        
        print(f"⏰ Scheduled delayed trigger for {trigger_time.strftime('%Y-%m-%d %H:%M:%S')} with rule: {rule_name}")
        
    except Exception as e:
        print(f"❌ Error scheduling delayed trigger: {str(e)}")

def should_trigger_pending_batch(bucket):
    """
    Check if there's a pending batch that should trigger now
    """
    
    try:
        pending_batch = get_pending_batch(bucket)
        
        if not pending_batch:
            return False
        
        trigger_time = datetime.fromisoformat(pending_batch['trigger_time'])
        
        # Check if trigger time has passed
        if datetime.now() >= trigger_time:
            print(f"⏰ Pending batch timer expired, triggering now")
            return True
        
        return False
        
    except Exception as e:
        print(f"❌ Error checking pending batch: {str(e)}")
        return False

def get_pending_batch(bucket):
    """
    Get pending batch info from DynamoDB
    """
    
    try:
        response = pending_batches_table.get_item(
            Key={'bucket_name': bucket}
        )
        
        if 'Item' in response and response['Item']['status'] == 'PENDING':
            return response['Item']
        
        return None
        
    except Exception as e:
        print(f"❌ Error getting pending batch: {str(e)}")
        return None

def clear_pending_batch_timer(bucket):
    """
    Clear pending batch timer and EventBridge rule
    """
    
    try:
        # Get pending batch info
        pending_batch = get_pending_batch(bucket)
        
        if pending_batch:
            # Update status to TRIGGERED
            pending_batches_table.update_item(
                Key={'bucket_name': bucket},
                UpdateExpression='SET #status = :status, triggered_at = :triggered_at',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':status': 'TRIGGERED',
                    ':triggered_at': datetime.now().isoformat()
                }
            )
            
            print(f"✅ Cleared pending batch timer for {bucket}")
        
    except Exception as e:
        print(f"❌ Error clearing pending batch timer: {str(e)}")

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

def trigger_pipeline(bucket, batch_files):
    """
    Trigger Step Functions with ALL discovered file paths
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

def handle_delayed_trigger(event, context):
    """
    Handle EventBridge-scheduled delayed triggers
    """
    
    try:
        bucket = event['bucket']
        rule_name = event.get('rule_name')
        
        print(f"⏰ Processing delayed trigger for bucket: {bucket}")
        
        # Check current batch status
        batch_status = check_batch_status(bucket, None)
        
        if batch_status['has_minimum_files']:
            print(f"🚀 Triggering delayed pipeline with {batch_status.get('total_files', 0)} files")
            
            # Mark files as IN_PROGRESS
            mark_files_in_progress(batch_status['batch_files'])
            
            # Trigger pipeline
            trigger_pipeline(bucket, batch_status['batch_files'])
            
            # Clean up the EventBridge rule
            if rule_name:
                cleanup_eventbridge_rule(rule_name)
            
            # Clear pending batch
            clear_pending_batch_timer(bucket)
        else:
            print(f"⚠️ Delayed trigger fired but minimum files no longer present")
        
        return {'statusCode': 200, 'body': 'Delayed trigger processed'}
        
    except Exception as e:
        print(f"❌ Error processing delayed trigger: {str(e)}")
        raise

def cleanup_eventbridge_rule(rule_name):
    """
    Clean up EventBridge rule after use
    """
    
    try:
        # Remove targets first
        eventbridge.remove_targets(
            Rule=rule_name,
            Ids=['1']
        )
        
        # Delete the rule
        eventbridge.delete_rule(Name=rule_name)
        
        print(f"🧹 Cleaned up EventBridge rule: {rule_name}")
        
    except Exception as e:
        print(f"❌ Error cleaning up EventBridge rule {rule_name}: {str(e)}")

