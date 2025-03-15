from app.database import groups_collection, messages_collection
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.telegram_client import telegram_client
from app.services.threat_service import process_message_for_alerts, setup_scheduled_email_alerts
from contextlib import asynccontextmanager
from app.routers import messages, groups, categories, auth, alerts, threats
from app.utils.gpt_generations import generate_tags, save_tags
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta, timezone
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
import boto3
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, RPCError
from mimetypes import guess_extension
from app.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, CLIENT_URL, CRON_JOB_DURATION
import logging
import os
import asyncio
import socketio
import re
import math
import random
import time
import uuid
from starlette.concurrency import run_in_threadpool

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize the scheduler
scheduler = AsyncIOScheduler()

# S3 Configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# Define S3 buckets and their regions
buckets = {
    "telescopebucket0212": "eu-north-1",
    "telescopebucket101": "ap-south-1",
}

# Ensure the temp_media directory exists
temp_media_dir = "temp_media"
os.makedirs(temp_media_dir, exist_ok=True)

# Semaphores for various operations
upload_semaphore = asyncio.Semaphore(5)  # For S3 uploads
group_semaphore = asyncio.Semaphore(1)   # For processing groups
api_semaphore = asyncio.Semaphore(50)    # For regular API endpoints

# Keep track of the scheduler and its job
scheduler_job_id = "update_telegram_groups_job"

# Separate rate limit tracking for Telegram API calls
telegram_last_api_call = datetime.now(timezone.utc)
telegram_api_call_count = 0
telegram_api_call_history = []  # Track timestamps of recent API calls
TELEGRAM_API_CALL_RESET_INTERVAL = 60  # seconds
TELEGRAM_API_CALL_HISTORY_WINDOW = 300  # 5 minutes
TELEGRAM_MAX_API_CALLS_PER_INTERVAL = 20  # Base limit
TELEGRAM_MAX_API_CALLS_PER_MINUTE = 25  # Hard limit per minute
TELEGRAM_MAX_API_CALLS_PER_5_MINUTES = 100  # Hard limit per 5 minutes

# Flag to indicate if Telegram operations are running
telegram_operations_running = False
telegram_operations_lock = asyncio.Lock()

# Track ongoing operations
ongoing_operations = {}

async def wait_for_telegram_rate_limit():
    """
    Advanced rate limiting function specifically for Telegram API calls
    Uses a dynamic approach based on recent activity patterns
    """
    global telegram_last_api_call, telegram_api_call_count, telegram_api_call_history
    
    current_time = datetime.now(timezone.utc)
    
    # Clean up old history entries
    cutoff_time = current_time - timedelta(seconds=TELEGRAM_API_CALL_HISTORY_WINDOW)
    telegram_api_call_history = [timestamp for timestamp in telegram_api_call_history if timestamp > cutoff_time]
    
    # Add current call to history
    telegram_api_call_history.append(current_time)
    
    # Calculate calls in the last minute and 5 minutes
    one_minute_ago = current_time - timedelta(seconds=60)
    calls_last_minute = len([t for t in telegram_api_call_history if t > one_minute_ago])
    calls_last_5_minutes = len(telegram_api_call_history)
    
    # 1. Check 5-minute limit (most strict Telegram limit)
    if calls_last_5_minutes >= TELEGRAM_MAX_API_CALLS_PER_5_MINUTES:
        wait_time = 30 + random.uniform(0, 10)  # 30-40 second cooldown
        logger.warning(f"Approaching 5-minute rate limit ({calls_last_5_minutes} calls). Cooling down for {wait_time:.2f}s")
        await asyncio.sleep(wait_time)
        # Don't use recursion - it can lead to stack overflow
        # Just continue after cooling down
    
    # 2. Check 1-minute limit
    elif calls_last_minute >= TELEGRAM_MAX_API_CALLS_PER_MINUTE:
        wait_time = 10 + random.uniform(0, 5)  # 10-15 second cooldown
        logger.warning(f"Approaching 1-minute rate limit ({calls_last_minute} calls). Cooling down for {wait_time:.2f}s")
        await asyncio.sleep(wait_time)
    
    # 3. Check interval counter (our self-imposed limit)
    time_diff = (current_time - telegram_last_api_call).total_seconds()
    if time_diff > TELEGRAM_API_CALL_RESET_INTERVAL:
        # Reset counter if interval has passed
        telegram_api_call_count = 0
        telegram_last_api_call = current_time
    
    if telegram_api_call_count >= TELEGRAM_MAX_API_CALLS_PER_INTERVAL:
        # Wait until the interval resets
        wait_time = TELEGRAM_API_CALL_RESET_INTERVAL - time_diff + random.uniform(1, 3)
        if wait_time > 0:
            logger.info(f"Self-imposed rate limiting: waiting {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)
            # Reset after waiting
            telegram_api_call_count = 0
            telegram_last_api_call = datetime.now(timezone.utc)
    
    # 4. Dynamic delay based on recent activity
    if calls_last_minute > TELEGRAM_MAX_API_CALLS_PER_MINUTE * 0.7:  # If at 70% of limit
        delay = random.uniform(1.5, 3.0)  # Longer delay when approaching limits
    elif calls_last_minute > TELEGRAM_MAX_API_CALLS_PER_MINUTE * 0.5:  # If at 50% of limit
        delay = random.uniform(0.5, 1.5)  # Medium delay
    else:
        delay = random.uniform(0.1, 0.5)  # Standard small random delay
    
    await asyncio.sleep(delay)
    
    # Increment the counter
    telegram_api_call_count += 1


async def handle_flood_wait_error(e, entity_name="unknown"):
    """
    Centralized handler for FloodWaitError with proper logging and backoff
    """
    wait_time = e.seconds
    
    # Add some extra padding to the wait time to be safe
    wait_time_with_padding = wait_time * 1.1 + random.uniform(1, 5)
    
    logger.warning(
        f"⚠️ RATE LIMITED: FloodWaitError for {entity_name}. "
        f"Telegram requires waiting {wait_time} seconds. "
        f"Waiting {wait_time_with_padding:.2f} seconds with safety padding."
    )
    
    # If wait time is very long, log at error level
    if wait_time > 300:  # > 5 minutes
        logger.error(
            f"🛑 SEVERE RATE LIMITING: Required to wait {wait_time} seconds ({wait_time/60:.1f} minutes). "
            f"Consider reducing frequency of operations or adding more accounts."
        )
    
    await asyncio.sleep(wait_time_with_padding)
    logger.info(f"Resuming after FloodWaitError wait for {entity_name}")


def get_extension_from_message(message):
    """
    Determine the correct file extension for a media file based on the Telegram message.
    """
    if isinstance(message.media, MessageMediaPhoto):
        return ".jpg"  # Default extension for photos
    elif isinstance(message.media, MessageMediaDocument):
        if hasattr(message.media.document, "mime_type"):
            mime_type = message.media.document.mime_type
            ext = guess_extension(mime_type)
            if ext:
                return ext
        if hasattr(message.media.document, "attributes"):
            for attr in message.media.document.attributes:
                if hasattr(attr, "file_name") and "." in attr.file_name:
                    return os.path.splitext(attr.file_name)[-1]
    return None


async def download_media_with_extension(message, retries=5, backoff_factor=2):
    """
    Download media from Telegram with retry logic, dynamically determining the file extension.
    No file size limits.
    """
    # Wait for rate limit before downloading
    await wait_for_telegram_rate_limit()
    
    extension = get_extension_from_message(message)
    if not extension:
        logger.warning(
            f"Unable to determine file extension for message ID {message.id}. Skipping...")
        return None
        
    # Log file size for monitoring but don't skip based on size
    if isinstance(message.media, MessageMediaDocument) and hasattr(message.media.document, "size"):
        file_size_mb = message.media.document.size / (1024 * 1024)  # Convert to MB
        logger.info(f"Downloading file for message ID {message.id}: File size {file_size_mb:.2f} MB")

    file_name = f"{message.id}{extension}"
    file_path = os.path.join(temp_media_dir, file_name)

    attempt = 0
    while attempt < retries:
        try:
            await telegram_client.download_media(message, file_path)
            if os.path.exists(file_path):
                return {"file_path": file_path, "file_name": file_name}
        except FloodWaitError as e:
            await handle_flood_wait_error(e, f"media download for message {message.id}")
            attempt += 1
        except Exception as e:
            attempt += 1
            logger.error(
                f"Error downloading media (attempt {attempt}/{retries}): {e}")
            await asyncio.sleep(backoff_factor ** attempt)

    logger.error(
        f"Failed to download media after {retries} attempts: {file_path}")
    return None


async def multipart_upload_to_s3(file_path: str, s3_key: str, bucket_name: str, region: str) -> str:
    """
    Perform a multipart upload to S3 with no file size limit.
    """
    try:
        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)
        logger.info(f"Uploading file {file_path} to S3: Size {file_size_mb:.2f} MB")

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=region,
        )

        chunk_size = 5 * 1024 * 1024
        response = s3_client.create_multipart_upload(
            Bucket=bucket_name, Key=s3_key)
        upload_id = response["UploadId"]
        parts = []

        with open(file_path, "rb") as file:
            part_number = 1
            while chunk := file.read(chunk_size):
                part_response = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                parts.append({"PartNumber": part_number,
                             "ETag": part_response["ETag"]})
                part_number += 1

        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )

        return f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"

    except Exception as e:
        logger.error(
            f"Failed to perform multipart upload for file {file_path}: {str(e)}")
        return None


async def process_message_media(message, group_id: int, bucket_index: int):
    """
    Process media from a message: download, upload to S3, and return the URL.
    """
    if not message.media:
        return None

    downloaded_file = await download_media_with_extension(message)
    if not downloaded_file:
        return None  # Skip processing if media download failed

    try:
        async with upload_semaphore:
            bucket_name = list(buckets.keys())[bucket_index % len(buckets)]
            region = buckets[bucket_name]
            s3_key = f"{group_id}/{downloaded_file['file_name']}"

            # Use run_in_threadpool to avoid blocking the event loop during S3 uploads
            # This ensures API endpoints remain responsive
            s3_url = await run_in_threadpool(
                lambda: asyncio.run(multipart_upload_to_s3(
                    downloaded_file['file_path'], s3_key, bucket_name, region
                ))
            )

            if s3_url:
                logger.info(
                    f"Media uploaded to S3 bucket {bucket_name}: {s3_url}")
                return s3_url

    except Exception as e:
        logger.error(
            f"Error processing media for message {message.id}: {str(e)}")
        return None

    finally:
        # Ensure the downloaded file is deleted after upload
        if downloaded_file and os.path.exists(downloaded_file["file_path"]):
            os.remove(downloaded_file["file_path"])

    return None


async def process_single_group(group, bucket_index):
    """Process a single group and collect new messages."""
    operation_id = str(uuid.uuid4())
    operation_description = f"Processing group {group['username']}"
    ongoing_operations[operation_id] = {
        "start_time": datetime.now(timezone.utc),
        "description": operation_description,
        "status": "running"
    }
    
    try:
        # Wait for rate limit before getting entity
        await wait_for_telegram_rate_limit()
        
        retry_count = 0
        max_retries = 5
        backoff_factor = 2
        
        while retry_count < max_retries:
            try:
                entity = await telegram_client.get_entity(group["username"])
                if not entity:
                    logger.warning(f"Group {group['username']} is invalid")
                    await groups_collection.update_one(
                        {"_id": group["_id"]},
                        {"$set": {"is_active": False}}
                    )
                    return []
                break  # Successfully got entity, exit retry loop
            except FloodWaitError as e:
                await handle_flood_wait_error(e, group["username"])
                continue  # Retry the same group
            except Exception as e:
                if "FLOOD_WAIT" in str(e):
                    # Extract wait time from the error message
                    wait_match = re.search(r"A wait of (\d+) seconds", str(e))
                    wait_time = int(wait_match.group(1)) if wait_match else 60
                    logger.warning(f"Rate limited for {group['username']}, waiting for {wait_time} seconds")
                    await asyncio.sleep(wait_time * 1.1)  # Add 10% extra time to be safe
                    retry_count += 1
                else:
                    logger.error(f"Error verifying group {group['username']}: {str(e)}")
                    return []
        
        if retry_count >= max_retries:
            logger.error(f"Failed to get entity for {group['username']} after {max_retries} attempts")
            return []

        # Get the latest message from our database
        latest_message = await messages_collection.find_one(
            {"group_id": group["group_id"]},
            sort=[("date", -1)]
        )

        # Set the min_date to latest stored message date
        min_date = None
        if latest_message:
            min_date = latest_message["date"]
            if min_date.tzinfo is None:
                min_date = min_date.replace(tzinfo=timezone.utc)

        # Remove all pagination variables and logic
        new_messages = []
        messages_processed = 0
        
        # Get messages in a single batch (no pagination)
        try:
            batch_size = 200  # Get up to 200 messages at once
            messages_in_batch = 0
            
            # Process messages in a single batch, no pagination
            try:
                async for message in telegram_client.iter_messages(
                    entity,
                    limit=batch_size,
                    reverse=True,
                    offset_date=min_date
                ):
                    # Periodically yield control to let other tasks run
                    if messages_processed % 5 == 0:
                        await asyncio.sleep(0)  # Just yield control, don't actually sleep
                    
                # Simplify the handling of message date
                    try:
                        if message.date:
                            message_date = message.date
                            if message_date.tzinfo is None:
                                message_date = message_date.replace(tzinfo=timezone.utc)
                        else:
                            # Use current time if message date is missing
                            message_date = datetime.now(timezone.utc)
                    except Exception:
                        # Fallback if any date-related error occurs
                        message_date = datetime.now(timezone.utc)

                    # Skip if message is older than our latest stored message (safely)
                    try:
                        if min_date and message_date <= min_date:
                            continue
                    except Exception:
                        # Skip date comparison errors
                        pass

                    logger.info(
                        f"Processing new message {message.id} from {group['username']} "
                        f"recorded at {message_date}, content: {message.text[:100] if message.text else 'No text'}..."
                    )

                    tags = []

                    if message.media:
                        media_url = await process_message_media(message, group["group_id"], bucket_index)
                        bucket_index += 1  # Increment bucket index for distributing uploads
                    else:
                        media_url = None

                    # Uncomment if tag generation is needed
                    # if message.text:
                    #     # Generate tags for the message using GPT-4
                    #     tags = await generate_tags(message.text)
                    #     # Save unique tags to the database
                    #     await save_tags(tags)

                    message_doc = {
                        "message_id": message.id,
                        "group_id": group["group_id"],
                        "text": message.text or "",
                        "date": message_date,
                        "sender_id": message.sender_id,
                        "created_at": datetime.now(timezone.utc),
                        "media": media_url if media_url else None,
                        "tags": tags if tags else []
                    }

                    # Upsert the message in the database
                    result = await messages_collection.update_one(
                        {
                            "message_id": message_doc["message_id"],
                            "group_id": message_doc["group_id"]
                        },
                        {"$set": message_doc},
                        upsert=True
                    )

                    # Format message for socket broadcasting that matches frontend expectations
                    formatted_message = {
                        "id": str(message.id),  # Frontend expects string IDs
                        "message_id": message.id, 
                        "group_id": group["group_id"],
                        "channel": group["title"],  # Use group title from database
                        "timestamp": message_date.isoformat(),
                        "content": message.text or "No content",
                        "tags": tags if tags else [],
                        "has_previous": True  # Assume there might be more previous messages
                    }

                    # Handle media in a format matching frontend expectations
                    if media_url:
                        # Determine media type based on file extension
                        file_ext = os.path.splitext(media_url)[1].lower() if media_url else ""
                        if file_ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                            formatted_message["media"] = {
                                "type": "image", 
                                "url": media_url, 
                                "alt": "Image content"
                            }
                        elif file_ext in ['.mp4', '.mkv', '.avi', '.mov']:
                            formatted_message["media"] = {
                                "type": "video", 
                                "url": media_url, 
                                "thumbnail": "Telegram Video"
                            }
                        else:
                            formatted_message["media"] = {
                                "type": "file", 
                                "url": media_url, 
                                "name": os.path.basename(media_url)
                            }
                    else:
                        formatted_message["media"] = None

                    # Add to new messages for broadcasting
                    new_messages.append(formatted_message)

                    # Process this message for alerts
                    await process_message_for_alerts(message_doc, group)

                    # Send immediate notification for individual messages
                    await sio.emit('new_message', formatted_message)
                    
                    # REMOVE ALL COMPARISONS ENTIRELY
                    # Just collect the message data without any comparison-based logic
                    # Increment our counters
                    messages_processed += 1
                    messages_in_batch += 1
                    
                    # Dynamically adjust frequency based on how many messages we've processed
                    if messages_processed < 50:
                        # Process faster initially
                        if messages_processed % 10 == 0:
                            await asyncio.sleep(0.5)
                    elif messages_processed < 100:
                        # Slow down a bit
                        if messages_processed % 10 == 0:
                            await asyncio.sleep(1)
                    else:
                        # Even more cautious for high volume groups
                        if messages_processed % 5 == 0:
                            await asyncio.sleep(0.5)
                        if messages_processed % 20 == 0:
                            await asyncio.sleep(2)
                        
                # End of messages for loop
                
            except Exception as msg_error:
                logger.error(f"Error in message loop for {group['username']}: {str(msg_error)}")
                
            logger.info(f"Processed {messages_processed} messages from group: {group['username']}")
            
        except Exception as batch_error:
            logger.error(f"Error processing batch for {group['username']}: {str(batch_error)}")
            logger.exception(batch_error)  # Log full traceback
        
        logger.info(f"Successfully processed {messages_processed} messages from group: {group['username']}")
        ongoing_operations[operation_id]["status"] = "completed"
        return new_messages
        
    except Exception as e:
        logger.error(f"Error processing group {group['username']}: {str(e)}")
        ongoing_operations[operation_id]["status"] = "failed"
        ongoing_operations[operation_id]["error"] = str(e)
        return []


async def update_telegram_groups():
    """Background task to update messages from all groups and broadcast new ones via Socket.IO."""
    global telegram_operations_running
    
    # If already running, don't start another instance
    async with telegram_operations_lock:
        if telegram_operations_running:
            logger.info("Update operation already in progress, skipping this run")
            return
        telegram_operations_running = True
    
    operation_id = str(uuid.uuid4())
    operation_description = "Full telegram groups update"
    ongoing_operations[operation_id] = {
        "start_time": datetime.now(timezone.utc),
        "description": operation_description,
        "status": "running"
    }
    
    try:
        current_time = datetime.now(timezone.utc)
        logger.info(f"Starting scheduled update at {current_time}")

        # Get all active groups
        groups = await groups_collection.find({"is_active": True}).to_list(None)
        logger.info(f"Found {len(groups)} active groups to process")
        
        # Shuffle groups to avoid hitting the same channels in the same order every time
        random.shuffle(groups)
        
        # Calculate optimal batch size based on total number of groups
        # Smaller batch sizes for larger total group counts to avoid exhausting rate limits
        total_groups = len(groups)
        if total_groups > 500:
            batch_size = 10
            batch_delay = 600  # 10 minutes between batches for very large collections
        elif total_groups > 200:
            batch_size = 15
            batch_delay = 300  # 5 minutes between batches for large collections
        else:
            batch_size = 25
            batch_delay = 180  # 3 minutes between batches for smaller collections
            
        logger.info(f"Using batch size of {batch_size} with {batch_delay} seconds between batches")
        
        new_messages_all = []
        
        for batch_index in range(0, len(groups), batch_size):
            batch = groups[batch_index:batch_index + batch_size]
            logger.info(f"Processing batch {batch_index // batch_size + 1} with {len(batch)} groups")
            
            for idx, group in enumerate(batch):
                # Use the semaphore to limit concurrent group processing
                async with group_semaphore:
                    try:
                        # Process each group and collect new messages
                        new_messages = await process_single_group(group, idx)
                        new_messages_all.extend(new_messages)
                        
                        # Yield control to let other tasks run
                        await asyncio.sleep(0)
                        
                        # Add a delay between groups to avoid rate limiting
                        wait_time = random.uniform(30, 90)  # Random delay between 30-90 seconds
                        logger.info(f"Waiting {wait_time:.2f} seconds before processing next group")
                        await asyncio.sleep(wait_time)
                    except Exception as e:
                        logger.error(f"Error processing group {group['username']}: {str(e)}")
            
            # Add a longer delay between batches
            if batch_index + batch_size < len(groups):
                logger.info(f"Finished batch {batch_index // batch_size + 1}. Waiting {batch_delay} seconds before next batch")
                
                # Split the wait into smaller chunks to allow other tasks to run
                chunks = 6  # Split into 6 parts (for a 10-minute wait, this is ~100 seconds each)
                for _ in range(chunks):
                    await asyncio.sleep(batch_delay / chunks)
                    # This gives opportunity for other async tasks to run

        # Only broadcast if we have new messages
        if new_messages_all:
            # Group messages by group_id
            grouped_messages = {}
            for msg in new_messages_all:
                group_id = msg["group_id"]
                if group_id not in grouped_messages:
                    grouped_messages[group_id] = []
                grouped_messages[group_id].append(msg)
            
            # For each group, select only the latest message
            latest_group_messages = []
            for group_id, msgs in grouped_messages.items():
                # Sort by timestamp descending and take the first one
                sorted_msgs = sorted(msgs, key=lambda x: x["timestamp"], reverse=True)
                latest_group_messages.append(sorted_msgs[0])
            
            logger.info(f"Broadcasting {len(latest_group_messages)} new messages via Socket.IO...")
            
            # Format the broadcast to match frontend expectations
            await sio.emit('new_messages', {
                "type": "new_messages",
                "data": latest_group_messages
            })

        # Reschedule the next cron job
        next_run_time = datetime.now(timezone.utc) + timedelta(minutes=int(CRON_JOB_DURATION))
        scheduler.add_job(
            update_telegram_groups,
            trigger=DateTrigger(run_date=next_run_time),
            id=scheduler_job_id,
            replace_existing=True
        )

        logger.info(f"Completed scheduled update at {datetime.now(timezone.utc)}")
        ongoing_operations[operation_id]["status"] = "completed"

    except Exception as e:
        logger.error(f"Error in update_telegram_groups: {str(e)}")
        import traceback
        traceback.print_exc()
        ongoing_operations[operation_id]["status"] = "failed"
        ongoing_operations[operation_id]["error"] = str(e)
        
        # Ensure we still reschedule even if there was an error
        try:
            next_run_time = datetime.now(timezone.utc) + timedelta(minutes=int(CRON_JOB_DURATION))
            scheduler.add_job(
                update_telegram_groups,
                trigger=DateTrigger(run_date=next_run_time),
                id=scheduler_job_id,
                replace_existing=True
            )
            logger.info(f"Rescheduled job despite error")
        except Exception as sched_err:
            logger.error(f"Failed to reschedule job: {str(sched_err)}")
    
    finally:
        # Mark as not running so future scheduled tasks can proceed
        async with telegram_operations_lock:
            telegram_operations_running = False


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    try:
        # Start Telegram client
        await telegram_client.start()
        print("Telegram client started.")
        print("Listener initialized at app startup.")

        # Start the scheduler with a delayed initial run of the job
        # This gives time for the API to fully initialize
        next_run_time = datetime.now(timezone.utc) + timedelta(seconds=30)
        scheduler.add_job(
            update_telegram_groups,
            trigger=DateTrigger(run_date=next_run_time),
            id=scheduler_job_id
        )
        
        # Set up scheduled email alerts for daily and weekly digests
        await setup_scheduled_email_alerts(scheduler)

        # Add health check job
        scheduler.add_job(
            health_check_task,
            trigger=IntervalTrigger(minutes=5),  # Run every 5 minutes
            id="health_check_job"
        )

        scheduler.start()
        print("Scheduler started.")

        yield

    finally:
        print("Shutting down services...")
        scheduler.shutdown()
        await telegram_client.disconnect()
        print("All services shut down successfully.")


# Health check task
async def health_check_task():
    """Periodic health check to ensure the application is functioning correctly"""
    try:
        # Check database connectivity
        result = await groups_collection.find_one({}, {"_id": 1})
        db_status = "ok" if result is not None else "error"
        
        # Check if Telegram client is connected
        telegram_status = "ok" if telegram_client.is_connected() else "disconnected"
        
        # Check ongoing operations for any that are stalled
        current_time = datetime.now(timezone.utc)
        stalled_operations = []
        for op_id, op_info in ongoing_operations.items():
            if op_info["status"] == "running":
                duration = (current_time - op_info["start_time"]).total_seconds() / 60
                if duration > 60:  # If operation is running for more than 60 minutes
                    stalled_operations.append(op_id)
                    
        health_data = {
            "timestamp": current_time.isoformat(),
            "database": db_status,
            "telegram_client": telegram_status,
            "ongoing_operations": len(ongoing_operations),
            "stalled_operations": len(stalled_operations)
        }
        
        # Log health status
        if db_status == "ok" and telegram_status == "ok" and len(stalled_operations) == 0:
            logger.info(f"Health check passed: {health_data}")
        else:
            logger.warning(f"Health check issues detected: {health_data}")
            
        # Clean up old completed operations
        cleanup_time = current_time - timedelta(hours=24)
        to_remove = []
        for op_id, op_info in ongoing_operations.items():
            if op_info["status"] in ["completed", "failed"] and op_info["start_time"] < cleanup_time:
                to_remove.append(op_id)
        
        for op_id in to_remove:
            del ongoing_operations[op_id]
            
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")


# Create FastAPI app with lifespan context manager
app = FastAPI(lifespan=app_lifespan)
ALLOWED_ORIGINS = [CLIENT_URL]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "Accept", "Origin", "X-Requested-With"]
)


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    path = request.url.path
    method = request.method
    
    # Skip logging for certain paths to reduce noise
    if not any(x in path for x in ['/health', '/metrics', '.js', '.css']):
        logger.info(f"Request started: {method} {path}")
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    status_code = response.status_code
    
    # Log detailed info for slow requests or errors
    if process_time > 1 or status_code >= 400:
        logger.info(f"Request completed: {method} {path} took {process_time:.2f}s with status {status_code}")
    
    return response


# Prioritize regular API calls middleware
@app.middleware("http")
async def prioritize_api_calls(request: Request, call_next):
    """Give priority to regular API calls over Telegram operations"""
    if "trigger-update" not in request.url.path and "socket.io" not in request.url.path:
        # Regular API calls get higher priority (use the api_semaphore)
        async with api_semaphore:
            response = await call_next(request)
            return response
    else:
        # Lower priority for Telegram-related operations
        response = await call_next(request)
        return response


# Include routers for API endpoints
app.include_router(auth.router)
app.include_router(messages.router)
app.include_router(groups.router)
app.include_router(categories.router)
app.include_router(alerts.router)
app.include_router(threats.router)

# Create Socket.IO server
sio = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins=ALLOWED_ORIGINS,
    logger=False,
    engineio_logger=False
)

# Create a separate socket app
socket_app = socketio.ASGIApp(sio)

# Mount the socket app at a specific path instead of root
app.mount("/socket.io", socket_app)


@sio.on('connect')
async def connect(sid, environ, auth=None):
    """
    Handle client connection with optional authentication.
    
    Args:
        sid: Session ID assigned by Socket.IO
        environ: WSGI environment dictionary
        auth: Optional authentication data sent by client
    """
    try:
        # Log the connection
        logger.info(f"Client connected: {sid}")
        
        # If you want to implement authentication, check the auth parameter
        if auth and 'token' in auth:
            # You can verify the token here
            token = auth['token']
            # For example, call your authentication function
            # is_valid = await verify_token(token)
            # if not is_valid:
            #     return False  # Reject the connection
            
            logger.info(f"Client authenticated: {sid}")
        
        return True  # Accept the connection
        
    except Exception as e:
        logger.error(f"Socket connection error: {e}")
        return False  # Reject on error


@sio.on('disconnect')
async def disconnect(sid):
    logger.info(f"Client disconnected: {sid}")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": "An unexpected error occurred."}
    )


@app.post("/trigger-update")
async def trigger_update(background_tasks: BackgroundTasks):
    """Manually trigger the update process for all groups (non-blocking)."""
    try:
        # Check if already running
        async with telegram_operations_lock:
            if telegram_operations_running:
                return JSONResponse(
                    status_code=409,
                    content={"message": "Update operation already in progress"}
                )
        
        # Run as background task to avoid blocking the API response
        background_tasks.add_task(update_telegram_groups)
        return {"message": "Update process triggered successfully in background"}
    except Exception as e:
        logger.error(f"Error triggering update: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"message": f"Error triggering update: {str(e)}"}
        )


@app.get("/operations")
async def get_operations_status():
    """Get status of ongoing Telegram operations."""
    return {
        "operations_running": telegram_operations_running,
        "total_operations": len(ongoing_operations),
        "active_operations": sum(1 for op in ongoing_operations.values() if op["status"] == "running"),
        "recent_operations": [
            {
                "id": op_id,
                "description": op_info["description"],
                "status": op_info["status"],
                "start_time": op_info["start_time"].isoformat(),
                "duration_minutes": (datetime.now(timezone.utc) - op_info["start_time"]).total_seconds() / 60,
                "error": op_info.get("error", None)
            }
            for op_id, op_info in ongoing_operations.items()
            if (datetime.now(timezone.utc) - op_info["start_time"]).total_seconds() / 3600 < 24  # Last 24 hours
        ]
    }


@app.get("/health")
async def health_check():
    """Health check endpoint to verify API is responsive."""
    return {
        "status": "ok",
        "telegram_operations_running": telegram_operations_running,
        "time": datetime.now(timezone.utc).isoformat(),
        "api_semaphore": {
            "value": api_semaphore._value,
            "waiting": len(api_semaphore._waiters) if hasattr(api_semaphore, "_waiters") else "unknown"
        }
    }


@app.get("/")
async def root():
    return {"message": "Welcome to Telegram Monitor API"}