from app.database import groups_collection, messages_collection
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.telegram_client import telegram_client
from app.services.threat_service import process_message_for_alerts, setup_scheduled_email_alerts
from contextlib import asynccontextmanager
from app.routers import messages, groups, categories, auth, alerts, threats
from app.services.telegram_listener import update_listener
from app.utils.gpt_generations import generate_tags, save_tags
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta, timezone
from apscheduler.triggers.date import DateTrigger
import boto3
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from mimetypes import guess_extension
from app.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, CLIENT_URL, CRON_JOB_DURATION
import logging
import os
import asyncio
import socketio

# Set up logging
logging.basicConfig(level=logging.INFO)
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

# Semaphore to limit parallel uploads
semaphore = asyncio.Semaphore(10)

# Keep track of the scheduler and its job
scheduler_job_id = "update_telegram_groups_job"


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


async def download_media_with_extension(message, retries=5, backoff_factor=2, max_size_mb=210):
    """
    Download media from Telegram with retry logic, dynamically determining the file extension.
    Skip files larger than a specified size (in MB).
    """
    extension = get_extension_from_message(message)
    if not extension:
        logger.warning(
            f"Unable to determine file extension for message ID {message.id}. Skipping...")
        return None

    # Check file size before downloading
    if isinstance(message.media, MessageMediaDocument) and hasattr(message.media.document, "size"):
        file_size_mb = message.media.document.size / \
            (1024 * 1024)  # Convert to MB
        if file_size_mb > max_size_mb:
            logger.warning(
                f"Skipping download for message ID {message.id}: File size {file_size_mb:.2f} MB exceeds limit.")
            return None

    file_name = f"{message.id}{extension}"
    file_path = os.path.join(temp_media_dir, file_name)

    attempt = 0
    while attempt < retries:
        try:
            await telegram_client.download_media(message, file_path)
            if os.path.exists(file_path):
                return {"file_path": file_path, "file_name": file_name}
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
    Perform a multipart upload to S3 for files under 210 MB.
    """
    try:
        file_size = os.path.getsize(file_path)
        max_file_size = 210 * 1024 * 1024  # 210 MB limit

        if file_size > max_file_size:
            logger.warning(
                f"File {file_path} exceeds the 210 MB size limit. Skipping upload.")
            return None

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
        async with semaphore:
            bucket_name = list(buckets.keys())[bucket_index % len(buckets)]
            region = buckets[bucket_name]
            s3_key = f"{group_id}/{downloaded_file['file_name']}"

            # Perform multipart upload to S3
            s3_url = await multipart_upload_to_s3(
                downloaded_file['file_path'], s3_key, bucket_name, region
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


async def update_telegram_groups():
    """Background task to update messages from all groups and broadcast new ones via Socket.IO."""
    try:
        current_time = datetime.now(timezone.utc)
        logger.info(f"Starting scheduled update at {current_time}")

        groups = await groups_collection.find({"is_active": True}).to_list(None)
        semaphore = asyncio.Semaphore(2)  # Process only 2 groups at a time
        new_messages = []  # Store new messages to broadcast

        async def process_group(group, bucket_index):
            """Process each group and collect new messages."""
            async with semaphore:
                try:
                    entity = await telegram_client.get_entity(group["username"])
                    if not entity:
                        logger.warning(f"Group {group['username']} is invalid")
                        await groups_collection.update_one(
                            {"_id": group["_id"]},
                            {"$set": {"is_active": False}}
                        )
                        return
                except Exception as e:
                    logger.error(
                        f"Error verifying group {group['username']}: {str(e)}")
                    return

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

                # Get messages from Telegram
                async for message in telegram_client.iter_messages(
                    entity,
                    reverse=True,  # Start from oldest to newest
                    offset_date=min_date  # This will start from messages after min_date
                ):
                    message_date = message.date.replace(
                        tzinfo=timezone.utc) if message.date.tzinfo is None else message.date

                    # Skip if message is older than our latest stored message
                    if min_date and message_date <= min_date:
                        logger.debug(
                            f"Skipping message {message.id} as it's older than or equal to our latest stored message")
                        continue

                    logger.info(
                        f"Processing new message {message.id} from {group['username']} "
                        f"recorded at {message_date}, content: {message.text}"
                    )

                    tags = None

                    if message.media:
                        media_url = await process_message_media(message, group["group_id"], bucket_index)
                        bucket_index += 1  # Increment bucket index for distributing uploads
                    else:
                        media_url = None

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

                    # Upsert the message
                    await messages_collection.update_one(
                        {
                            "message_id": message_doc["message_id"],
                            "group_id": message_doc["group_id"]
                        },
                        {"$set": message_doc},
                        upsert=True
                    )

                    new_messages.append({
                        "id": message.id,
                        "channel": group["username"],
                        "timestamp": message_date.isoformat(),
                        "content": message.text or "No content",
                        "media": media_url if media_url else None,
                        "tags": tags if tags else []
                    })

                    # Process this message for alerts
                    await process_message_for_alerts(message_doc, group)

                logger.info(f"Successfully updated group: {group['username']}")
                await asyncio.sleep(60)  # Prevent Telegram rate-limiting

        # Process all groups concurrently, but only 2 at a time
        tasks = [process_group(group, idx) for idx, group in enumerate(groups)]
        await asyncio.gather(*tasks)

        # Broadcast new messages via Socket.IO if available
        if new_messages:
            logger.info(
                f"Broadcasting {len(new_messages)} new messages via Socket.IO...")
            await sio.emit('new_messages', {
                "type": "new_messages",
                "data": new_messages
            })

        # Reschedule the next cron job in 20 minutes
        next_run_time = datetime.now(
            timezone.utc) + timedelta(minutes=int(CRON_JOB_DURATION))
        scheduler.add_job(
            update_telegram_groups,
            trigger=DateTrigger(run_date=next_run_time),
            id=scheduler_job_id,
            replace_existing=True
        )

        logger.info(
            f"Completed scheduled update at {datetime.now(timezone.utc)}")

    except Exception as e:
        logger.error(f"Error in update_telegram_groups: {str(e)}")


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    try:
        await telegram_client.start()
        print("Telegram client started.")
        await update_listener()
        print("Listener initialized at app startup.")

        # Start the scheduler with an initial run of the job
        next_run_time = datetime.now(timezone.utc) + timedelta(seconds=10)
        scheduler.add_job(update_telegram_groups, trigger=DateTrigger(
            run_date=next_run_time), id=scheduler_job_id)
        
        # Set up scheduled email alerts for daily and weekly digests
        await setup_scheduled_email_alerts(scheduler)

        scheduler.start()
        print("Scheduler started.")

        yield

    finally:
        print("Shutting down services...")
        scheduler.shutdown()
        await telegram_client.disconnect()
        print("All services shut down successfully.")


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
    cors_allowed_origins=ALLOWED_ORIGINS,  # Use the same CORS origins as FastAPI
    logger=False,
    engineio_logger=False
)

# Create a separate socket app
socket_app = socketio.ASGIApp(sio)

# Mount the socket app at a specific path instead of root
app.mount("/socket.io", socket_app)


@sio.on('connect')
async def connect(sid, environ):
    print(f"Client connected: {sid}")


@sio.on('disconnect')
async def disconnect(sid):
    print(f"Client disconnected: {sid}")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": "An unexpected error occurred."}
    )


@app.post("/trigger-update")
async def trigger_update():
    """Manually trigger the update process for all groups."""
    try:
        await update_telegram_groups()
        return {"message": "Update process triggered successfully"}
    except Exception as e:
        logger.error(f"Error triggering update: {str(e)}")
        return {"message": f"Error triggering update: {str(e)}"}, 500


@app.get("/")
async def root():
    return {"message": "Welcome to Telegram Monitor API"}
