import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from pydantic import BaseModel
from datetime import datetime, timezone
from app.database import groups_collection, messages_collection
from app.telegram_client import telegram_client
from app.services.telegram_listener import update_listener
from app.utils.serialize_mongo import serialize_mongo_document
from app.middlewares.auth_middleware import isAuthenticated
from telethon.tl.types import Channel, MessageMediaPhoto, MessageMediaDocument
from mimetypes import guess_extension
from telethon.errors import FloodWaitError
from app.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_BUCKET_NAME
import asyncio
import re
import os

router = APIRouter()
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
bucket_name = AWS_BUCKET_NAME

# Ensure the temp_media directory exists
temp_media_dir = "temp_media"
os.makedirs(temp_media_dir, exist_ok=True)

# Semaphore to limit parallel uploads
semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent uploads


async def upload_to_s3_with_bucket_and_region(file_path: str, s3_key: str, bucket_name: str, region: str) -> str:
    """
    Upload a file to a specific S3 bucket in the specified region and return the S3 URL.
    :param file_path: Local file path of the file to upload.
    :param s3_key: The key (path) to save the file in the S3 bucket.
    :param bucket_name: The name of the S3 bucket.
    :param region: The AWS region of the S3 bucket.
    :return: The URL of the uploaded file or None if upload fails.
    """
    try:
        # Create an S3 client for the specific region
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=region,
        )

        # Upload the file
        await asyncio.to_thread(s3_client.upload_file, file_path, bucket_name, s3_key)

        # Generate the S3 URL for the uploaded file
        file_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"
        return file_url

    except NoCredentialsError:
        print(f"AWS credentials not found for file: {file_path}")
        return None

    except ClientError as e:
        print(
            f"ClientError during upload to S3 bucket '{bucket_name}': {str(e)}")
        return None

    except Exception as e:
        print(f"Failed to upload to S3 bucket '{bucket_name}': {str(e)}")
        return None


async def upload_to_s3_with_retry(file_path: str, s3_key: str, retries=5, backoff_factor=2) -> str:
    """
    Upload a file to AWS S3 with retry logic.
    """
    attempt = 0
    while attempt < retries:
        try:
            s3_client.upload_file(file_path, bucket_name, s3_key)
            file_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
            return file_url
        except NoCredentialsError:
            raise HTTPException(
                status_code=500, detail="AWS credentials not found")
        except Exception as e:
            attempt += 1
            if attempt >= retries:
                raise HTTPException(
                    status_code=500, detail=f"Failed to upload to S3 after {retries} attempts: {str(e)}"
                )
            # Exponential backoff
            await asyncio.sleep(backoff_factor ** attempt)


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
        # Fallback to attributes for a file name
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
    # Determine the file extension
    extension = get_extension_from_message(message)
    if not extension:
        print(
            f"Unable to determine file extension for message ID {message.id}. Skipping...")
        return None

    # Check the file size (if available) and skip if larger than the max_size_mb
    if isinstance(message.media, MessageMediaDocument) and hasattr(message.media.document, "size"):
        file_size = message.media.document.size  # Size is in bytes
        file_size_mb = file_size / (1024 * 1024)  # Convert to MB
        if file_size_mb > max_size_mb:
            print(
                f"Skipping download for message ID {message.id}: File size {file_size_mb:.2f} MB exceeds limit of {max_size_mb} MB."
            )
            return None

    # Prepare the file name and path
    file_name = f"{message.id}{extension}"
    file_path = os.path.join(temp_media_dir, file_name)

    # Retry logic for downloading the media
    attempt = 0
    while attempt < retries:
        try:
            await telegram_client.download_media(message, file_path)
            if os.path.exists(file_path):
                return {
                    "file_path": file_path,
                    "file_name": file_name
                }
            else:
                print(f"File not found after download: {file_path}")
                return None
        except Exception as e:
            attempt += 1
            print(
                f"Error downloading media (attempt {attempt}/{retries}): {e}")
            await asyncio.sleep(backoff_factor ** attempt)

    print(f"Failed to download media after {retries} attempts: {file_path}")
    return None


async def multipart_upload_to_s3(file_path: str, s3_key: str, bucket_name: str, region: str) -> str:
    """
    Perform a multipart upload to S3 for files under 20 MB and return the S3 URL.
    Files larger than 20 MB will not be uploaded and handled gracefully.
    :param file_path: Path of the local file to upload.
    :param s3_key: Key (path) under which the file will be stored in S3.
    :param bucket_name: Target S3 bucket name.
    :param region: AWS region for the target bucket.
    :return: URL of the uploaded file, or None if the file size exceeds the limit or upload fails.
    """
    try:
        # Check file size
        file_size = os.path.getsize(file_path)
        max_file_size = 210 * 1024 * 1024  # 210 MB limit

        if file_size > max_file_size:
            print(
                f"File {file_path} exceeds the 210 MB size limit. Skipping upload.")
            return None

        # Create an S3 client for the specified region
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=region,
        )

        # Chunk size of 1 MB
        chunk_size = 5 * 1024 * 1024

        # Initiate the multipart upload
        response = s3_client.create_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key
        )
        upload_id = response["UploadId"]

        parts = []  # Track the parts uploaded

        # Upload file in chunks
        with open(file_path, "rb") as file:
            part_number = 1
            while chunk := file.read(chunk_size):
                # Upload each chunk
                part_response = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )

                # Save the ETag for completing the multipart upload
                parts.append({
                    "PartNumber": part_number,
                    "ETag": part_response["ETag"]
                })

                print(f"Uploaded part {part_number} for {file_path}")
                part_number += 1

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )

        # Generate and return the S3 URL
        file_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"
        return file_url

    except NoCredentialsError:
        print(f"AWS credentials not found for file: {file_path}")
        return None

    except ClientError as e:
        print(f"ClientError during multipart upload to S3: {str(e)}")
        return None

    except Exception as e:
        print(
            f"Failed to perform multipart upload for file {file_path}: {str(e)}")
        return None


async def upload_to_s3(file_path: str, s3_key: str) -> str:
    """
    Upload a file to AWS S3 and return the file URL.
    :param file_path: Local file path
    :param s3_key: S3 key (filename in the bucket)
    :return: S3 URL of the uploaded file
    """
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        file_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return file_url
    except NoCredentialsError:
        raise HTTPException(
            status_code=500, detail="AWS credentials not found")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to upload to S3: {str(e)}")


class Group(BaseModel):
    username: str


async def get_group_with_messages(username: str):
    """Fetch group details and associated messages."""
    try:
        # Fetch group details
        group = await groups_collection.find_one({"username": username.lstrip("@")})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        # Serialize the group document
        group = serialize_mongo_document(group)

        # Fetch associated messages
        messages = await messages_collection.find({"group_id": group.get("group_id")}).to_list(length=100)
        messages = [serialize_mongo_document(msg) for msg in messages]

        return {"group": group, "messages": messages}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups")
async def get_groups():
    """
    Fetch all groups from the database and verify their validity on Telegram.
    Only return groups that are active and recognized by Telegram.
    """
    try:
        groups = await groups_collection.find().to_list(length=100)
        valid_groups = []

        for group in groups:
            try:
                # Verify group on Telegram
                entity = await telegram_client.get_entity(group["username"])
                if entity:
                    valid_groups.append(serialize_mongo_document(group))
            except Exception:
                # Skip invalid or banned groups
                continue

        await update_listener()  # Refresh the listener with updated groups
        return {"groups": valid_groups}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups")
async def add_group(group: dict):
    """
    Add a group to the database, scrape historical messages with media,
    upload media to multiple S3 buckets in different regions, and store the media URLs in the database.
    """
    try:
        # Define S3 buckets and their regions
        buckets = {
            "telescopebucket0212": "eu-north-1",
            "telescopebucket101": "ap-south-1",
        }

        # Validate the group on Telegram
        try:
            entity = await telegram_client.get_entity(group["username"].lstrip("@"))

            if not isinstance(entity, Channel):
                raise HTTPException(
                    status_code=403, detail="Entity is not a channel or supergroup"
                )

            if entity.username is None:
                if not hasattr(entity, "usernames") or not entity.usernames:
                    raise HTTPException(
                        status_code=403,
                        detail="Cannot track private groups or channels (no usernames found)",
                    )
                usernames_list = [
                    username_obj.username for username_obj in entity.usernames
                ]
                if group["username"].lstrip("@") not in usernames_list:
                    raise HTTPException(
                        status_code=403,
                        detail=f"The username '{group['username']}' does not exist in the group aliases: {usernames_list}",
                    )

        except Exception as e:
            raise HTTPException(
                status_code=404, detail=f"Failed to validate group: {str(e)}"
            )

        # Add or update group in the database
        group_data = {
            "username": group["username"].lstrip("@"),
            "group_id": entity.id,
            "title": entity.title,
            "member_count": entity.participants_count
            if hasattr(entity, "participants_count")
            else None,
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
        }

        await groups_collection.update_one(
            {"username": group_data["username"]},
            {"$set": group_data},
            upsert=True,
        )

        # Ensure the temp_media directory exists
        temp_media_dir = "temp_media"
        os.makedirs(temp_media_dir, exist_ok=True)

        # Semaphore to limit parallel uploads
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent uploads

        async def handle_message(message, bucket_index):
            """
            Handle a single message: download media, upload to S3, and save to the database.
            """
            message_doc = {
                "message_id": message.id,
                "group_id": entity.id,
                "text": message.text or "",
                "date": message.date,
                "sender_id": message.sender_id,
                "message_time": message.date,  # Store the actual Telegram message time
                "media": None,
                "created_at": datetime.now(timezone.utc),
            }

            if message.media:

                # Download media with retry
                downloaded_file = await download_media_with_extension(message)
                if downloaded_file:
                    async with semaphore:  # Limit concurrent uploads
                        # Get the current bucket and region for this batch
                        bucket_name = list(buckets.keys())[
                            bucket_index % len(buckets)]
                        region = buckets[bucket_name]
                        s3_key = f"{entity.id}/{downloaded_file['file_name']}"

                        # Upload to the current bucket and region
                        # Inside the handle_message function
                        s3_url = await multipart_upload_to_s3(downloaded_file['file_path'], s3_key, bucket_name, region)

                        if s3_url:
                            print(
                                f"Media uploaded to S3 bucket {bucket_name}: {s3_url}")
                            message_doc["media"] = s3_url

                        # Remove the file after uploading
                        os.remove(downloaded_file["file_path"])

            # Save the message to the database
            await messages_collection.update_one(
                {"message_id": message_doc["message_id"],
                 "group_id": message_doc["group_id"]},
                {"$set": message_doc},
                upsert=True,
            )

        # Scrape historical messages and process them
        tasks = []
        bucket_index = 0  # Start with the first bucket

        async for message in telegram_client.iter_messages(entity):
            tasks.append(handle_message(message, bucket_index))
            bucket_index += 1  # Increment bucket index for the next batch

            if len(tasks) >= 50:  # Batch size
                await asyncio.gather(*tasks)
                tasks = []
                await asyncio.sleep(10)  # Delay between batches

        # Finalize remaining tasks
        if tasks:
            await asyncio.gather(*tasks)

        # Add the group to the listener
        await update_listener()

        return {
            "message": "Group added successfully, historical messages scraped, and listener updated.",
            "group": group_data,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/groups/{username}")
async def delete_group(username: str):
    """
    Delete a group and all its associated messages from the database.
    No interaction with the Telegram API.
    """
    try:
        # Remove '@' from the username if present
        clean_username = username.lstrip("@")

        # Find the group in the database
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(
                status_code=404, detail="Group not found in the database"
            )

        # Get the group_id to delete messages
        group_id = group.get("group_id")

        # Delete all messages associated with the group
        delete_messages_result = await messages_collection.delete_many(
            {"group_id": group_id}
        )
        print(
            f"Deleted {delete_messages_result.deleted_count} messages associated with group '{clean_username}'."
        )

        # Delete the group itself
        delete_group_result = await groups_collection.delete_one(
            {"username": clean_username}
        )
        if delete_group_result.deleted_count == 0:
            raise HTTPException(
                status_code=404, detail="Failed to delete the group"
            )

        return {
            "message": "Group and all associated messages deleted successfully.",
            "deleted_group": serialize_mongo_document(group),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/bulk_add_from_file")
async def bulk_add_from_file(file: UploadFile = File(...)):
    """
    Extract Telegram links from an uploaded file, validate them, and add them to the database.
    Processes in batches of 20 groups with a 60-second delay between batches to avoid rate limits.
    """
    added_groups = []
    skipped_groups = []
    invalid_links = []

    try:
        # Read the file content
        content = (await file.read()).decode("utf-8")

        # Regular expression to match Telegram links
        link_pattern = re.compile(r"https://t\.me/([\w\d_]+)")
        matches = link_pattern.findall(content)

        if not matches:
            raise HTTPException(
                status_code=400, detail="No valid Telegram links found in the file."
            )

        # Remove duplicates and process usernames in batches of 20
        usernames = list(set(matches))
        batch_size = 20  # Number of groups per batch

        for i in range(0, len(usernames), batch_size):
            batch = usernames[i:i + batch_size]
            for username in batch:
                try:
                    # Check if the group already exists in the database
                    existing_group = await groups_collection.find_one(
                        {"username": username}
                    )
                    if existing_group:
                        skipped_groups.append(username)
                        continue

                    # Validate the group on Telegram
                    try:
                        entity = await telegram_client.get_entity(username)
                        if not isinstance(entity, Channel):
                            skipped_groups.append(username)
                            continue
                        if entity.username is None:  # Private group check
                            skipped_groups.append(username)
                            continue

                        # Add the group to the database
                        group_data = {
                            "username": username,
                            "group_id": entity.id,
                            "title": entity.title,
                            "member_count": entity.participants_count
                            if hasattr(entity, "participants_count")
                            else None,
                            "is_active": True,
                            "created_at": datetime.now(timezone.utc),
                        }
                        await groups_collection.update_one(
                            {"username": group_data["username"]},
                            {"$set": group_data},
                            upsert=True,
                        )
                        added_groups.append(username)

                    except FloodWaitError as e:
                        wait_time = e.seconds
                        print(
                            f"Rate limited while processing '{username}', waiting for {wait_time} seconds..."
                        )
                        await asyncio.sleep(wait_time)
                        # Retry this username after the wait
                        usernames.append(username)
                    except Exception:
                        invalid_links.append(f"https://t.me/{username}")

                except Exception as e:
                    print(f"Unexpected error for '{username}': {e}")
                    invalid_links.append(f"https://t.me/{username}")

            # Delay between batches to avoid rate limits
            if i + batch_size < len(usernames):
                print("Sleeping for 60 seconds to avoid rate limits...")
                await asyncio.sleep(60)

        # Update the listener with all valid groups
        await update_listener()

        return {
            "message": "Bulk group addition completed.",
            "added_groups": added_groups,
            "skipped_groups": skipped_groups,
            "invalid_links": invalid_links,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing file: {str(e)}"
        )


@router.get("/groups/{username}")
async def get_group(username: str):
    """
    Fetch a group and its associated messages by username.
    Only display the messages already added to the database.
    """
    try:
        clean_username = username.lstrip("@")

        # Fetch group details from the database
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(
                status_code=404, detail="Group not found in the database"
            )

        # Verify group validity on Telegram
        try:
            entity = await telegram_client.get_entity(clean_username)
            if not entity:
                raise HTTPException(
                    status_code=404, detail="Group is invalid or banned on Telegram"
                )
        except Exception:
            raise HTTPException(
                status_code=404, detail="Group is invalid or banned on Telegram"
            )

        # Serialize the group document
        group = serialize_mongo_document(group)

        # Fetch messages from the database
        messages_in_db = await messages_collection.find(
            {"group_id": group.get("group_id")}
        ).to_list(length=100)
        serialized_messages = [serialize_mongo_document(
            msg) for msg in messages_in_db]

        return {
            "group": group,
            "messages": serialized_messages,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/groups/{username}/deactivate")
async def deactivate_group(username: str):
    """Deactivate (soft delete) a group."""
    try:
        clean_username = username.lstrip("@")
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        # Deactivate the group
        result = await groups_collection.update_one({"username": clean_username}, {"$set": {"is_active": False}})
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404, detail="Failed to deactivate the group")

        await update_listener()  # Refresh the listener
        response = await get_group_with_messages(username)
        return {
            "message": "Group deactivated successfully, listener updated.",
            "deactivated_group": response["group"],
            "messages": response["messages"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/groups/{username}/reactivate")
async def reactivate_group(username: str):
    """Reactivate a previously deactivated group."""
    try:
        clean_username = username.lstrip("@")
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        # Reactivate the group
        result = await groups_collection.update_one({"username": clean_username}, {"$set": {"is_active": True}})
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404, detail="Failed to reactivate the group")

        await update_listener()  # Refresh the listener
        response = await get_group_with_messages(username)
        return {
            "message": "Group reactivated successfully, listener updated.",
            "reactivated_group": response["group"],
            "messages": response["messages"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/{username}/scrape_historical_data")
async def scrape_historical_data(username: str, limit: int = 100, isAuthenticated: bool = Depends(isAuthenticated)):
    """
    Scrape historical messages from a group and store them in the database.
    :param username: The username of the group.
    :param limit: The maximum number of messages to scrape (default: 100).
    """
    try:
        if not isAuthenticated:
            raise HTTPException(status_code=401, detail="Unauthorized")

        clean_username = username.lstrip("@")

        # Fetch group details from the database
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(
                status_code=404, detail="Group not found in the database"
            )

        # Validate group on Telegram
        try:
            entity = await telegram_client.get_entity(clean_username)
            if not entity:
                raise HTTPException(
                    status_code=404, detail="Group is invalid or banned on Telegram"
                )
        except Exception as e:
            raise HTTPException(
                status_code=404, detail=f"Failed to validate group on Telegram: {str(e)}"
            )

        # Fetch historical messages from Telegram
        message_count = 0
        async for message in telegram_client.iter_messages(entity, limit=limit):
            try:
                message_doc = {
                    "message_id": message.id,
                    "group_id": group["group_id"],
                    "group_username": clean_username,
                    "text": message.text or "",
                    "date": message.date,
                    "sender_id": message.sender_id,
                    "media": message.media is not None,
                    "created_at": datetime.now(timezone.utc),
                }

                # Save the message to the database
                result = await messages_collection.update_one(
                    {
                        "message_id": message_doc["message_id"],
                        "group_id": message_doc["group_id"],
                    },
                    {"$set": message_doc},
                    upsert=True,
                )

                if result.upserted_id or result.modified_count > 0:
                    message_count += 1

            except Exception as e:
                print(f"Error saving message {message.id}: {e}")

        return {
            "message": f"Scraped {message_count} messages from the group '{username}'.",
            "group": serialize_mongo_document(group),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
