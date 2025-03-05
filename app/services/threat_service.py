from datetime import datetime, timezone, timedelta
from app.database import messages_collection, alert_settings_collection, threat_library_collection
from app.services.email_service import send_email
from apscheduler.triggers.cron import CronTrigger
import logging
import re

logger = logging.getLogger(__name__)

async def process_message_for_alerts(message_data, group_info):
    """
    Process a message to check if it matches any alert settings and needs to be stored as a threat.
    
    Args:
        message_data: The message data containing text and metadata
        group_info: Information about the group/channel the message is from
    """
    try:
        print("Processing message for alerts")
        # Get all active alert settings from all users
        alert_settings_cursor = alert_settings_collection.find({})
        alert_settings_list = await alert_settings_cursor.to_list(length=None)
        
        if not alert_settings_list:
            logger.info("No alert settings found, skipping alert processing")
            return
        
        # Extract message information
        message_id = message_data.get("message_id")
        message_text = message_data.get("text", "").lower()  # Convert to lowercase for case-insensitive matching
        channel_name = group_info.get("title", "").lower()  # Convert to lowercase for case-insensitive matching
        timestamp = message_data.get("created_at")
        media_url = message_data.get("media")
        tags = message_data.get("tags", [])

        
        # Process each user's alert settings
        for alert_setting in alert_settings_list:
            user_id = alert_setting.get("user_id")
            keyword = alert_setting.get("keyword", "").lower()  # Convert to lowercase for case-insensitive matching
            alert_types = alert_setting.get("alert_types", [])
            frequency = alert_setting.get("frequency", "immediate")
            
            # Check if message contains the keyword or channel matches the keyword
            is_keyword_match = False
            if keyword:
                # Check for exact match or word boundary match
                if (keyword in message_text or 
                    keyword in channel_name or
                    re.search(r'\b' + re.escape(keyword) + r'\b', message_text, re.IGNORECASE) or
                    re.search(r'\b' + re.escape(keyword) + r'\b', channel_name, re.IGNORECASE)):
                    is_keyword_match = True
            
            # If we have a match, save as a threat and process alert
            if is_keyword_match:
                # Create a threat document
                threat_doc = {
                    "user_id": user_id,
                    "message_id": message_id,
                    "group_id": group_info.get("group_id"),
                    "group_name": group_info.get("username"),
                    "text": message_data.get("text", ""),  # Store original text with case preserved
                    "matched_keyword": keyword,
                    "alert_types": alert_types,
                    "date_detected": datetime.now(timezone.utc),
                    "message_date": timestamp,
                    "media_url": media_url,
                    "tags": tags,
                    "is_notified": frequency == "immediate",  # Mark as notified if immediate notification
                }
                
                # Store in the threat library
                result = await threat_library_collection.insert_one(threat_doc)
                threat_id = result.inserted_id
                
                logger.info(f"Threat detected and saved with ID: {threat_id} for user: {user_id}")
                
                # Send immediate notification if configured
                if frequency == "immediate":
                    await send_threat_notification_email(user_id, [threat_doc])
    
    except Exception as e:
        logger.error(f"Error processing message for alerts: {str(e)}")


async def send_threat_notification_email(user_id, threats):
    """
    Send an email notification about detected threats to a user.
    
    Args:
        user_id: The user ID to send the notification to
        threats: List of threat documents to include in the notification
    """
    from app.database import users_collection
    
    try:
        # Get user details
        user = await users_collection.find_one({"_id": user_id})
        if not user or not user.get("email"):
            logger.error(f"User {user_id} not found or has no email")
            return
        
        user_email = user.get("email")
        
        # Prepare email content
        threat_count = len(threats)
        subject = f"Telescope Alert: {threat_count} new threat{'s' if threat_count > 1 else ''} detected"
        
        # Create HTML content for the email
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; }}
                .header {{ background-color: #121229; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .threat {{ margin-bottom: 20px; padding: 15px; border-left: 4px solid #ff3232; background-color: #f9f9f9; }}
                .footer {{ margin-top: 30px; text-align: center; font-size: 12px; color: #888; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Telescope Threat Alert</h1>
                </div>
                <div class="content">
                    <p>Hello,</p>
                    <p>Telescope has detected {threat_count} new threat{'s' if threat_count > 1 else ''} matching your alert settings:</p>
                    
                    {''.join([f'''
                    <div class="threat">
                        <p><strong>Channel:</strong> {threat.get('group_name')}</p>
                        <p><strong>Detected:</strong> {threat.get('date_detected').strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
                        <p><strong>Matched Keyword:</strong> {threat.get('matched_keyword')}</p>
                        <p><strong>Message:</strong> {threat.get('text')}</p>
                        {'<p><strong>Media:</strong> <a href="' + threat.get('media_url') + '">View Media</a></p>' if threat.get('media_url') else ''}
                    </div>
                    ''' for threat in threats])}
                    
                    <p>You can view more details in your Telescope dashboard.</p>
                </div>
                <div class="footer">
                    <p>This is an automated message from Telescope. Please do not reply to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Send the email
        send_email(user_email, subject, html_content)
        logger.info(f"Threat notification email sent to {user_email}")
        
        # Mark threats as notified
        for threat in threats:
            if '_id' in threat:
                await threat_library_collection.update_one(
                    {"_id": threat['_id']},
                    {"$set": {"is_notified": True}}
                )
    
    except Exception as e:
        logger.error(f"Error sending threat notification email: {str(e)}")


async def setup_scheduled_email_alerts(scheduler):
    """
    Set up scheduled tasks for sending daily and weekly digest emails.
    
    Args:
        scheduler: The APScheduler instance to use for scheduling
    """
    # Schedule daily digest at 9:00 AM
    scheduler.add_job(
        send_daily_digest_emails,
        trigger=CronTrigger(hour=9, minute=0),
        id="daily_digest_emails",
        replace_existing=True
    )
    
    # Schedule weekly digest on Monday at 9:00 AM
    scheduler.add_job(
        send_weekly_digest_emails,
        trigger=CronTrigger(day_of_week="mon", hour=9, minute=0),
        id="weekly_digest_emails",
        replace_existing=True
    )
    
    logger.info("Scheduled email alert tasks set up")


async def send_daily_digest_emails():
    """Send daily digest emails to users who have chosen daily frequency."""
    try:
        # Find users with daily digest frequency
        daily_settings = await alert_settings_collection.find({"frequency": "daily"}).to_list(None)
        
        for setting in daily_settings:
            user_id = setting.get("user_id")
            
            # Find threats that haven't been notified yet
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            threats = await threat_library_collection.find({
                "user_id": user_id,
                "is_notified": False,
                "date_detected": {"$gte": yesterday}
            }).to_list(None)
            
            if threats:
                # Send notification email with all threats
                await send_threat_notification_email(user_id, threats)
            else:
                # Optionally send a "no threats" email
                await send_no_threats_email(user_id, "daily")
    
    except Exception as e:
        logger.error(f"Error sending daily digest emails: {str(e)}")


async def send_weekly_digest_emails():
    """Send weekly digest emails to users who have chosen weekly frequency."""
    try:
        # Find users with weekly digest frequency
        weekly_settings = await alert_settings_collection.find({"frequency": "weekly"}).to_list(None)
        
        for setting in weekly_settings:
            user_id = setting.get("user_id")
            
            # Find threats that haven't been notified yet
            last_week = datetime.now(timezone.utc) - timedelta(days=7)
            threats = await threat_library_collection.find({
                "user_id": user_id,
                "is_notified": False,
                "date_detected": {"$gte": last_week}
            }).to_list(None)
            
            if threats:
                # Send notification email with all threats
                await send_threat_notification_email(user_id, threats)
            else:
                # Optionally send a "no threats" email
                await send_no_threats_email(user_id, "weekly")
    
    except Exception as e:
        logger.error(f"Error sending weekly digest emails: {str(e)}")


async def send_no_threats_email(user_id, frequency):
    """
    Send an email notification when no threats were detected in the period.
    
    Args:
        user_id: The user ID to send the notification to
        frequency: The frequency setting ('daily' or 'weekly')
    """
    from app.database import users_collection
    
    try:
        # Get user details
        user = await users_collection.find_one({"_id": user_id})
        if not user or not user.get("email"):
            return
        
        user_email = user.get("email")
        period = "day" if frequency == "daily" else "week"
        
        # Prepare email content
        subject = f"Telescope Alert: No threats detected this {period}"
        
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; }}
                .header {{ background-color: #121229; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .footer {{ margin-top: 30px; text-align: center; font-size: 12px; color: #888; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Telescope Threat Alert</h1>
                </div>
                <div class="content">
                    <p>Hello,</p>
                    <p>Good news! Telescope has not detected any threats matching your alert settings during this {period}.</p>
                    <p>We'll continue monitoring and will notify you if any threats are detected.</p>
                </div>
                <div class="footer">
                    <p>This is an automated message from Telescope. Please do not reply to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Send the email
        send_email(user_email, subject, html_content)
        logger.info(f"No threats email sent to {user_email}")
    
    except Exception as e:
        logger.error(f"Error sending no-threats email: {str(e)}")


# Modify the update_telegram_groups function in your main.py
# Add these lines where you process new messages

async def process_new_message_for_alerts(message, group):
    """
    Process a new message for alerts - to be called from the update_telegram_groups function.
    
    Args:
        message: The message data dictionary from Telegram
        group: The group/channel information
    """
    # Prepare message data for alert processing
    message_data = {
        "id": message.id,
        "channel": group["username"],
        "timestamp": message.date.replace(tzinfo=timezone.utc),
        "content": message.text or "No content",
        "media": message.get("media_url"),
        "tags": message.get("tags", [])
    }
    
    # Process the message for alerts
    await process_message_for_alerts(message_data, group)