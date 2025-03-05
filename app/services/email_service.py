import requests
import logging
from app.config import RESEND_API_KEY, RESEND_SENDING_EMAIL

logger = logging.getLogger(__name__)

def send_email(to_email, subject, html_content, from_email=None):
    """
    Send an email using the Resend API.
    
    Args:
        to_email: The recipient's email address
        subject: Email subject line
        html_content: HTML content of the email
        from_email: Optional sender email (defaults to config value)
        
    Returns:
        Response from the Resend API
    """
    if not RESEND_API_KEY:
        logger.error("RESEND_API_KEY is not set")
        raise ValueError("RESEND_API_KEY is not set in the environment")
    
    sender = from_email or RESEND_SENDING_EMAIL
    if not sender:
        logger.error("Sender email is not provided and RESEND_SENDING_EMAIL is not set in config")
        raise ValueError("Sender email is required")
    
    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "from": sender,
        "to": to_email,
        "subject": subject,
        "html": html_content,
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        logger.info(f"Email sent successfully to {to_email}")
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send email: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")
        raise