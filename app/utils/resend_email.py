import re
import resend
from typing import Optional, Dict, Any
from app.config import RESEND_API_KEY, RESEND_SENDING_EMAIL

resend.api_key = RESEND_API_KEY


def is_valid_email(email: str) -> bool:
    """Check if the email address is valid using a regex."""
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return re.match(pattern, email) is not None


def send_email(to: str, subject: str, message: str) -> Optional[Dict[str, Any]]:
    """
    Send an email using the Resend API after validating the recipient's email.
    """
    try:
        # Sanitize and validate the email address
        to = to.strip()
        if not is_valid_email(to):
            print(f"Invalid email address provided: {to}")
            return None

        print(f"Sending email to: {to}")
        params: resend.Emails.SendParams = {
            "from": RESEND_SENDING_EMAIL,
            "to": [to],  # Ensure 'to' is a list with a valid email string
            "subject": subject,
            "html": f"<strong>{message}</strong>",
        }
        email = resend.Emails.send(params)
        return email
    except Exception as e:
        print(f"Error sending email: {e}")
        return None
