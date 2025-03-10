from pydantic import BaseModel, EmailStr, field_validator
from disposable_mail_checker import is_disposable_mail
import re

# Common personal and temporary email domains to block
BLOCKED_EMAIL_DOMAINS = [
    # Personal email providers
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com", "icloud.com", "protonmail.com",
    "mail.com", "zoho.com", "yandex.com", "gmx.com", "live.com", "msn.com",
    
    # Temporary/disposable email providers
    "temp-mail.org", "tempmail.com", "guerrillamail.com", "mailinator.com", "10minutemail.com",
    "throwawaymail.com", "yopmail.com", "getnada.com", "dispostable.com", "sharklasers.com",
    "trashmail.com", "maildrop.cc", "tempr.email", "fakeinbox.com", "tempinbox.com", "burpcollaborator.net"
]

class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    
    @field_validator('email')
    def validate_work_email(cls, v):
        # Extract domain from email
        domain = v.split('@')[-1].lower()
        
        # Check if domain is in blocked list
        if domain in BLOCKED_EMAIL_DOMAINS:
            raise ValueError("Please use a work email. Personal and temporary emails are not allowed.")
        
        # Check if email is disposable
        is_disposable = is_disposable_mail(v)

        print(is_disposable)

        if is_disposable:
            raise ValueError("Please use a work email. Temporary emails are not allowed.")
        
        # Check for common patterns of disposable emails (additional check)
        if re.search(r'temp|fake|disposable|trash|throw|junk', domain):
            raise ValueError("Please use a work email. Temporary emails are not allowed.")
            
        return v


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserResponse(BaseModel):
    id: str
    username: str
    email: EmailStr
