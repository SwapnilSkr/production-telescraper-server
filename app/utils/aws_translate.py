import boto3
import os
from botocore.exceptions import ClientError
from app.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# Initialize the translate client with explicit credentials
# AWS credentials should be set as environment variables
translate = boto3.client(
    service_name='translate',
    region_name='ap-south-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    use_ssl=True
)


def translate_to_english(text: str) -> str:
    """
    Translates given text to English. Source language is automatically detected.

    Args:
        text (str): The text to translate

    Returns:
        str: The translated English text or original text if translation fails
    """
    try:
        result = translate.translate_text(
            Text=text,
            SourceLanguageCode="auto",
            TargetLanguageCode="en"
        )
        return result.get('TranslatedText')
    except ClientError as e:
        print(f"AWS Translate error: {e}")
        return text  # Return original text if translation fails
