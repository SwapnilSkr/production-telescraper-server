import boto3

translate = boto3.client(service_name='translate',
                         region_name='ap-south-1',
                         use_ssl=True)


def translate_to_english(text: str) -> str:
    """
    Translates given text to English. Source language is automatically detected.

    Args:
        text (str): The text to translate

    Returns:
        str: The translated English text
    """
    result = translate.translate_text(
        Text=text,
        SourceLanguageCode="auto",
        TargetLanguageCode="en"
    )
    return result.get('TranslatedText')
