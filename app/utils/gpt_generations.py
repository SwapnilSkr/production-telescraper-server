from app.gpt_client import client
from app.database import tags_collection


async def fetch_existing_tags():
    """
    Fetch all existing tags from the tags_collection.
    """
    try:
        tags = await tags_collection.find().to_list(length=None)
        # Extract the 'tag' field from each document
        return [tag["tag"] for tag in tags]
    except Exception as e:
        print(f"Error fetching existing tags: {e}")
        return []


async def generate_tags(message_text):
    """
    Generate tags for the message using OpenAI's GPT-4 model.
    Incorporates existing tags from the database into the prompt and prioritizes them.
    """
    # Fetch existing tags from the database
    existing_tags = await fetch_existing_tags()

    # Construct the prompt with existing tags
    prompt = (
        "You are an intelligent AI trained to generate relevant tags for messages scraped from possible cybercrime groups. "
        "Given the following message, provide a list of up to 5 relevant tags that capture the main themes, "
        "keywords, and topics discussed. Prioritize using tags from the existing list if they are relevant: {existing_tags}. "
        "If none of the existing tags are suitable, create new ones. "
        "Tags should be concise, descriptive, lowercase, without hashes or special characters, and separated by commas. "
        "Provide only the tags without any additional text or explanation. "
        "Message: '{message_text}'"
    )

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": prompt.format(
                    message_text=message_text,
                    # Pass existing tags as a comma-separated string
                    existing_tags=", ".join(existing_tags)
                )}
            ]
        )

        # Assuming tags are comma-separated
        tags = response.choices[0].message.content.strip().split(",")
        return [tag.strip() for tag in tags]  # Clean up whitespace around tags
    except Exception as e:
        print(f"Error generating tags with GPT: {e}")
        return []


async def save_tags(tags):
    """
    Save unique tags to the tags collection, avoiding duplicates.
    """
    for tag in tags:
        # Check if the tag already exists in the database
        existing_tag = await tags_collection.find_one({"tag": tag})
        if not existing_tag:
            # If it doesn't exist, insert it into the collection
            await tags_collection.insert_one({"tag": tag})
            print(f"New tag saved: {tag}")
        else:
            print(f"Tag already exists: {tag}")
