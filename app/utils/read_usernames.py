import re


def extract_usernames_from_file(file_path: str):
    """
    Extract Telegram usernames from a text file containing links.
    :param file_path: Path to the text file with Telegram links.
    :return: List of usernames.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        # Regular expression to match Telegram links
        link_pattern = re.compile(r"https://t\.me/([\w\d_]+)")

        # Find all matches in the file
        matches = link_pattern.findall(content)

        # Return a list of unique usernames
        return list(set(matches))  # Remove duplicates, if any
    except Exception as e:
        print(f"Error reading or processing the file: {e}")
        return []
