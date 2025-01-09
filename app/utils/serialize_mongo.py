from bson import ObjectId


def serialize_mongo_document(doc):
    """Convert MongoDB document to a JSON-serializable format."""
    if not doc:
        return None
    return {
        key: (str(value) if isinstance(value, ObjectId) else value)
        for key, value in doc.items()
    }
