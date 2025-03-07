from database.database import get_db



def get_dynamic_db(username: str):
    if username is None:
        raise ValueError("Username is required")
    return next(get_db(username))