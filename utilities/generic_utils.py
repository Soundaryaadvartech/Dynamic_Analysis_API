import importlib
from database.database import get_db

# Dictionary mapping username to the correct models file
MODEL_FILES = {
    "zing": "models/zing_db.py",  
    "prathiksham": "models/pkm_db.py" 
}


def get_dynamic_db(username: str):
    if username is None:
        raise ValueError("Username is required")
    return next(get_db(username))


def get_models(username: str):
    """Dynamically import the correct models file."""
    if username not in MODEL_FILES:
        raise ValueError(f"Models for {username} not found")
    
    module_name = MODEL_FILES[username]
    
    try:
        models_module = importlib.import_module(module_name)
        return models_module
    except ModuleNotFoundError:
        raise ValueError(f"Models module {module_name} not found")