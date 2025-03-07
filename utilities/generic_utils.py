import importlib
from database.database import get_db

# Dictionary mapping business name to the correct models file
MODEL_FILES = {
    "zing": "models.zing_db",  
    "prathiksham": "models.pkm_db" 
}


def get_dynamic_db(business: str):
    if business is None:
        raise ValueError("Business Name  is required")
    return next(get_db(business))


def get_models(business: str):
    """Dynamically import the correct models file."""
    if business not in MODEL_FILES:
        raise ValueError(f"Models for {business} not found")
    
    module_name = MODEL_FILES[business]
    
    try:
        models_module = importlib.import_module(module_name)
        print(models_module)
        return models_module
    except ModuleNotFoundError:
        raise ValueError(f"Models module {module_name} not found")