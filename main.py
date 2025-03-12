from fastapi import FastAPI
from fastapi_pagination import add_pagination
from routers.router import router
from utilities.generic_utils import get_models
from database.database import  engines

app = FastAPI(title="Inventory Summary")

app.include_router(router, prefix="/api")

# Create tables for each database
for business, engine in engines.items():
    models = get_models(business)
    models.Base.metadata.create_all(bind=engine)

# Set default pagination limit to 1000
add_pagination(app)
