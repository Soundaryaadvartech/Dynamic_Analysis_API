from fastapi import FastAPI
from routers.router import router
from database.database import Base, engines

app = FastAPI(title="Inventory Summary")

app.include_router(router, prefix="/api")
# Create tables for each database
for engine in engines.values():
    Base.metadata.create_all(bind=engine)