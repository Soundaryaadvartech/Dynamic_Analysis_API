import traceback
import json
from typing import Optional
from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from sqlalchemy import distinct
from fastapi.responses import JSONResponse
from database.database import get_db
from utilities.utils import generate_inventory_summary
from utilities.generic_utils import get_dynamic_db, get_models
from database.models import Item

router = APIRouter()

@router.get("/inventory_summary")
def inventory_summary(username: str, days: Optional[int] = None, days_to_predict: Optional[int] = None, db:Session = Depends(get_dynamic_db)):
    try:
        days = days or 60
        days_to_predict = days_to_predict or 30
        models = get_models(username)
        summary_df = generate_inventory_summary(db, models, days, days_to_predict)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content = json.loads(summary_df.to_json(orient="records"))
        )
    
    except Exception:
        traceback.print_exc()
        return  JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content = {"message":"Something went wrong"}
        )

