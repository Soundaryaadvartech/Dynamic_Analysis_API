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

router = APIRouter()

@router.get("/inventory_summary")
def inventory_summary(business: str, days: Optional[int] = None, group_by: Optional[str] = None, db:Session = Depends(get_dynamic_db)):
    try:
        days = days or 60
        group_by = group_by or "Item_Id"
        models = get_models(business)
        summary_df = generate_inventory_summary(db, models, days, group_by,business)

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

