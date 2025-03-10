import traceback
import json
import asyncio
from pandas import DataFrame
from typing import Optional
from fastapi import APIRouter, Depends, status, HTTPException 
from sqlalchemy.orm import Session
from sqlalchemy import distinct
from fastapi.responses import JSONResponse
from database.database import get_db
from utilities.utils import generate_inventory_summary
from utilities.generic_utils import get_dynamic_db, get_models

router = APIRouter()

UNIQUE_COLUMN_MAPPINGS = {
    "zing": ["Item_Name", "Item_Type", "Category", "Colour", "__Batch", "Fabric", "Fit", 
                    "Neck", "Occasion", "Print", "Size", "Sleeve", "Mood"],
    
    "prathiksham": ["Item_Name", "Item_Type", "Category", "Colour", "__Batch", "Fabric", "Fit", "Lining", 
             "Neck", "Occasion", "Print", "Product_Availability", "Size", "Sleeve", "Bottom_Length", 
             "Bottom_Print", "Bottom_Type", "Collections", "Details", "Pocket", "Top_Length", "Waist_Band"] ,
    
    "beelittle" : [
        "Item_Name", "Item_Type", "Age", "Colour", "Fabric", "Pattern", "Product_Type", "Style", "Weave_Type", 
        "Print_Colour", "Print_Size", "Print_Theme", "Print_Style"
    ]
}

async def run_in_thread(fn, *args):
    """Run blocking function in separate thread to avoid blocking FastAPI"""
    return await asyncio.to_thread(fn, *args)

@router.get("/inventory_summary")
async def inventory_summary(business: str, days: Optional[int] = None, group_by: Optional[str] = None, db:Session = Depends(get_dynamic_db)):
    try:
        days = days or 60
        group_by = group_by or "Item_Id"
        models = get_models(business)
        summary_df: DataFrame = await run_in_thread(generate_inventory_summary, db, models, days, group_by, business)

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

@router.get("/unique_values")
def unique_values(business: str, db: Session = Depends(get_db)):
    try:
        models = get_models(business)  # Load the correct SQLAlchemy models
        Item = models.Item  # Access the Item model dynamically

        if business not in UNIQUE_COLUMN_MAPPINGS:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid business name")

        # Get the column names specific to the business
        selected_columns = UNIQUE_COLUMN_MAPPINGS[business]

        unique_values = {}
        
        for column_name in selected_columns:
            column_attr = getattr(Item, column_name, None)  # Dynamically get column attribute
            if column_attr is not None:
                unique_values[column_name] = [
                    row[0] for row in db.query(distinct(column_attr)).all() if row[0] is not None
                ]

        return JSONResponse(status_code=status.HTTP_200_OK, content=unique_values)
    
    except HTTPException as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=e.status_code,
            content = e.detail )

    except Exception:
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"message": "Something Went Wrong"}
        )

