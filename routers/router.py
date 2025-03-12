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
    "zing": ["Item_Name", "Item_Type", "Category","Is_Public", "Colour", "__Batch", "Fabric", "Fit", 
                    "Neck", "Occasion", "Print", "Size", "Sleeve", "Mood", "__Details", "__Print_Type", 
                    "__Quadrant", "__Style_Type"],
    
    "prathiksham": ["Item_Name", "Item_Type", "Category", "Colour", "__Batch", "Is_Public","Fabric", "Fit", "Lining", 
             "Neck", "Occasion", "Print", "Product_Availability", "Size", "Sleeve", "Pack","Bottom_Length", 
             "Bottom_Print", "Bottom_Type", "Collections", "Details", "Pocket", "Top_Length", "Waist_Band"] ,
    
    "beelittle" : [
        "Item_Name", "Item_Type", "Is_Public","Age", "Bottom","Bundles","Gender","Pack_Size","Pattern","Sale","Size",
        "Sleeve","Style","Top","Weight","Width","__Season","Colour", "Fabric", "Pattern", "Product_Type", 
        "Weave_Type", "Print_Colour", "Print_Size", "Print_Theme", "Print_Style", "Print Key Motif",
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

        # Convert DataFrame to JSON
        json_data = json.loads(summary_df.to_json(orient="records"))

        # Fixed Pagination (Send Only First 1000 Rows)
        FIXED_LIMIT = 1000
        paginated_data = json_data[:FIXED_LIMIT]

        return {
            "total_records": len(json_data),
            "returned_records": len(paginated_data),
            "data": paginated_data
        }
    
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

