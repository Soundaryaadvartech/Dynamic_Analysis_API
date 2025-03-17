import pandas as pd
import numpy as np
from sqlalchemy.orm import Session


def get_filter_data(db: Session, models, business: str):

    if business.lower() == "zing":
        item_query = db.query(models.Item.Item_Name, models.Item.Item_Type, models.Item.Category, models.Item.Colour, 
                            models.Item.Fabric, models.Item.Fit, models.Item.Neck, models.Item.Offer, models.Item.Print, 
                            models.Item.Size, models.Item.Sleeve)
        columns = ["Item_Name", "Item_Type", "Category", "Colour", "Fabric", 
                "Fit", "Neck", "Offer", "Print", "Size", "Sleeve"]  

    elif business.lower() == "beelittle":
        item_query = db.query(models.Item.Item_Name, models.Item.Item_Type, models.Item.Product_Type, models.Item.Age,models.Item.Bottom, 
                            models.Item.Bundles,models.Item.Colour, models.Item.Fabric, models.Item.Filling, models.Item.Gender, 
                            models.Item.Pack_Size, models.Item.Size, models.Item.Sleeve, models.Item.Style, models.Item.Top, models.Item.Weave_Type, 
                            models.Item.Width,models.Item.Weight)

        columns = ["Item_Name", "Item_Type", "Product_Type", "Age", "Bottom", "Bundles", "Colour", 
                "Fabric", "Filling", "Gender", "Pack_Size", "Size", "Sleeve", "Style", 
                "Top", "Weave_Type", "Width", "Weight"]
    elif business.lower() == "prathiksham":
        item_query = db.query(models.Item.Item_Name, models.Item.Item_Type, models.Item.Category, models.Item.Colour, models.Item.Fabric, 
                            models.Item.Fit, models.Item.Lining, models.Item.Neck, models.Item.Occasion, models.Item.Print, 
                            models.Item.Product_Availability, models.Item.Size, models.Item.Sleeve, models.Item.Pack)

        columns = ["Item_Name", "Item_Type", "Category", "Colour", "Fabric", "Fit", "Linning", 
                "Neck", "Occation", "Print", "Product_Availability", "Size", "Sleeve", "Pack"]
    else:
        raise ValueError("business name does not match")\
        
    items = item_query.all()
    filter_data = pd.DataFrame(items, columns=columns)

    return filter_data

