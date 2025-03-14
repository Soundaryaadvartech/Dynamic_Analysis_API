import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func
from functios.period import process_period_data
from sqlalchemy import and_

def generate_inventory_summary(db: Session, models, days: int, group_by: str, business: str,filter_jason):
  
    # Validate and set grouping columns
    if group_by.lower() == "item_id":
        grp = ["Item_Id"]
    elif group_by.lower() == "item_name":
        if business.lower() in ["prathiksham", "zing"]:
            grp = ["Item_Name", "Item_Type", "Category"]
        elif business.lower() == "beelittle":
            grp = ["Item_Name", "Item_Type", "Product_Type"]
    else:
        raise ValueError("group_by must be either 'item_id' or 'item_name'")

    # Set category column based on business
    colu = "Product_Type" if business.lower() == "beelittle" else "Category"
    
    # Fetch only the necessary columns for items based on business type
    common_columns = ["Item_Id", "Item_Name", "Item_Type", "Item_Code", 
                      "Current_Stock", "launch_date", "Sale_Price", "Sale_Discount", "batch"]
    
    if business.lower() in ["prathiksham", "zing"]:
        item_query = db.query(models.Item.Item_Id, models.Item.Item_Name, models.Item.Item_Type, 
                             models.Item.Item_Code, models.Item.Category,
                             models.Item.Current_Stock, models.Item.launch_date, 
                             models.Item.Sale_Price, models.Item.Sale_Discount, models.Item.batch)
        columns = ["Item_Id", "Item_Name", "Item_Type", "Item_Code", "Category", 
                  "Current_Stock", "__Launch_Date", "Sale_Price", "Sale_Discount", "__Batch"]
    else:  # beelittle
        item_query = db.query(models.Item.Item_Id, models.Item.Item_Name, models.Item.Item_Type, 
                             models.Item.Item_Code, models.Item.Product_Type,models.Item.Age,models.Item.Discount, 
                             models.Item.Current_Stock, models.Item.launch_date, 
                             models.Item.Sale_Price, models.Item.Sale_Discount, models.Item.batch)
        columns = ["Item_Id", "Item_Name", "Item_Type", "Item_Code", "Product_Type","Age","Discount", 
                  "Current_Stock", "__Launch_Date", "Sale_Price", "Sale_Discount", "__Batch"]
    
    def apply_filters(query, filters):
        filter_conditions = []
        for column, values in filters.items():
            if hasattr(models.Item, column):
                filter_conditions.append(getattr(models.Item, column).in_(values))

        if filter_conditions:
            query = query.filter(and_(*filter_conditions))
    
        return query

    if filter_jason:
        item_query = apply_filters(item_query, filter_jason)

        
    # Execute all database queries upfront to minimize DB round trips
    items = item_query.all()
    t1 = pd.DataFrame(items, columns=columns)
    
    # Only fetch needed attributes for dt dataframe
    if business.lower() == "prathiksham":
        dt_attrs = ["Item_Id", "Item_Name", "Item_Type", "Category", 
                    "Colour", "Fabric", "Fit", "Lining", "Neck", "Size", "Sleeve", "Pack"]
    elif business.lower() == "zing":
        dt_attrs = ["Item_Id", "Item_Name", "Item_Type", "Category", 
                    "Colour", "Fabric", "Fit", "Neck", "Occasion", "Print", "Size", "Sleeve"]
    else:  # beelittle
        dt_attrs = ["Item_Id", "Item_Name", "Item_Type", "Product_Type","Age", "Bottom", "Bundles", "Fabric",
                   "Filling", "Gender", "Pack_Size", "Pattern", "Size", "Sleeve", "Style", 
                   "Top", "Weave_Type", "Weight", "Width"]
    
    # Fetch attributes for dt only if needed for variations
    if group_by.lower() == "item_id":
            dt_items = db.query(*[getattr(models.Item, attr) for attr in dt_attrs]).all()
            dt_values = [list(item) for item in dt_items]  # Convert each ORM object to a list of values
            dt = pd.DataFrame(dt_values, columns=dt_attrs)
    else:
        dt = ""
    # Batch all database queries together
    sales = db.query(models.Sale.Item_Id, models.Sale.Date, models.Sale.Quantity, models.Sale.Total_Value).all()
    viewsatc = db.query(models.ViewsAtc.Item_Id, models.ViewsAtc.Date, models.ViewsAtc.Items_Viewed, models.ViewsAtc.Items_Addedtocart).all()
    first_sold_dates = db.query(models.Sale.Item_Id, func.min(models.Sale.Date).label("First_Sold_Date")).group_by(models.Sale.Item_Id).all()
    last_sold_dates = db.query(models.Sale.Item_Id, func.max(models.Sale.Date).label("Last_Sold_Date")).group_by(models.Sale.Item_Id).all()
    
    # Convert to dataframes
    t2 = pd.DataFrame(sales, columns=["Item_Id", "Date", "Quantity", "Total_Value"])
    t3 = pd.DataFrame(viewsatc, columns=["Item_Id", "Date", "Items_Viewed", "Items_Addedtocart"])
    t4 = pd.DataFrame(first_sold_dates, columns=["Item_Id", "First_Sold_Date"])
    t5 = pd.DataFrame(last_sold_dates, columns=["Item_Id", "Last_Sold_Date"])
    
    # Preprocess data types in one batch to avoid redundant conversions
    t1["__Launch_Date"] = pd.to_datetime(t1["__Launch_Date"])
    t1["Item_Id"] = t1["Item_Id"].astype(int)
    t1["Sale_Price"] = t1["Sale_Price"].astype(int)
    t1["Current_Stock"] = t1["Current_Stock"].astype(int)
    t1["Sale_Discount"] = t1["Sale_Discount"].astype(float)
    t2["Date"] = pd.to_datetime(t2["Date"])
    t3["Date"] = pd.to_datetime(t3["Date"])
    t3["Item_Id"] = t3["Item_Id"].astype(int)
    t5["Item_Id"] = t5["Item_Id"].astype(int)
    t5["Last_Sold_Date"] = pd.to_datetime(t5["Last_Sold_Date"])
    
    # Merge first sold date
    t1 = pd.merge(t1, t4, how="left", on="Item_Id")
    t1["__Launch_Date"] = t1["__Launch_Date"].fillna(t1["First_Sold_Date"])
    
    # Pre-calculate all-time aggregations to avoid redundant calculations
    if group_by.lower() == "item_id":
        temp_t2 = t2.groupby("Item_Id").agg({"Quantity": "sum"}).rename(columns={"Quantity": "Alltime_Total_Quantity"}).reset_index()
        t3_total = t3.groupby("Item_Id").agg({
            "Items_Viewed": "sum",
            "Items_Addedtocart": "sum"
        }).rename(columns={
            "Items_Addedtocart": "Alltime_Items_Addedtocart",
            "Items_Viewed": "Alltime_Items_Viewed"
        }).reset_index()
    else:
        # For item_name grouping, add the grouping columns first
        t2_with_groups = pd.merge(t2, t1[['Item_Id'] + grp].drop_duplicates(), on='Item_Id', how='left')
        t3_with_groups = pd.merge(t3, t1[['Item_Id'] + grp].drop_duplicates(), on='Item_Id', how='left')
        
        temp_t2 = t2_with_groups.groupby(grp).agg({"Quantity": "sum"}).rename(columns={"Quantity": "Alltime_Total_Quantity"}).reset_index()
        t3_total = t3_with_groups.groupby(grp).agg({
            "Items_Viewed": "sum",
            "Items_Addedtocart": "sum"
        }).rename(columns={
            "Items_Addedtocart": "Alltime_Items_Addedtocart",
            "Items_Viewed": "Alltime_Items_Viewed"
        }).reset_index()
    
    # Create variations function optimized for performance
    
    
    # Optimized item summary function
    def get_item_summary(t1, t2, t3, start_offset, end_offset):
        # Calculate date range for each item
        t1['Start_Date'] = t1['__Launch_Date'] + pd.to_timedelta(start_offset, unit='D')
        t1['End_Date'] = t1['__Launch_Date'] + pd.to_timedelta(end_offset, unit='D')
        t1['Period_Days'] = end_offset - start_offset
        
        get_lst = grp + ['Start_Date', 'End_Date']
        
        # For item_name grouping, create group columns in t2 and t3 if not already done
        if group_by.lower() == "item_name" and 'Item_Name' not in t2.columns:
            # Add the grouping columns to t2 and t3 (if they haven't been added above)
            grp_mapping = t1[['Item_Id'] + grp].drop_duplicates()
            t2 = pd.merge(t2, grp_mapping, on='Item_Id', how='left')
            t3 = pd.merge(t3, grp_mapping, on='Item_Id', how='left')
        
        # Filter data based on date range
        join_cols = 'Item_Id' if group_by.lower() == "item_id" else grp
        
        # Use vectorized operations for filtering
        t2_merge = pd.merge(t2, t1[get_lst], on=join_cols, how='inner')
        t3_merge = pd.merge(t3, t1[get_lst], on=join_cols, how='inner')
        
        t2_filtered = t2_merge[(t2_merge['Date'] >= t2_merge['Start_Date']) & (t2_merge['Date'] <= t2_merge['End_Date'])]
        t3_filtered = t3_merge[(t3_merge['Date'] >= t3_merge['Start_Date']) & (t3_merge['Date'] <= t3_merge['End_Date'])]
        
        # Aggregate data by grouping columns - use optimized agg with dict
        agg_dict = {
            'Item_Id': 'first' if group_by.lower() == "item_name" else 'first',
            'Item_Code': 'first',
            'Current_Stock': 'sum',
            '__Launch_Date': 'min',
            'Period_Days': 'first',
            'Sale_Price': 'mean',
            'Sale_Discount': 'mean',
    
        }
        
        t1_agg = t1.groupby(grp, as_index=False).agg(agg_dict)
        t2_agg = t2_filtered.groupby(grp, as_index=False)[['Quantity', 'Total_Value']].sum()
        t3_agg = t3_filtered.groupby(grp, as_index=False)[['Items_Viewed', 'Items_Addedtocart']].sum()
        
        # Merge using one operation when possible
        period_df = pd.merge(t1_agg, t2_agg, on=join_cols, how='left')
        period_df = pd.merge(period_df, t3_agg, on=join_cols, how='left')
        
        # Fill NA values all at once
        period_df = period_df.fillna(0)
        
        return period_df
    
    
    
    # Generate summaries for both periods
    first_period_df = get_item_summary(t1, t2, t3, 0, days)
 
    second_period_df = get_item_summary(t1, t2, t3, days, 2 * days)

    # Process both periods
    first_period_results = process_period_data(t1,t2,t3,t4,t5,temp_t2,t3_total,dt,colu,days,first_period_df,"first_period",group_by,grp)
    
    
    second_period_results = process_period_data(t1,t2,t3,t4,t5,temp_t2,t3_total,dt,colu,days,second_period_df,"second_period",group_by,grp)
   
    
    # Ensure both dataframes have necessary columns for item_id grouping
    
    
    # Define common columns for the combined results
    common_cols = ["Item_Id",
        "Item_Name", "Item_Type", colu,"Variations","Sale_Discount", "__Launch_Date", 
        "days_since_launch", "Projected_Days_to_Sellout", "Days_Sold_Out_Past", 
        "Current_Stock", "Total_Stock", "Current_Stock_Value", "Total_Stock_Value", 
        "Sale_Price", "Sale_Price_After_Discount", "Alltime_Total_Quantity",
        "Alltime_Total_Quantity_Value", "Alltime_Perday_Quantity", "Alltime_Items_Viewed",
        "Alltime_Perday_View", "Alltime_Items_Addedtocart", "Alltime_Perday_ATC",
        "Total_Stock_Sold_Percentage" 
    ]
    

  
    
    # Get period-specific columns
    first_period_specific_cols = [col for col in first_period_results.columns 
                                 if col.startswith("first_period") or 
                                 (col.startswith("Predicted_Quantity_Next") and "first_period" in col)]
    
    second_period_specific_cols = [col for col in second_period_results.columns 
                                  if col.startswith("second_period") or 
                                  (col.startswith("Predicted_Quantity_Next") and "second_period" in col)]
    
    # Create combined results using a single merge operation when possible
    combined_results = first_period_results[common_cols].copy()
    
    # Add first period specific columns
    for col in first_period_specific_cols:
        combined_results[col] = first_period_results[col]
    
    # Add second period specific columns with a single merge
    join_cols = ['Item_Id'] if group_by.lower() == "item_id" else grp
    second_period_cols = join_cols + second_period_specific_cols
    combined_results = pd.merge(combined_results, second_period_results[second_period_cols], on=join_cols, how='left')
    combined_results = combined_results.loc[:, ~combined_results.columns.duplicated()]
    # Final formatting - do this in bulk
    # Round numeric columns
    numeric_cols = combined_results.select_dtypes(include=['number']).columns
    
    combined_results[numeric_cols] = combined_results[numeric_cols].round(2)
    
    # Format date columns if they exist
    if "__Launch_Date" in combined_results.columns and not combined_results["__Launch_Date"].empty:
        if pd.api.types.is_datetime64_any_dtype(combined_results["__Launch_Date"]):
            combined_results["__Launch_Date"] = combined_results["__Launch_Date"].dt.strftime('%Y-%m-%d')
    
    # Sort by primary grouping column
    
    combined_results = combined_results.sort_values(by="Item_Id").reset_index(drop=True)

    return combined_results