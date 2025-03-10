# Process period data function
import pandas as pd
import numpy as np
from functios.variation import create_variations_dataframe

def process_period_data(t1,t2,t3,t4,t5,temp_t2,t3_total,dt,colu,days,period_df, period_name,group_by,grp):
    # Merge with alltime quantity data
    join_cols = 'Item_Id' if group_by.lower() == "item_id" else grp
    df_final = pd.merge(period_df, temp_t2, how="left", on=join_cols)
    df_final["Alltime_Total_Quantity"] = df_final["Alltime_Total_Quantity"].fillna(0)
    
    # Calculate derived columns using vectorized operations
    df_final["Total_Stock"] = df_final["Alltime_Total_Quantity"] + df_final["Current_Stock"]
    df_final["Stock_Sold_Percentage"] = (df_final["Quantity"] / df_final["Total_Stock"] * 100).round(2).fillna(0)
    
    # Calculate days since launch
    df_final['__Launch_Date'] = pd.to_datetime(df_final['__Launch_Date'], errors='coerce')
    df_final['days_since_launch'] = (pd.Timestamp.today() - df_final['__Launch_Date']).dt.days
    
    # Merge all-time data
    df_final = pd.merge(df_final, t3_total, how="left", on=join_cols)
    df_final["Alltime_Items_Addedtocart"] = df_final["Alltime_Items_Addedtocart"].fillna(0)
    df_final["Alltime_Items_Viewed"] = df_final["Alltime_Items_Viewed"].fillna(0)
    
    # Merge last sold date efficiently
    if group_by.lower() == "item_id":
        df_final = pd.merge(df_final, t5, how="left", on="Item_Id")
    else:
        t5_with_groups = pd.merge(t5, t1[['Item_Id'] + grp].drop_duplicates(), on='Item_Id', how='left')
        t5_grouped = t5_with_groups.groupby(grp).agg({"Last_Sold_Date": "max"}).reset_index()
        df_final = pd.merge(df_final, t5_grouped, how="left", on=grp)
    
    # Vectorized calculation for days sold out
    df_final['Days_Sold_Out_Past'] = np.where(
        df_final['Current_Stock'] == 0,
        (df_final['Last_Sold_Date'] - df_final['__Launch_Date']).dt.days,0)
    df_final['Days_Sold_Out_Past'] =df_final['Days_Sold_Out_Past'].fillna(0)
    
    # Calculate period-specific metrics using vectorized operations
    df_final["Period_Perday_Quantity"] = df_final["Quantity"] / df_final["Period_Days"]
    df_final["Period_Perday_View"] = df_final["Items_Viewed"] / df_final["Period_Days"]
    df_final["Period_Perday_ATC"] = df_final["Items_Addedtocart"] / df_final["Period_Days"]
    
    # All-time per-day metrics using numpy for vectorized operations
    df_final["Alltime_Perday_Quantity"] = np.where(
        df_final["Current_Stock"] == 0,
        df_final["Alltime_Total_Quantity"] / df_final["Days_Sold_Out_Past"],
        df_final["Alltime_Total_Quantity"] / df_final["days_since_launch"]
    ).round(2)
    df_final["Alltime_Perday_Quantity"] = df_final["Alltime_Perday_Quantity"].fillna(0)
    
    # Calculate sale price after discount
    sale_price_after_discount = (df_final["Sale_Price"] * (100 - df_final["Sale_Discount"]) / 100)
    
    # Calculate stock values (vectorized)
    df_final["Alltime_Total_Quantity_Value"] = df_final["Alltime_Total_Quantity"] * sale_price_after_discount
    df_final["Current_Stock_Value"] = df_final["Current_Stock"] * sale_price_after_discount
    df_final["Total_Stock_Value"] = df_final["Total_Stock"] * sale_price_after_discount
    df_final['Sale_Price_After_Discount'] = sale_price_after_discount
    
    # Rename for clarity
    df_final = df_final.rename(columns={"Quantity": "Quantity_Sold", "Total_Value": "Sold_Quantity_Value"})
    
    # Calculate remaining metrics using vectorized operations
    df_final["Alltime_Perday_View"] = (df_final["Alltime_Items_Viewed"] / df_final["days_since_launch"]).round(2).fillna(0)
    df_final["Alltime_Perday_ATC"] = (df_final["Alltime_Items_Addedtocart"] / df_final["days_since_launch"]).round(2).fillna(0)
    df_final["Total_Stock_Sold_Percentage"] = (df_final["Alltime_Total_Quantity"] / df_final["Total_Stock"] * 100).round(2).fillna(0)
    
    # Avoid division by zero for projected days calculation
    with np.errstate(divide='ignore', invalid='ignore'):
        df_final["Projected_Days_to_Sellout"] = np.where(
            df_final["Alltime_Perday_Quantity"] > 0,
            df_final["Current_Stock"] / df_final["Alltime_Perday_Quantity"],
            np.inf
        )
    
    # Period-specific prediction
    column_name = f"Predicted_Quantity_Next_{days}Days_Based_on_{period_name}"
    df_final[column_name] = np.where(
        df_final["Current_Stock"] != 0,
        df_final["Period_Perday_Quantity"] * days,
        0
    )
    
    # Add period identifier
    df_final["Period"] = period_name
    
    # Select columns with period-specific prefix for later renaming
    period_specific_columns = {
        "Quantity_Sold": f"{period_name}_Quantity_Sold",
        "Sold_Quantity_Value": f"{period_name}_Sold_Quantity_Value",
        "Items_Viewed": f"{period_name}_Items_Viewed",
        "Items_Addedtocart": f"{period_name}_Items_Addedtocart",
        "Period_Perday_Quantity": f"{period_name}_Perday_Quantity",
        "Period_Perday_View": f"{period_name}_Perday_View",
        "Period_Perday_ATC": f"{period_name}_Perday_ATC",
        "Stock_Sold_Percentage": f"{period_name}_Stock_Sold_Percentage",
        column_name: column_name
    }
    
    # Add variations data if using Item_Id grouping
    if group_by.lower() == "item_id":
        try:
            var1 = create_variations_dataframe(dt, colu)
            df_final = pd.merge(df_final, var1[["Item_Id", "Variations"]], how="left", on="Item_Id")
        except Exception:
            df_final["Variations"] = ""
    else:
        df_final["Variations"] = ""
    
    # Rename period-specific columns
    df_final = df_final.rename(columns=period_specific_columns)
    
    return df_final