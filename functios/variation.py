import pandas as pd

def create_variations_dataframe(df, colu):
        df[colu] = df[colu].fillna("None")
        grouped = df.groupby(['Item_Name', 'Item_Type', colu])
        
        result_rows = []
        for (item_name, item_type, product_type), group in grouped:
            remaining_cols = [col for col in df.columns if col not in ['Item_Name', 'Item_Type', colu, 'Item_Id']]
            varied_cols = [col for col in remaining_cols if len(group[col].unique()) > 1]
            
            if varied_cols:
                all_variation_cols = varied_cols + ['Item_Id']
                unique_combinations = group[all_variation_cols].drop_duplicates()
                
                for _, combo in unique_combinations.iterrows():
                    variation_values = [f"{col}:{combo[col]}" for col in varied_cols 
                                        if combo[col] != 'None' and pd.notna(combo[col])]
                    variation_str = ", ".join(variation_values)
                    
                    result_rows.append({
                        'Item_Id': combo['Item_Id'],
                        'Item_Name': item_name,
                        'Item_Type': item_type,
                        colu: product_type,
                        'Variations': variation_str
                    })
            else:
                for item_id in group['Item_Id'].unique():
                    result_rows.append({
                        'Item_Id': item_id,
                        'Item_Name': item_name,
                        'Item_Type': item_type,
                        colu: product_type,
                        'Variations': "No variations"
                    })
        
        return pd.DataFrame(result_rows)