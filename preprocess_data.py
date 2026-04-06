import pandas as pd
import numpy as np
import os

def preprocess():
    datasets = [
        {'file': 'product_price.csv', 'cols': {'State': 'city', 'Product': 'item', 'Category': 'category', '1st Quality': 'price_1', '2nd Quality': 'price_2', '3rd Quality': 'price_3'}},
        {'file': 'india_gi_products_price_tiers.xlsx', 'cols': {'State': 'city', 'Product': 'item', 'Category': 'category', '1st Quality Price': 'price_1', '2nd Quality Price': 'price_2', '3rd Quality Price': 'price_3'}},
        {'file': 'product_price_3.xlsx', 'cols': {'State': 'city', 'Product': 'item', 'Category': 'category', '1st Quality Price': 'price_1', '2nd Quality Price': 'price_2', '3rd Quality Price': 'price_3'}},
        {'file': 'product_price_2.xlsx', 'cols': {'State': 'city', 'Product': 'item', 'Category': 'category', '1st Quality Price': 'price_1', '2nd Quality Price': 'price_2', '3rd Quality Price': 'price_3'}} 
    ]

    unified_data = []

    for ds in datasets:
        file_path = ds['file']
        if not os.path.exists(file_path):
            print(f"Skipping {file_path}, not found.")
            continue
        
        print(f"Processing {file_path}...")
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        # Rename columns to standardized names
        df = df.rename(columns=ds['cols'])
        
        # Keep only the columns we need
        needed_cols = ['city', 'item', 'category', 'price_1', 'price_2', 'price_3']
        # Check if all needed columns exist
        existing_cols = [c for c in needed_cols if c in df.columns]
        df = df[existing_cols]
        
        # Add source file
        df['source_file'] = file_path
        df['country'] = 'India'
        
        # Melt to handle different price tiers
        melted = df.melt(id_vars=['country', 'city', 'item', 'category', 'source_file'], 
                         value_vars=[c for c in ['price_1', 'price_2', 'price_3'] if c in df.columns],
                         var_name='tier', value_name='price')
        
        unified_data.append(melted)

    full_df = pd.concat(unified_data, ignore_index=True)

    # Data Cleaning
    full_df = full_df.dropna(subset=['price'])
    full_df['price'] = pd.to_numeric(full_df['price'], errors='coerce')
    full_df = full_df.dropna(subset=['price'])
    
    # Standardize names
    full_df['city'] = full_df['city'].str.strip().str.title()
    full_df['item'] = full_df['item'].str.strip().str.title()
    full_df['category'] = full_df['category'].str.strip().str.title()
    
    # Handle duplicates
    full_df = full_df.drop_duplicates()
    
    # Outlier detection (Z-score > 3 per item)
    def remove_outliers(group):
        if len(group) < 3:
            return group
        z_scores = np.abs((group['price'] - group['price'].mean()) / group['price'].std())
        return group[z_scores <= 3]

    full_df = full_df.groupby(['city', 'item'], group_keys=False).apply(remove_outliers)

    # Save to CSV for easy import
    full_df.to_csv('unified_travel_data.csv', index=False)
    print("Preprocessed data saved to unified_travel_data.csv")
    print(f"Total records: {len(full_df)}")

if __name__ == "__main__":
    preprocess()
