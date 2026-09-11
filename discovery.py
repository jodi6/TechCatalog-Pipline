import pandas as pd
import json

def discover_and_convert(json_file_path, csv_output_path):
    print(f"=== Data Discovery: {json_file_path} ===")
    
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        
    products = [item for item in data if isinstance(item, dict)]
    df = pd.json_normalize(products)
    
    if df.empty:
        print("[FLAG] The file is empty. No data to convert.")
        print("-" * 40)
        return
        
    print("\nColumns:")
    print(df.columns.tolist())
    
    print("\nData Types:")
    print(df.dtypes)
    
    df.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
    print(f"\n[SUCCESS] File converted and saved as: {csv_output_path}")
    print("-" * 40)

discover_and_convert('KSA_Extra_Products.json', 'KSA_Extra_Products.csv')
discover_and_convert('US_Extra_Products.json', 'US_Extra_Products.csv')