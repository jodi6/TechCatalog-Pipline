import pandas as pd
import json
import os

def profile_and_export_data(json_file_path, csv_output_path):
    file_name = os.path.basename(json_file_path)
    print(f"=== Starting Data Discovery & Profiling for: {file_name} ===")
    
    if not os.path.exists(json_file_path):
        print(f" - [ERROR] File not found: {json_file_path}\n" + "=" * 60 + "\n")
        return
        
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    products = [item for item in data if isinstance(item, dict)]
    df = pd.json_normalize(products)
    
    if df.empty:
        print(" - [FLAG] The file is completely empty. No data to profile or export.\n" + "=" * 60 + "\n")
        return
    
    # 2. Profiling summary table
    # استخدام astype(str) لحل مشكلة القوائم (unhashable list)
    profiling_table = pd.DataFrame({
        'dtype': df.dtypes,
        'null_count': df.isnull().sum(),
        'percent_null': round((df.isnull().sum() / len(df)) * 100, 2),
        'unique_count': df.astype(str).nunique(), 
        'sample_value': df.apply(lambda x: x.dropna().iloc[0] if not x.dropna().empty else None)
    })
    
    print("\n[1] Profiling Summary Table:")
    try:
        display(profiling_table) 
    except NameError:
        print(profiling_table)
    
    # 3. Issues list
    print("\n[2] Issues List:")
    # تحويل البيانات لنصوص أولاً لتجنب انهيار الكود عند فحص التكرار
    duplicates_count = df.astype(str).duplicated().sum()
    if duplicates_count > 0:
        print(f" - [FLAG] Found {duplicates_count} duplicate rows.")
    else:
        print(" - [OK] No duplicate rows found.")
        
    cols_with_nulls = profiling_table[profiling_table['null_count'] > 0].index.tolist()
    if cols_with_nulls:
        print(f" - [FLAG] The following columns have missing values and require cleaning:\n   {cols_with_nulls}")
    else:
        print(" - [OK] No missing values found.")
        
    # 4. Export to CSV
    os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)
    df.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
    print(f"\n[3] Data successfully flattened and saved to: {csv_output_path}\n" + "=" * 60 + "\n")


# قائمة الملفات بأسماء صحيحة ومسارات متوافقة مع الهيكل الجديد
files_to_process = [
    ('data/raw/KSA_Extra_Products.json', 'data/interim/KSA_Extra_Products.csv'),
    ('data/raw/US_Extra_Products.json', 'data/interim/US_Extra_Products.csv'),
    ('data/raw/KSA_Google_Devices.json', 'data/interim/KSA_Google_Devices.csv'),
    ('data/raw/US_Google_Devices.json', 'data/interim/US_Google_Devices.csv')
]

for json_in, csv_out in files_to_process:
    profile_and_export_data(json_in, csv_out)