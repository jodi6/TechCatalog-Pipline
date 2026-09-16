import pandas as pd

def standardize_google_columns(df):
    rename_map = {
        "title": "product_title",
        "price": "product_price",
        "productId": "asin",
        "rating": "product_star_rating",
        "ratingCount": "product_num_ratings",
        "link": "product_url",
        "imageUrl": "product_photo",
    }
    df = df.rename(columns=rename_map)
    return df

def clean_products_file(csv_path, output_path):
    df = pd.read_csv(csv_path)

    if "Google_Devices" in csv_path:
        df = standardize_google_columns(df)

    print(f"--- {csv_path} ---")
    print(f"Columns: {list(df.columns)}")
    print(f"Total rows before filtering: {len(df)}")

    price_cols = ["product_price", "product_original_price", "product_minimum_offer_price"]
    for col in price_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("\u200f", "", regex=False)
                .str.replace("ريال", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.replace("USD", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    non_device_cols = ["book_format", "book_formats", "unit_price", "unit_count", "product_byline"]
    for col in non_device_cols:
        if col in df.columns:
            df = df[df[col].isnull()]

    print(f"Total rows after removing books/accessories: {len(df)}")

    df["price_missing"] = df["product_price"].isnull()

    if "product_original_price" in df.columns:
        df["has_discount"] = df["product_original_price"].notna()
        df["product_original_price"] = df["product_original_price"].fillna(df["product_price"])
    else:
        df["has_discount"] = False

    df = df.drop_duplicates(subset=["asin"])
    df = df.dropna(subset=["asin"])

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Saved: {output_path}")
    print(f"Products with missing price: {df['price_missing'].sum()}")
    print(f"Products with a discount: {df['has_discount'].sum()}")
    print()
    return df

ksa_files = ["Data/interim/KSA_Extra_Products.csv", "Data/interim/KSA_Google_Devices.csv"]
us_files = ["Data/interim/US_Extra_Products.csv", "Data/interim/US_Google_Devices.csv"]

ksa_cleaned = [clean_products_file(f, f"Data/processed/cleaned_{f.split('/')[-1]}") for f in ksa_files]
final_ksa = pd.concat(ksa_cleaned, ignore_index=True)
print(f"KSA rows before dedup: {len(final_ksa)}")
final_ksa = final_ksa.drop_duplicates(subset=["asin"])
print(f"KSA rows after dedup: {len(final_ksa)}")
final_ksa.to_csv("Data/processed/final_KSA_products.csv", index=False, encoding="utf-8-sig")
print(f"Final KSA file saved with {len(final_ksa)} products total")

us_cleaned = [clean_products_file(f, f"Data/processed/cleaned_{f.split('/')[-1]}") for f in us_files]
final_us = pd.concat(us_cleaned, ignore_index=True)
print(f"US rows before dedup: {len(final_us)}")
final_us = final_us.drop_duplicates(subset=["asin"])
print(f"US rows after dedup: {len(final_us)}")
final_us.to_csv("Data/processed/final_US_products.csv", index=False, encoding="utf-8-sig")
print(f"Final US file saved with {len(final_us)} products total")