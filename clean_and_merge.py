import pandas as pd

def clean_products_file(csv_path, output_path):
    df = pd.read_csv(csv_path)

    print(f"--- {csv_path} ---")
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
    df["has_discount"] = df["product_original_price"].notna()
    df["product_original_price"] = df["product_original_price"].fillna(df["product_price"])

    df = df.drop_duplicates(subset=["asin"])
    df = df.dropna(subset=["asin"])

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Saved: {output_path}")
    print(f"Products with missing price: {df['price_missing'].sum()}")
    print(f"Products with a discount: {df['has_discount'].sum()}")
    print()
    return df

ksa_files = ["KSA_Extra_Products.csv"]
us_files = ["US_Extra_Products.csv"]

ksa_cleaned = [clean_products_file(f, f"cleaned_{f}") for f in ksa_files]
final_ksa = pd.concat(ksa_cleaned, ignore_index=True)
print(f"KSA rows before dedup: {len(final_ksa)}")
final_ksa = final_ksa.drop_duplicates(subset=["asin"])
print(f"KSA rows after dedup: {len(final_ksa)}")
final_ksa.to_csv("final_KSA_products.csv", index=False, encoding="utf-8-sig")
print(f"Final KSA file saved with {len(final_ksa)} products total")

us_cleaned = [clean_products_file(f, f"cleaned_{f}") for f in us_files]
final_us = pd.concat(us_cleaned, ignore_index=True)
print(f"US rows before dedup: {len(final_us)}")
final_us = final_us.drop_duplicates(subset=["asin"])
print(f"US rows after dedup: {len(final_us)}")
final_us.to_csv("final_US_products.csv", index=False, encoding="utf-8-sig")
print(f"Final US file saved with {len(final_us)} products total")