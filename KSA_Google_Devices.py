import json
import time
import requests

# 1. API Configurations and Credentials
url = "https://google-search-master-mega.p.rapidapi.com/shopping"

headers = {
    "x-rapidapi-key": "f9fa266b82msh98eb3a7ba4c2878p195ea2jsnf9accfedeac9",
    "x-rapidapi-host": "google-search-master-mega.p.rapidapi.com",
}

# 2. Categories and Search Keywords (10 categories to reach ~400 items)
categories = {
    "laptops": "Laptop",
    "gaming_laptops": "Gaming Laptop",
    "iphones": "iPhone",
    "samsung_phones": "Samsung Galaxy Phone",
    "general_smartphones": "Smartphone",
    "smart_tvs": "Smart TV",
    "android_tablets": "Android Tablet",
    "ipads": "iPad",
    "smartwatches": "Smartwatch",
    "monitors": "Gaming Monitor",
}

country = "sa"  # Use 'sa' for Saudi Arabia or 'us' for the United States
pages_per_keyword = 2  # 2 pages per keyword x 40 items per page = 80 items per category

all_devices_dataset = []

# 3. Fetching and Aggregating Data
for category_code, keyword in categories.items():
    print(f"=== Fetching category: {keyword} ===")

    for page in range(1, pages_per_keyword + 1):
        params = {
            "q": keyword,
            "gl": country,
            "hl": "en",
            "num": "40",  # Maximum items per request
            "page": str(page),
        }

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()
                items = data.get("shopping", [])

                if not items:
                    print(
                        f"No more results found for '{keyword}' on page {page}."
                    )
                    break

                # Tag each item with its category code
                for item in items:
                    item["device_category"] = category_code
                    all_devices_dataset.append(item)

                print(
                    f"Successfully fetched {len(items)} items from page {page} (Current Total: {len(all_devices_dataset)})"
                )
            else:
                print(
                    f"Error on page {page}: Status code {response.status_code}"
                )
                break

        except Exception as e:
            print(f"An error occurred during request: {e}")
            break

        time.sleep(1)  # Brief delay to avoid rate limiting

# 4. Saving All Fetched Items into a Single Unified JSON File
output_filename = f"all_devices_{country}_400items.json"
with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(all_devices_dataset, f, ensure_ascii=False, indent=4)

print("=" * 60)
print(
    f"Process Complete! Total {len(all_devices_dataset)} items saved to '{output_filename}'."
)