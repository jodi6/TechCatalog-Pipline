import json
import time
import requests

# API Configuration
url = "https://google-search-master-mega.p.rapidapi.com/shopping"

headers = {
    "x-rapidapi-key": "242576a031mshd54a5d3eb47e701p12d692jsne33cbfddc219",
    "x-rapidapi-host": "google-search-master-mega.p.rapidapi.com",
}

# The 5 device categories for the US market
categories = {
    "laptops": "Laptop",
    "phones": "Smartphone",
    "tvs": "Smart TV",
    "tablets": "Tablet",
    "ipads": "iPad",
}

country = "us"  # United States Market
pages_per_category = 2  # Fetches up to 80 items per category (40 items x 2 pages)

all_us_devices_dataset = []

# Fetching loop across all 5 device categories
for category_code, keyword in categories.items():
    print(
        f"=== Fetching US Market Data for: {category_code} ({keyword}) ==="
    )

    for page in range(1, pages_per_category + 1):
        params = {
            "q": keyword,
            "gl": country,
            "hl": "en",
            "num": "40",  # Maximum allowed results per request
            "page": str(page),
        }

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()
                items = data.get("shopping", [])

                if not items:
                    print(
                        f"No more items found for '{keyword}' on page {page}."
                    )
                    break

                # Tag each item with its specific device category
                for item in items:
                    item["device_category"] = category_code
                    all_us_devices_dataset.append(item)

                print(
                    f"Successfully fetched {len(items)} items from page {page} for '{category_code}'. Total count: {len(all_us_devices_dataset)}"
                )

            else:
                print(
                    f"Failed request on page {page}. Status code: {response.status_code}"
                )
                break

        except Exception as e:
            print(f"Error occurred during request execution: {e}")
            break

        time.sleep(1)  # Brief pause between API requests

# Save all gathered devices across all 5 categories into a single unified JSON file
output_filename = "all_us_devices_dataset.json"
with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(all_us_devices_dataset, f, ensure_ascii=False, indent=4)

print("=" * 65)
print(
    f"Task Completed Successfully! Saved a total of {len(all_us_devices_dataset)} US devices into a single file: '{output_filename}'"
)