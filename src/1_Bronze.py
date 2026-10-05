# Databricks notebook source
import requests
import json
import time

# 1. Define the categories to search for
categories = ["Laptop", "Phone", "TV", "Ipad", "Tablet"]

# 2. Define APIs configuration with your updated keys and URLs
api_configs = {
    "amazon": {
        "url": "https://real-time-amazon-data.p.rapidapi.com/search",
        "headers": {
            "x-rapidapi-key": "6423579a71mshc03ee95162db25dp1defacjsn816b69348617", 
            "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
        },
        "query_param_name": "query", 
        "extra_params": {"page": "1", "country": "SA"}
    },
    "google_shopping": {
        "url": "https://google-search-master-mega.p.rapidapi.com/shopping",
        "headers": {
            "x-rapidapi-key": "6423579a71mshc03ee95162db25dp1defacjsn816b69348617", # المفتاح الجديد لقوقل
            "x-rapidapi-host": "google-search-master-mega.p.rapidapi.com"
        },
        "query_param_name": "q", 
        "extra_params": {"gl": "sa", "hl": "ar"}
    }
}

print("Starting API data ingestion for all categories... ⏳")

# 3. Loop through each platform (Amazon, Google)
for platform, config in api_configs.items():
    print(f"--- Processing platform: {platform.upper()} ---")
    
    # 4. Loop through each category for the current platform
    for category in categories:
        print(f"Fetching {category} data...")
        
        # Prepare parameters dynamically based on platform requirements
        params = config["extra_params"].copy()
        params[config["query_param_name"]] = category
        
        try:
            # Make the API request
            response = requests.get(config["url"], headers=config["headers"], params=params)

            if response.status_code == 200:
                data = response.json()
                
                # Generate a highly organized filename
                file_name = f"raw_{platform}_{category.lower()}_{int(time.time())}.json"
                raw_path = f"abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/raw/{file_name}"
                
                # Save data directly to the Raw layer in Azure
                dbutils.fs.put(raw_path, json.dumps(data), True)
                
                print(f"Success! [{category}] saved to: {raw_path} 📥")
            else:
                print(f"Failed [{category}]. Status code: {response.status_code} ❌")
                print(f"Details: {response.text}")
                
        except Exception as e:
            print(f"Error fetching [{category}]: {str(e)} ❌")
        
        # Pause for 5 seconds to avoid rate limiting blocks
        time.sleep(5)

print("All ingestion tasks completed successfully! 🚀")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# 1. Define the raw data path in the data lake
file_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/raw/"

# 2. Read multiline JSON files from Raw layer
df = spark.read.option("multiline", "true").json(file_path)

# 3. Define the Bronze layer path
bronze_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/bronze/"

# 4. Add ingestion timestamp column for historical tracking
bronze_df = df.withColumn("ingestion_timestamp", current_timestamp())

# 5. Save raw data in Delta format using Append mode (Incremental Load)
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(bronze_path)

print("Bronze Layer saved successfully with Incremental Load (Append)! 🥉")

# Display the dataframe to verify
display(bronze_df)

# COMMAND ----------

# Define the raw data path in the data lake
file_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/raw/"

# Read multiline JSON files
df = spark.read.option("multiline", "true").json(file_path)

# Display the dataframe
display(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Define the Bronze layer path
bronze_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/bronze/"

# Add ingestion timestamp column for historical tracking
bronze_df = df.withColumn("ingestion_timestamp", current_timestamp())

# Save raw data in Delta format using Append mode (Incremental Load)
bronze_df.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)

print("Bronze Layer saved successfully with Incremental Load (Append)! 🥉")