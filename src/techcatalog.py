# Databricks notebook source
import requests
import json
import time

# 1. Define the categories to search for
categories = ["Laptop", "Phone", "TV", "Ipad", "Tablet"]

# 2. Define APIs configuration with your updated keys and URLs
# 2. Define APIs configuration with both SA and US markets
api_configs = {
    "amazon_sa": {
        "url": "https://real-time-amazon-data.p.rapidapi.com/search",
        "headers": {
            "x-rapidapi-key": "6423579a71mshc03ee95162db25dp1defacjsn816b69348617", 
            "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
        },
        "query_param_name": "query", 
        "extra_params": {"page": "1", "country": "SA"}
    },
    "amazon_us": {
        "url": "https://real-time-amazon-data.p.rapidapi.com/search",
        "headers": {
            "x-rapidapi-key": "6423579a71mshc03ee95162db25dp1defacjsn816b69348617", 
            "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
        },
        "query_param_name": "query", 
        "extra_params": {"page": "1", "country": "US"} # السوق الأمريكي
    },
    "google_shopping_sa": {
        "url": "https://google-search-master-mega.p.rapidapi.com/shopping",
        "headers": {
            "x-rapidapi-key": "6423579a71mshc03ee95162db25dp1defacjsn816b69348617",
            "x-rapidapi-host": "google-search-master-mega.p.rapidapi.com"
        },
        "query_param_name": "q", 
        "extra_params": {"gl": "sa", "hl": "ar"}
    },
    "google_shopping_us": {
        "url": "https://google-search-master-mega.p.rapidapi.com/shopping",
        "headers": {
            "x-rapidapi-key": "6423579a71mshc03ee95162db25dp1defacjsn816b69348617",
            "x-rapidapi-host": "google-search-master-mega.p.rapidapi.com"
        },
        "query_param_name": "q", 
        "extra_params": {"gl": "us", "hl": "en"} # السوق الأمريكي
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

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, when, lower, lit

# 1. مسح الطبقة الفضية القديمة لضمان نظافة البيانات
silver_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/silver/"
dbutils.fs.rm(silver_path, True)

# 2. قراءة البيانات من البرونزية
bronze_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/bronze/"
bronze_df = spark.read.format("delta").load(bronze_path)

# 3. تحديد السوق (Market)
silver_df = bronze_df.withColumn(
    "market", 
    when(col("price").cast("string").contains("$") | col("price").cast("string").contains("USD"), lit("US")).otherwise(lit("SA"))
)

# 🌟 4. استخراج التصنيف (Category) من اسم المنتج لأننا فقدناه سابقاً
silver_df = silver_df.withColumn(
    "category",
    when(lower(col("title")).contains("laptop") | lower(col("title")).contains("لاب توب") | lower(col("title")).contains("macbook"), "Laptop")
    .when(lower(col("title")).contains("phone") | lower(col("title")).contains("جوال") | lower(col("title")).contains("iphone") | lower(col("title")).contains("smartphone"), "Phone")
    .when(lower(col("title")).contains("tv") | lower(col("title")).contains("تلفزيون"), "TV")
    .when(lower(col("title")).contains("ipad") | lower(col("title")).contains("ايباد"), "Ipad")
    .when(lower(col("title")).contains("tablet") | lower(col("title")).contains("تابلت"), "Tablet")
    .otherwise("Other")
)

# 5. اختيار الأعمدة شاملة category و market
silver_df = silver_df.select(
    "asin",         
    col("title").alias("product_title"),        
    col("price").alias("product_price"),        
    col("rating").alias("product_star_rating"),       
    "source",       
    "ratingCount",
    "market",
    "category" # 🌟 عمود التصنيف جاهز
)

# 6. إضافة الأعمدة الوهمية للداشبورد
silver_df = silver_df.withColumn("product_availability", lit("In Stock"))\
                     .withColumn("delivery", lit("Free Shipping"))\
                     .withColumn("product_photo", lit("No Image"))

# 7. إزالة القيم الفارغة
silver_df = silver_df.dropna(subset=["product_title", "product_price"])

# 8. تنظيف السعر من النصوص وتحويله لأرقام
silver_df = silver_df.withColumn(
    "product_price",
    regexp_replace(col("product_price").cast("string"), r"[^\d\.]", "")
).withColumn(
    "product_price",
    when(col("product_price") == "", None).otherwise(col("product_price").cast("float"))
)

# 9. توحيد العملة (تحويل الدولار إلى ريال سعودي)
silver_df = silver_df.withColumn(
    "product_price",
    when(col("market") == "US", col("product_price") * 3.75).otherwise(col("product_price"))
)

# 10. إزالة التكرارات
silver_df = silver_df.dropna(subset=["product_price"])
silver_df = silver_df.dropDuplicates(["product_title"])

# 11. استخراج الماركة
silver_df = silver_df.withColumn(
    "brand",
    when(lower(col("product_title")).contains("apple") | col("product_title").contains("أبل"), "Apple")
    .when(lower(col("product_title")).contains("honor") | col("product_title").contains("هونر"), "Honor")
    .when(lower(col("product_title")).contains("samsung") | col("product_title").contains("سامسونج"), "Samsung")
    .when(lower(col("product_title")).contains("lenovo") | col("product_title").contains("لينوفو"), "Lenovo")
    .when(lower(col("product_title")).contains("dell") | col("product_title").contains("ديل"), "Dell")
    .when(lower(col("product_title")).contains("asus") | col("product_title").contains("اسوس"), "Asus")
    .when(lower(col("product_title")).contains("acer") | col("product_title").contains("ايسر"), "Acer")
    .when(lower(col("product_title")).contains("microsoft") | col("product_title").contains("مايكروسوفت"), "Microsoft")
    .when(lower(col("product_title")).contains("hp") | col("product_title").contains("اتش بي"), "HP")
    .otherwise("Other")
)

# 12. حفظ الطبقة الفضية
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)

display(silver_df.select("product_title", "category", "brand", "market", "product_price"))

# COMMAND ----------

silver_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/silver/"
silver_df = spark.read.format("delta").load(silver_path)

# COMMAND ----------

# 1. أضيفي هذا السطر مباشرة بعد سطر قراءة البيانات الفضية (load)
silver_df = silver_df.withColumnRenamed("title", "product_title")

# 2. في الأكواد اللي تحتك (جداول الحقائق والأبعاد)، تأكدي إنك تستخدمين الاسم الجديد داخل الـ select
# مثال:
# fact_products = silver_df.select("product_title", "price", "rating", ...)

# COMMAND ----------

df = df.fillna({
    "product_star_rating": 0,
    "product_availability": "In Stock"
})

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

known_brands = ["HP", "Apple", "Huawei", "Dell", "Lenovo", "Asus", "Acer",
                 "Samsung", "MacBook", "Microsoft", "Honor", "هونر", "آبل"]

def extract_brand(title):
    if title is None:
        return "Unknown"
    for brand in known_brands:
        if brand.lower() in title.lower():
            return brand
    return "Other"

extract_brand_udf = udf(extract_brand, StringType())
df = df.withColumn("brand", extract_brand_udf(col("product_title")))

# COMMAND ----------

df = df.dropDuplicates(["asin"])

# COMMAND ----------

from pyspark.sql.functions import count, avg, round, col, trim

silver_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/silver/"
df = spark.read.format("delta").load(silver_path)

# 1. إزالة أي مسافات مخفية قد تسبب تكراراً وهمياً
df = df.withColumn("brand", trim(col("brand")))

# 2. التجميع
brand_gold_df = df.groupBy("brand").agg(
    count("asin").alias("total_products"),
    round(avg("product_price"), 2).alias("avg_price"),
    round(avg("product_star_rating"), 2).alias("avg_rating")
)

# 🌟 3. إجبار النظام على إزالة أي تكرار (الحل النهائي)
brand_gold_df = brand_gold_df.dropDuplicates(["brand"])

gold_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/gold/"

# مسح الملفات القديمة تماماً
dbutils.fs.rm(gold_path, True)

# الحفظ الجديد
brand_gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)

display(brand_gold_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# أضفنا عمود market بقيمة SA قبل عملية الـ select
dim_product = silver_df.select(
    "asin",
    "product_title",
    "brand",
    "category", # 🌟 تمت الإضافة
    "source",
    "product_availability",
    "delivery",
    "product_photo",
    "market"
).dropDuplicates(["product_title"])

dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_product")

# COMMAND ----------

from pyspark.sql.functions import lit

fact_products = silver_df.select(
    "product_title", 
    "product_price",
    "product_star_rating",
    "ratingCount",
    "product_availability",
    "delivery",
    "product_photo",
    "market",
    "category" # 🌟 تمت الإضافة
)

fact_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_products")

# COMMAND ----------

# 1. Dimension 1: Product Info
dim_product = silver_df.select("product_title", "asin", "brand", "source").dropDuplicates(["product_title"])
dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_product")

# ملاحظة: تم استبعاد جدول Dimension 2 الخاص بالتوصيل لعدم توفر بياناته في المصادر الحالية.