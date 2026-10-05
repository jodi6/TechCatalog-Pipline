# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace, when, lower
from delta.tables import DeltaTable

# 1. Read data from the Bronze layer
bronze_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/bronze/"
bronze_df = spark.read.format("delta").load(bronze_path)

# 2. Select specific electronics columns based on the actual schema
silver_df = bronze_df.select(
    "asin",         
    "title",        
    "price",        
    "rating",       
    "source",       
    "ratingCount"   
)

# 3. Remove rows missing product title or price
silver_df = silver_df.dropna(subset=["title", "price"])

# 4. Clean price: remove non-numeric characters and cast to float safely
silver_df = silver_df.withColumn(
    "price",
    regexp_replace(col("price"), r"[^\d\.]", "")
).withColumn(
    "price",
    when(col("price") == "", None).otherwise(col("price").cast("float"))
)

# حذف أي صف أصبح سعره Null بعد التنظيف لضمان جودة بيانات لوحة القيادة
silver_df = silver_df.dropna(subset=["price"])

# 5. Remove duplicates based on product title to prevent Merge conflicts
silver_df = silver_df.dropDuplicates(["title"])

# 5.5 Extract and Clean Brand from Title
silver_df = silver_df.withColumn(
    "brand",
    when(lower(col("title")).contains("apple") | col("title").contains("أبل"), "Apple")
    .when(lower(col("title")).contains("honor") | col("title").contains("هونر"), "Honor")
    .when(lower(col("title")).contains("samsung") | col("title").contains("سامسونج"), "Samsung")
    .when(lower(col("title")).contains("lenovo") | col("title").contains("لينوفو"), "Lenovo")
    .when(lower(col("title")).contains("dell") | col("title").contains("ديل"), "Dell")
    .when(lower(col("title")).contains("asus") | col("title").contains("اسوس"), "Asus")
    .when(lower(col("title")).contains("acer") | col("title").contains("ايسر"), "Acer")
    .when(lower(col("title")).contains("microsoft") | col("title").contains("مايكروسوفت"), "Microsoft")
    .when(lower(col("title")).contains("hp") | col("title").contains("اتش بي"), "HP")
    .otherwise("Other")
)

# 6. Save/Update Silver Layer using Incremental Load (Merge/Upsert)
silver_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/silver/"

# Check if the Silver table already exists
if DeltaTable.isDeltaTable(spark, silver_path):
    print("Table exists. Performing Incremental Load (Merge)...")
    delta_table = DeltaTable.forPath(spark, silver_path)
    
    # Merge new data with existing data based on 'title'
    delta_table.alias("target").merge(
        silver_df.alias("source"),
        "target.title = source.title"
    ).whenMatchedUpdateAll(
        # Update existing records
    ).whenNotMatchedInsertAll(
        # Insert new records
    ).execute()
    
    print("Silver Layer updated with Incremental Load (Merge) successfully! 🚀")
else:
    # If table doesn't exist, perform initial load
    print("Table not found. Performing Initial Full Load...")
    silver_df.write.format("delta").mode("overwrite").save(silver_path)
    print("Initial Silver Layer created successfully! 🥈")

# 7. Display the cleaned data
display(silver_df)