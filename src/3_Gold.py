# Databricks notebook source
silver_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/silver/"
df = spark.read.format("delta").load(silver_path)

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

from pyspark.sql.functions import count, avg, round, col

# 1. قراءة البيانات الفضية المحدثة من مساحة التخزين مباشرة لتجديد ذاكرة المتغير df
silver_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/silver/"
df = spark.read.format("delta").load(silver_path)

# 2. التجميع باستخدام المسميات الجديدة
brand_gold_df = df.groupBy("brand").agg(
    count("title").alias("total_products"),
    round(avg("price"), 2).alias("avg_price"),
    round(avg("rating"), 2).alias("avg_rating")
)

gold_path = "abfss://techcatalog@techcataloglake2026.dfs.core.windows.net/gold/"

# 3. حفظ البيانات مع خيار overwriteSchema لإجبار النظام على نسيان المسميات القديمة
brand_gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)

display(brand_gold_df)

# COMMAND ----------

# Create Dimension Table for Products
dim_product = silver_df.select(
    "asin",
    "title",
    "brand",
    "source"
).dropDuplicates(["title"])

# Save to Gold Layer as Delta Table
dim_product.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_product")

# COMMAND ----------

# Create Fact Table for Metrics & Prices
fact_products = silver_df.select(
    "title",
    "price",
    "rating",
    "ratingCount"
)

# Save to Gold Layer as Delta Table
fact_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_fact_products")

# COMMAND ----------

# 1. Dimension 1: Product Info
dim_product = silver_df.select("title", "asin", "brand", "source").dropDuplicates(["title"])
dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_product")

# ملاحظة: تم استبعاد جدول Dimension 2 الخاص بالتوصيل لعدم توفر بياناته في المصادر الحالية.