"""
ملف اختبار جودة المخرجات النهائي (test_transform.py)
"""

import pandas as pd

try:
    df = pd.read_csv("final_price_comparison.csv")
except Exception as e:
    print(f"❌ تعذر فتح الملف: {e}")
    exit()

errors = []

# اختبار 1: التأكد من أن الملف ليس فارغاً
if len(df) == 0:
    errors.append("الملف final_price_comparison.csv فاضي! تأكدي إن الدمج نجح.")

# اختبار 2: عدم وجود قيم فارغة (NaN) في الأعمدة الأساسية
important_cols = ["product_title_ksa", "product_title_us", "price_diff_sar", "price_diff_percent"]
for col in important_cols:
    if col in df.columns:
        if df[col].isna().any():
            errors.append(f"فيه قيم فاضية (NaN) بعمود '{col}'.")
    else:
        errors.append(f"العمود المهم '{col}' غير موجود بالملف.")

# اختبار 3: الأسعار أرقام موجبة وأكبر من الصفر
price_cols = [c for c in df.columns if "price" in c.lower() and "diff" not in c.lower() and "percent" not in c.lower()]
for col in price_cols:
    if (df[col] <= 0).any():
        errors.append(f"فيه سعر أقل من أو يساوي صفر بعمود '{col}'.")

# اختبار 4: التحقق الصحيح من معادلة نسبة فرق السعر لأول صف
if len(df) > 0:
    row = df.iloc[0]
    if "price_diff_sar" in df.columns and "price_us_in_sar" in df.columns:
        calculated_diff = round((row["price_diff_sar"] / row["price_us_in_sar"]) * 100, 2)
        if abs(calculated_diff - row["price_diff_percent"]) > 0.01:
            errors.append(f"حساب نسبة الفرق بالصف الأول غير دقيق. المحسوب: {calculated_diff}، الموجود: {row['price_diff_percent']}")

# النتيجة النهائية
if errors:
    print("❌ فيه مشاكل لازم تصلحينها:\n")
    for e in errors:
        print(" -", e)
else:
    print("✅ كل الاختبارات نجحت! ملفك جاهز للتسليم 100%.")