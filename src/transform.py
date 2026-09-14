import pandas as pd
import re

# ============================================================
# STEP 0: تحميل الملفات
# ============================================================

try:
    ksa_df = pd.read_csv("final_KSA_products.csv")
    us_df = pd.read_csv("final_US_products.csv")
except Exception as e:
    print(f"Error reading CSV files: {e}")
    exit()

PRODUCT_NAME_COL = "product_title"
PRICE_COL = "product_price"
USD_TO_SAR = 3.75


# ============================================================
# STEP 1: تنظيف الأسعار
# ============================================================

def clean_price(value):
    if pd.isna(value) or value is None:
        return None
    text = str(value)
    cleaned = re.sub(r"[^0-9.]", "", text)
    if not cleaned or cleaned == ".":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None

ksa_df[PRICE_COL] = ksa_df[PRICE_COL].apply(clean_price)
us_df[PRICE_COL] = us_df[PRICE_COL].apply(clean_price)


# ============================================================
# STEP 2: خريطة الترجمة وتوحيد المصطلحات (Normalizer)
# ============================================================

TRANSLATION_MAP = {
    "سامسونج": "samsung",
    "جالكسي": "galaxy",
    "تاب": "tab",
    "أبل": "apple",
    "ابل": "apple",
    "آيباد": "ipad",
    "ايباد": "ipad",
    "اير": "air",
    "برو": "pro",
    "ماك بوك": "macbook",
    "ساعة": "watch",
    "بوصة": "inch",
    "انش": "inch",
    "جيجابايت": "gb",
    "جيجا": "gb",
    "تيرابايت": "tb",
}

def normalize_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower()
    
    # استبدال الكلمات العربية بالترجمة الإنجليزية المقابلة
    for ar_word, en_word in TRANSLATION_MAP.items():
        text = text.replace(ar_word, en_word)
        
    return text

def tokenize(title):
    normalized = normalize_text(title)
    # استخراج الأحرف الإنجليزية والأرقام فقط بعد الترجمة
    words = re.findall(r"[a-zA-Z0-9]+", normalized)
    
    stop_words = {"case", "cover", "for", "with", "gen", "generation", "حافظة", "حماية", "كفر", "غطاء"}
    
    tokens = [w for w in words if w not in stop_words and len(w) >= 2]
    return list(set(tokens))

ksa_df["_tokens"] = ksa_df[PRODUCT_NAME_COL].apply(tokenize)
us_df["_tokens"] = us_df[PRODUCT_NAME_COL].apply(tokenize)


# ============================================================
# STEP 3: خوارزمية المطابقة المتقدمة (Similarity Search)
# ============================================================

def jaccard_similarity(list_a, list_b):
    set_a = set(list_a) if isinstance(list_a, list) else set()
    set_b = set(list_b) if isinstance(list_b, list) else set()
    
    if not set_a or not set_b:
        return 0.0
    
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union

us_tokens_list = us_df["_tokens"].tolist()

# خفض الحد الأدنى للقبول بعد توحيد المصطلحات للوصول لـ Matches أكثر
SIMILARITY_THRESHOLD = 0.15

matched_indices = []
matched_scores = []

for tokens in ksa_df["_tokens"]:
    best_score = 0.0
    best_index = None
    for i, us_tokens in enumerate(us_tokens_list):
        score = jaccard_similarity(tokens, us_tokens)
        if score > best_score:
            best_score = score
            best_index = i
            
    if best_score >= SIMILARITY_THRESHOLD:
        matched_indices.append(best_index)
        matched_scores.append(best_score)
    else:
        matched_indices.append(None)
        matched_scores.append(best_score)

ksa_df["_matched_us_index"] = matched_indices
ksa_df["_match_score"] = matched_scores


# ============================================================
# STEP 4: بناء وتصدير الجدول النهائي
# ============================================================

rows = []
for _, row in ksa_df[ksa_df["_matched_us_index"].notna()].iterrows():
    us_row = us_df.iloc[int(row["_matched_us_index"])]
    ksa_price = row[PRICE_COL]
    us_price = us_row[PRICE_COL]

    if pd.isna(ksa_price) or pd.isna(us_price):
        continue

    us_price_in_sar = us_price * USD_TO_SAR
    price_diff_sar = ksa_price - us_price_in_sar
    price_diff_percent = round((price_diff_sar / us_price_in_sar) * 100, 2)

    rows.append({
        "product_title_ksa": row[PRODUCT_NAME_COL],
        "product_title_us": us_row[PRODUCT_NAME_COL],
        "price_ksa_sar": ksa_price,
        "price_us_usd": us_price,
        "price_us_in_sar": round(us_price_in_sar, 2),
        "price_diff_sar": round(price_diff_sar, 2),
        "price_diff_percent": price_diff_percent,
        "match_score": round(row["_match_score"], 2)
    })

final_df = pd.DataFrame(rows)
# ترتيب النتائج حسب أعلى نسبة مطابقة
final_df = final_df.sort_values(by="match_score", ascending=False)
final_df.to_csv("final_price_comparison.csv", index=False)

print(f"Matched products: {len(final_df)} out of {len(ksa_df)}")
print("Saved to final_price_comparison.csv successfully.")