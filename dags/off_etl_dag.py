import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from pymongo import MongoClient


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017/")
OFF_SEARCH_URL = "https://world.openfoodfacts.org/api/v2/search"
HEADERS = {"User-Agent": "off-etl-pipeline/1.0 (hcy1041116@gmail.com)"}


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["off"],
)
def off_etl():

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def extract_from_off() -> list:
        """抓 Open Food Facts snacks 第一頁（按 last_modified_t 降冪）"""
        params = {
            "categories_tags_en": "snacks",
            "sort_by": "last_modified_t",
            "page_size": 24,
            "page": 1,
        }
        time.sleep(1)
        resp = requests.get(OFF_SEARCH_URL, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        products = resp.json().get("products", [])
        print(f"[extract] 抓到 {len(products)} 筆")
        return products

    @task()
    def load_raw_to_mongo(products: list) -> int:
        """原始 JSON 存進 MongoDB，barcode 當 _id 自動去重"""
        client = MongoClient(MONGO_URI)
        col = client["off_raw"]["products"]

        inserted = skipped = 0
        for p in products:
            if not p.get("code"):
                skipped += 1
                continue
            p["_id"] = p["code"]
            try:
                col.insert_one(p)
                inserted += 1
            except Exception:
                skipped += 1

        print(f"[mongo] 新增 {inserted} 筆，跳過 {skipped} 筆")
        client.close()
        return inserted

    @task()
    def transform_with_pandas(inserted: int) -> int:
        """從 MongoDB 讀 raw，攤平 nutriments，清洗欄位"""
        client = MongoClient(MONGO_URI)
        raw = list(client["off_raw"]["products"].find())
        client.close()
        print(f"[transform] 從 Mongo 讀到 {len(raw)} 筆")

        def clean_tags(tags):
            if not isinstance(tags, list):
                return None
            return ", ".join(t.replace("en:", "") for t in tags)

        rows = []
        for p in raw:
            n = p.get("nutriments", {})
            rows.append({
                "barcode":            p.get("code"),
                "product_name":       p.get("product_name"),
                "brands":             p.get("brands"),
                "quantity":           p.get("quantity"),
                "nutriscore_grade":   p.get("nutriscore_grade"),
                "categories":         clean_tags(p.get("categories_tags")),
                "countries":          clean_tags(p.get("countries_tags")),
                "energy_kcal_100g":   n.get("energy-kcal_100g"),
                "fat_100g":           n.get("fat_100g"),
                "saturated_fat_100g": n.get("saturated-fat_100g"),
                "carbs_100g":         n.get("carbohydrates_100g"),
                "sugars_100g":        n.get("sugars_100g"),
                "fiber_100g":         n.get("fiber_100g"),
                "proteins_100g":      n.get("proteins_100g"),
                "salt_100g":          n.get("salt_100g"),
                "last_modified":      datetime.fromtimestamp(p["last_modified_t"]) if p.get("last_modified_t") else None,
                "created_at":         datetime.fromtimestamp(p["created_t"]) if p.get("created_t") else None,
            })

        df = pd.DataFrame(rows)
        missing = df.isnull().sum()
        print(f"[transform] 共 {len(df)} 筆，缺值統計:\n{missing[missing > 0].to_string()}")
        print(f"[transform] 預覽:\n{df[['barcode','product_name','brands','nutriscore_grade','energy_kcal_100g']].head().to_string()}")
        return len(df)

    products = extract_from_off()
    loaded = load_raw_to_mongo(products)
    transform_with_pandas(loaded)


off_etl()
