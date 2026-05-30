import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task, Variable
from pymongo import MongoClient
from sqlalchemy import create_engine, text


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017/")
ETL_POSTGRES_CONN = os.environ.get("ETL_POSTGRES_CONN", "postgresql+psycopg2://etl:etl@postgres-etl/off_etl")
OFF_SEARCH_URL = "https://world.openfoodfacts.org/api/v2/search"
HEADERS = {"User-Agent": "off-etl-pipeline/1.0 (hcy1041116@gmail.com)"}
WATERMARK_KEY = "off_etl_watermark"
CURSOR_KEY = "off_etl_next_page"
START_TS = 1772323200  # 2026-03-01 00:00:00 UTC

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS products (
    barcode             TEXT PRIMARY KEY,
    product_name        TEXT,
    brands              TEXT,
    quantity            TEXT,
    nutriscore_grade    TEXT,
    nova_group          INTEGER,
    ecoscore_grade      TEXT,
    additives_n         INTEGER,
    allergens           TEXT,
    completeness        FLOAT,
    categories          TEXT,
    countries           TEXT,
    energy_kcal_100g    FLOAT,
    fat_100g            FLOAT,
    saturated_fat_100g  FLOAT,
    carbs_100g          FLOAT,
    sugars_100g         FLOAT,
    fiber_100g          FLOAT,
    proteins_100g       FLOAT,
    salt_100g           FLOAT,
    last_modified       TIMESTAMP,
    created_at          TIMESTAMP,
    loaded_at           TIMESTAMP DEFAULT NOW()
);
ALTER TABLE products ADD COLUMN IF NOT EXISTS nova_group      INTEGER;
ALTER TABLE products ADD COLUMN IF NOT EXISTS ecoscore_grade  TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS additives_n     INTEGER;
ALTER TABLE products ADD COLUMN IF NOT EXISTS allergens       TEXT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS completeness    FLOAT;
"""

UPSERT_SQL = """
INSERT INTO products (
    barcode, product_name, brands, quantity, nutriscore_grade,
    nova_group, ecoscore_grade, additives_n, allergens, completeness,
    categories, countries, energy_kcal_100g, fat_100g, saturated_fat_100g,
    carbs_100g, sugars_100g, fiber_100g, proteins_100g, salt_100g,
    last_modified, created_at
) VALUES (
    :barcode, :product_name, :brands, :quantity, :nutriscore_grade,
    :nova_group, :ecoscore_grade, :additives_n, :allergens, :completeness,
    :categories, :countries, :energy_kcal_100g, :fat_100g, :saturated_fat_100g,
    :carbs_100g, :sugars_100g, :fiber_100g, :proteins_100g, :salt_100g,
    :last_modified, :created_at
)
ON CONFLICT (barcode) DO UPDATE SET
    product_name        = EXCLUDED.product_name,
    brands              = EXCLUDED.brands,
    quantity            = EXCLUDED.quantity,
    nutriscore_grade    = EXCLUDED.nutriscore_grade,
    nova_group          = EXCLUDED.nova_group,
    ecoscore_grade      = EXCLUDED.ecoscore_grade,
    additives_n         = EXCLUDED.additives_n,
    allergens           = EXCLUDED.allergens,
    completeness        = EXCLUDED.completeness,
    categories          = EXCLUDED.categories,
    countries           = EXCLUDED.countries,
    energy_kcal_100g    = EXCLUDED.energy_kcal_100g,
    fat_100g            = EXCLUDED.fat_100g,
    saturated_fat_100g  = EXCLUDED.saturated_fat_100g,
    carbs_100g          = EXCLUDED.carbs_100g,
    sugars_100g         = EXCLUDED.sugars_100g,
    fiber_100g          = EXCLUDED.fiber_100g,
    proteins_100g       = EXCLUDED.proteins_100g,
    salt_100g           = EXCLUDED.salt_100g,
    last_modified       = EXCLUDED.last_modified,
    loaded_at           = NOW();
"""


@dag(
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["off"],
)
def off_etl():

    @task(retries=5, retry_delay=timedelta(seconds=90))
    def extract_from_off() -> list:
        """
        抓 Open Food Facts snacks 產品。
        按 last_modified_t 降冪排列，遇到比 watermark 舊的產品即停止分頁。
        第一次跑 watermark=0，會抓第一頁（初始載入）。
        """
        # 1777593600 = 2026-05-01 00:00:00 UTC
        watermark = int(Variable.get(WATERMARK_KEY, default="1777593600"))
        print(f"[extract] watermark = {datetime.fromtimestamp(watermark) if watermark else '未設定'}")

        all_products = []
        page = 1

        while True:
            params = {
                "categories_tags_en": "snacks",
                "sort_by": "last_modified_t",
                "page_size": 50,
                "page": page,
            }
            time.sleep(12)
            resp = requests.get(OFF_SEARCH_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()

            products = resp.json().get("products", [])
            if not products:
                break

            new_products = []
            stop = False
            for p in products:
                if p.get("last_modified_t", 0) > watermark:
                    new_products.append(p)
                else:
                    stop = True
                    break

            all_products.extend(new_products)
            print(f"[extract] page {page}：{len(new_products)} 筆新產品，累計 {len(all_products)} 筆")

            if stop or page >= 2:
                break

            page += 1

        print(f"[extract] 共抓到 {len(all_products)} 筆")
        return all_products

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def load_raw_to_mongo(products: list) -> int:
        """原始 JSON 存進 MongoDB，barcode 當 _id 自動去重"""
        if not products:
            print("[mongo] 無新資料，跳過")
            return 0

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
                # _id 重複（已存在）→ upsert
                col.replace_one({"_id": p["_id"]}, p)
                skipped += 1

        total = client["off_raw"]["products"].count_documents({})
        print(f"[mongo] 新增 {inserted} 筆，更新 {skipped} 筆，collection 共 {total} 筆")
        client.close()
        return inserted + skipped

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def transform_and_load(inserted: int) -> None:
        """從 MongoDB 讀 raw，pandas 清洗，upsert 進 PostgreSQL，更新 watermark"""
        # --- transform ---
        client = MongoClient(MONGO_URI)
        raw = list(client["off_raw"]["products"].find())
        client.close()
        print(f"[transform] 從 Mongo 讀到 {len(raw)} 筆")

        def clean_tags(tags):
            if not isinstance(tags, list):
                return None
            return ", ".join(t.replace("en:", "") for t in tags)

        rows = []
        max_last_modified_t = 0
        for p in raw:
            n = p.get("nutriments", {})
            lmt = p.get("last_modified_t", 0)
            if lmt > max_last_modified_t:
                max_last_modified_t = lmt
            allergens = p.get("allergens") or None
            rows.append({
                "barcode":            p.get("code"),
                "product_name":       p.get("product_name"),
                "brands":             p.get("brands"),
                "quantity":           p.get("quantity") or None,
                "nutriscore_grade":   p.get("nutriscore_grade"),
                "nova_group":         p.get("nova_group"),
                "ecoscore_grade":     p.get("ecoscore_grade"),
                "additives_n":        p.get("additives_n"),
                "allergens":          allergens if allergens != "" else None,
                "completeness":       p.get("completeness"),
                "categories":         p.get("categories") or None,
                "countries":          clean_tags(p.get("countries_tags")),
                "energy_kcal_100g":   n.get("energy-kcal_100g"),
                "fat_100g":           n.get("fat_100g"),
                "saturated_fat_100g": n.get("saturated-fat_100g"),
                "carbs_100g":         n.get("carbohydrates_100g"),
                "sugars_100g":        n.get("sugars_100g"),
                "fiber_100g":         n.get("fiber_100g"),
                "proteins_100g":      n.get("proteins_100g"),
                "salt_100g":          n.get("salt_100g"),
                "last_modified":      datetime.fromtimestamp(lmt) if lmt else None,
                "created_at":         datetime.fromtimestamp(p["created_t"]) if p.get("created_t") else None,
            })

        df = pd.DataFrame(rows)
        missing = df.isnull().sum()
        print(f"[transform] 共 {len(df)} 筆，缺值統計:\n{missing[missing > 0].to_string()}")

        # --- load ---
        engine = create_engine(ETL_POSTGRES_CONN)
        with engine.begin() as conn:
            conn.execute(text(CREATE_TABLE_SQL))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS product_allergens (
                    barcode  TEXT REFERENCES products(barcode) ON DELETE CASCADE,
                    allergen TEXT NOT NULL,
                    PRIMARY KEY (barcode, allergen)
                );
            """))
            upserted = 0
            for p, row in zip(raw, df.itertuples(index=False)):
                row_dict = {k: (None if pd.isna(v) else v) for k, v in row._asdict().items()}
                if not row_dict.get("barcode"):
                    continue
                conn.execute(text(UPSERT_SQL), row_dict)
                upserted += 1
                # allergens
                for tag in p.get("allergens_tags", []):
                    allergen = tag.replace("en:", "").strip()
                    if allergen:
                        conn.execute(text("""
                            INSERT INTO product_allergens (barcode, allergen)
                            VALUES (:barcode, :allergen)
                            ON CONFLICT DO NOTHING;
                        """), {"barcode": row_dict["barcode"], "allergen": allergen})
        engine.dispose()
        print(f"[postgres] upsert {upserted} 筆")

        # --- 更新 watermark ---
        if max_last_modified_t:
            Variable.set(WATERMARK_KEY, str(max_last_modified_t))
            print(f"[watermark] 更新為 {datetime.fromtimestamp(max_last_modified_t)}")

    products = extract_from_off()
    loaded = load_raw_to_mongo(products)
    transform_and_load(loaded)


off_etl()
