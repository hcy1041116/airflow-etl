"""
遷移腳本：
1. 更新 products.categories 為簡短原始欄位
2. 從 allergens_tags 填充 product_allergens 表
"""
import os
from pymongo import MongoClient
from sqlalchemy import create_engine, text

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
ETL_POSTGRES_CONN = os.environ.get(
    "ETL_POSTGRES_CONN",
    "postgresql+psycopg2://etl:etl@localhost:5432/off_etl"
)

UPDATE_CATEGORIES_SQL = "UPDATE products SET categories = :categories WHERE barcode = :barcode;"

INSERT_ALLERGEN_SQL = """
INSERT INTO product_allergens (barcode, allergen)
VALUES (:barcode, :allergen)
ON CONFLICT DO NOTHING;
"""


def main():
    client = MongoClient(MONGO_URI)
    raw = list(client["off_raw"]["products"].find(
        {},
        {"code": 1, "categories": 1, "allergens_tags": 1}
    ))
    client.close()
    print(f"從 MongoDB 讀到 {len(raw)} 筆")

    engine = create_engine(ETL_POSTGRES_CONN)
    with engine.begin() as conn:
        cat_updated = allergen_inserted = skipped = 0

        for p in raw:
            barcode = p.get("code")
            if not barcode:
                skipped += 1
                continue

            # 更新 categories（簡短原始欄位）
            categories = p.get("categories") or None
            conn.execute(text(UPDATE_CATEGORIES_SQL), {
                "barcode": barcode,
                "categories": categories,
            })
            cat_updated += 1

            # 插入 allergens（從 allergens_tags 拆出來）
            for tag in p.get("allergens_tags", []):
                allergen = tag.replace("en:", "").strip()
                if allergen:
                    conn.execute(text(INSERT_ALLERGEN_SQL), {
                        "barcode": barcode,
                        "allergen": allergen,
                    })
                    allergen_inserted += 1

    engine.dispose()
    print(f"categories 更新 {cat_updated} 筆")
    print(f"allergens 插入 {allergen_inserted} 筆，跳過 {skipped} 筆")


if __name__ == "__main__":
    main()
