"""
一次性遷移腳本：從 MongoDB raw data 補充新欄位進 PostgreSQL
執行方式：python scripts/migrate_add_fields.py
"""
import os
from pymongo import MongoClient
from sqlalchemy import create_engine, text

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
ETL_POSTGRES_CONN = os.environ.get(
    "ETL_POSTGRES_CONN",
    "postgresql+psycopg2://etl:etl@localhost:5432/off_etl"
)

ALTER_SQL = """
ALTER TABLE products
    ADD COLUMN IF NOT EXISTS nova_group       INTEGER,
    ADD COLUMN IF NOT EXISTS ecoscore_grade   TEXT,
    ADD COLUMN IF NOT EXISTS additives_n      INTEGER,
    ADD COLUMN IF NOT EXISTS allergens        TEXT,
    ADD COLUMN IF NOT EXISTS completeness     FLOAT;
"""

UPDATE_SQL = """
UPDATE products SET
    nova_group     = :nova_group,
    ecoscore_grade = :ecoscore_grade,
    additives_n    = :additives_n,
    allergens      = :allergens,
    completeness   = :completeness
WHERE barcode = :barcode;
"""


def main():
    # 從 MongoDB 讀 raw
    client = MongoClient(MONGO_URI)
    raw = list(client["off_raw"]["products"].find(
        {},
        {"code": 1, "nova_group": 1, "ecoscore_grade": 1,
         "additives_n": 1, "allergens": 1, "completeness": 1}
    ))
    client.close()
    print(f"從 MongoDB 讀到 {len(raw)} 筆")

    engine = create_engine(ETL_POSTGRES_CONN)
    with engine.begin() as conn:
        # 加新欄位
        conn.execute(text(ALTER_SQL))
        print("ALTER TABLE 完成")

        # 逐筆更新
        updated = skipped = 0
        for p in raw:
            barcode = p.get("code")
            if not barcode:
                skipped += 1
                continue

            allergens = p.get("allergens") or None
            if allergens == "":
                allergens = None

            conn.execute(text(UPDATE_SQL), {
                "barcode":       barcode,
                "nova_group":    p.get("nova_group"),
                "ecoscore_grade": p.get("ecoscore_grade"),
                "additives_n":   p.get("additives_n"),
                "allergens":     allergens,
                "completeness":  p.get("completeness"),
            })
            updated += 1

    engine.dispose()
    print(f"更新 {updated} 筆，跳過 {skipped} 筆（無 barcode）")


if __name__ == "__main__":
    main()
