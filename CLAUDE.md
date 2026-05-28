# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Open Food Facts 多來源增量 ETL pipeline，目標是做出能寫進履歷的 DE 專案。

**架構：**
```
Open Food Facts API → MongoDB (raw) → pandas transform → PostgreSQL (分析層) → FastAPI
                              ↑ 全部由 Airflow 編排（每日增量）
```

## 技術棧

- Airflow 3.2.1，官方 Docker Compose，**TaskFlow API**（`@dag`/`@task`），不用舊的 PythonOperator
- MongoDB：獨立 docker service，pymongo
- PostgreSQL：獨立 docker service（**與 Airflow metadata DB 區隔**），psycopg2 + SQLAlchemy
- FastAPI：獨立 service，連 PostgreSQL
- 環境：WSL2，Docker Desktop

## 目錄結構

```
airflow-etl/
├── exercise/           # gitignore，練習用 Docker 環境
│   └── airflow-demo/
│       ├── docker-compose.yaml
│       ├── .env        # AIRFLOW_UID=1000
│       └── dags/
│           └── hello_dag.py
└── CLAUDE.md
```

## 分階段進度

- Stage 1: 環境跑通，hello_dag 全綠 ✅
- Stage 2: Extract + MongoDB
- Stage 3: Transform（pandas 清洗 + 多端點 join）
- Stage 4: Load PostgreSQL（upsert 冪等，barcode 為主鍵）
- Stage 5: 增量更新（last_modified_t watermark）+ retries + logging
- Stage 6: FastAPI service layer
- Stage 7: README 履歷化

## 關鍵設計決策

- **增量更新**：以 `last_modified_t` 為 watermark，每日只處理新增/修改的產品
- **冪等**：Postgres 用 `ON CONFLICT` upsert，Mongo 用 barcode 當 `_id`
- **兩個 DB 並存理由**：raw 是深度巢狀 JSON 適合文件庫，清洗後是結構化表格適合 RDBMS
- 每個 Stage 完成就 commit，不等全部做完
