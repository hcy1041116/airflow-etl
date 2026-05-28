<div align="center">

```
 ██████╗ ███████╗███████╗    ███████╗████████╗██╗
██╔═══██╗██╔════╝██╔════╝    ██╔════╝╚══██╔══╝██║
██║   ██║█████╗  █████╗      █████╗     ██║   ██║
██║   ██║██╔══╝  ██╔══╝      ██╔══╝     ██║   ██║
╚██████╔╝██║     ██║         ███████╗   ██║   ███████╗
 ╚═════╝ ╚═╝     ╚═╝         ╚══════╝   ╚═╝   ╚══════╝
```

**Open Food Facts — Multi-Source Incremental ETL Pipeline**

![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.2.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.x-150458?style=for-the-badge&logo=pandas&logoColor=white)

</div>

---

## Overview

以 **Apache Airflow** 編排的每日增量 ETL pipeline，從 Open Food Facts 公開 API 抓取食品資料，經 **MongoDB** 原始層暫存、**pandas** 清洗轉換，最終寫入 **PostgreSQL** 分析層，供後續查詢與分析使用。

專案特色：
- **真實增量更新**：以 `last_modified_t` 為 watermark，每日只處理新增或修改的產品
- **冪等寫入**：PostgreSQL 以 barcode 為主鍵做 `ON CONFLICT` upsert，MongoDB 用 barcode 當 `_id`，重跑不造成重複
- **多層架構**：raw layer（MongoDB）與分析 layer（PostgreSQL）各司其職，資料形狀決定儲存方式

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Apache Airflow 3.2.1                      │
│              Daily Schedule / Retry / Monitoring             │
└──────────────────────┬──────────────────────────────────────┘
                       │ orchestrates
          ┌────────────▼────────────┐
          │    extract_from_off     │  GET /api/v2/search
          │   Open Food Facts API   │  sort_by=last_modified_t
          └────────────┬────────────┘  page through watermark
                       │ raw JSON (巢狀)
          ┌────────────▼────────────┐
          │   load_raw_to_mongo     │
          │  ┌───────────────────┐  │
          │  │  MongoDB 7.0      │  │  barcode = _id（去重）
          │  │  off_raw.products │  │  原樣存入，不加工
          │  └───────────────────┘  │
          └────────────┬────────────┘
                       │ read raw
          ┌────────────▼────────────┐
          │  transform_with_pandas  │  攤平 nutriments
          │      pandas 2.x         │  清洗缺值 / 統一單位
          └────────────┬────────────┘  strip "en:" 前綴
                       │ structured rows
          ┌────────────▼────────────┐
          │    load_to_postgres     │
          │  ┌───────────────────┐  │
          │  │  PostgreSQL 16    │  │  ON CONFLICT upsert
          │  │  off_etl.products │  │  barcode = PRIMARY KEY
          │  └───────────────────┘  │
          └─────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | 用途 |
|-------|------|------|
| Orchestration | Apache Airflow 3.2.1 | DAG 排程、重試、監控 |
| Data Source | Open Food Facts API | 免費公開食品資料庫 |
| Raw Layer | MongoDB 7.0 | 存巢狀 JSON，不強行攤平 |
| Transform | pandas 2.x | 清洗、攤平、欄位整合 |
| Analytics Layer | PostgreSQL 16 | 結構化表格，供 SQL 查詢 |
| Infrastructure | Docker Compose | 本機全套環境一鍵啟動 |

---

## 為什麼這樣設計

**MongoDB 存 raw**：Open Food Facts 回傳深度巢狀 JSON（nutriments、categories_tags、多語言欄位...），原樣存文件資料庫比強行攤平進關聯表合理。

**PostgreSQL 存分析層**：清洗後的資料是欄位固定的結構化表格，適合做 SQL 聚合查詢。

**增量更新**：資料量超過百萬筆，每次全量重抓不現實。產品帶 `last_modified_t` 時間戳，以此為 watermark 只抓「上次跑完之後有變動的產品」，這是真實需求而非硬湊。

---

## Quick Start

**前置要求**：Docker Desktop（記憶體至少配 4GB）、WSL2

```bash
git clone git@github-hcy:hcy1041116/airflow-etl.git
cd airflow-etl

# 建必要目錄 + 設環境變數
mkdir -p logs plugins config
echo "AIRFLOW_UID=$(id -u)" >> .env

# 初始化資料庫（只需跑一次）
docker compose up airflow-init

# 啟動所有服務
docker compose up
```

開啟 [localhost:8080](http://localhost:8080)，帳密皆為 `airflow`。

找到 `off_etl` DAG，點 ▶ 手動觸發。

---

## DAG 結構

```
extract_from_off → load_raw_to_mongo → transform_with_pandas → load_to_postgres
```

| Task | 說明 |
|------|------|
| `extract_from_off` | 打 OFF API，按 last_modified_t 降冪取最新修改產品，retries=3 |
| `load_raw_to_mongo` | 原始 JSON 寫進 MongoDB，barcode 為 `_id` 自動去重 |
| `transform_with_pandas` | 從 Mongo 讀 raw，攤平 nutriments，處理缺值，統一欄位 |
| `load_to_postgres` | upsert 進 PostgreSQL，ON CONFLICT DO UPDATE |

---

## Services

| Service | Port | 說明 |
|---------|------|------|
| Airflow UI | 8080 | DAG 監控、手動觸發、log 查看 |
| PostgreSQL (ETL) | 5432 | 分析層資料庫（`off_etl` DB） |
| MongoDB | 27017 | Raw layer（`off_raw` DB） |
