# Meta Ads SQLite ETL

把 Meta Ads Insights API 資料抓回本機 SQLite，支援多個 ad account。

## 功能
- 同步 campaigns / adsets / ads
- 抓取 insights daily
- 保存 actions long table
- 支援多個 Meta ad account
- 支援 update / backfill / check

## 安裝

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

環境變數

請參考 .env.example

用法
更新最近 14 天
python meta_sqlite_etl_v_1.py --account act_1234567890 update --days 14
回補歷史資料
python meta_sqlite_etl_v_1.py --account act_1234567890 backfill --start 2025-01-01 --end 2026-03-08
檢查資料庫
python meta_sqlite_etl_v_1.py --account act_1234567890 check


---

## 5. 初始化 git 並提交

如果你這個資料夾還沒初始化：

```bash
git init
git add meta_sqlite_etl_v_1.py requirements.txt README.md .gitignore .env.example
git commit -m "Initial commit: Meta Ads SQLite ETL"# meta-ads-sqlite-etl
