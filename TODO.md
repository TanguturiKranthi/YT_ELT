# YT_ELT Airflow DAG Fix Plan

## Goal
Fix update_db DAG staging_table task to load YT data without errors.

## Steps
1. [ ] Fix data_utils.py: Column casing in SELECT and row access (use lowercase row['video_id'], quoted SELECT "Video_Id")
2. [x] Remove hardcoded database='yt_db' (confirmed fixed, connection succeeds)
3. [ ] Fix typos in data_utils.py (cur vs curr)
4. [ ] Rewrite data_modification.py SQL syntax and params
5. [ ] Fix data_transformation.py import datetime
6. [ ] Restart Airflow containers
7. [ ] Clear task state or re-run DAG
8. [ ] Verify data in staging.yt_api
9. [ ] Test core_table task

## Commands
- docker-compose restart
- docker exec -it postgres psql -U ELT_DATABASE_USERNAME -d ELT_DATABASE_NAME -c '\\dt staging.*'

