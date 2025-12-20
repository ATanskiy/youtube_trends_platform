{{ config(
    materialized='incremental',
    unique_key=['video_id', 'region_id', 'video_snapshot_at'],
    incremental_strategy='merge',
    tags=['postgres']
) }}

SELECT *
FROM bi.stg_videos_tableau