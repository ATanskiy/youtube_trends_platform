
  
    
        create or replace table gold.videos_enriched
      
      
    using iceberg
      
      
      
      
      
      

      as
      

SELECT
    g.video_id,
    g.region_id,
    g.language_id,
    g.language_id_src,
    g.channel_id,
    g.category_id,

    g.snapshot_at        AS video_snapshot_at,
    g.published_at       AS video_published_at,

    g.view_count,
    g.like_count,
    g.favorite_count,
    g.comment_count,

    g.view_growth,
    g.like_growth,
    g.favorite_growth,
    g.comment_growth,

    v.title,
    v.description,
    v.duration,

    ch.channel_title,
    c.category_name,
    COALESCE(
        l.language_name,
        CASE g.language_id_src
            WHEN 'ab'  THEN 'Abkhazian'
            WHEN 'aa'  THEN 'Afar'
            WHEN 'ff'  THEN 'Fulah'
            WHEN 'vro' THEN 'Võro'
            WHEN 'mo'  THEN 'Moldovan'
            WHEN 'wo'  THEN 'Wolof'
            WHEN 'ht'  THEN 'Haitian Creole'
            WHEN 'sn'  THEN 'Shona'
            WHEN 'fa'  THEN 'Persian'
            WHEN 'yue' THEN 'Cantonese'
            WHEN 'pt'  THEN 'Portuguese'
            WHEN 'zh'  THEN 'Chinese'
            WHEN 'bn'  THEN 'Bengali'
            WHEN 'bh'  THEN 'Bihari'
            WHEN 'ki'  THEN 'Kikuyu'
            WHEN 'nl'  THEN 'Dutch'
            WHEN 'de'  THEN 'German'
            WHEN 'ig'  THEN 'Igbo'
            WHEN 'yo'  THEN 'Yoruba'
            WHEN 'ps'  THEN 'Pashto'
            WHEN 'tg'  THEN 'Tajik'
            WHEN 'sh'  THEN 'Serbo-Croatian'
            WHEN 'jv'  THEN 'Javanese'

            -- special / technical cases
            WHEN 'und' THEN 'Undetermined'
            WHEN 'zxx' THEN 'No linguistic content'

            -- base languages that may appear without region
            WHEN 'en'  THEN 'English'
            WHEN 'es'  THEN 'Spanish'
            WHEN 'fr'  THEN 'French'

            ELSE NULL
        END
    ) AS language_name,
    r.region_name,
    rg.latitude,
    rg.longitude

FROM silver.fct_videos_growth g
LEFT JOIN silver.dim_videos v       ON g.video_id = v.video_id
LEFT JOIN silver.dim_channels ch    ON g.channel_id = ch.channel_id
LEFT JOIN silver.dim_categories c   ON g.category_id = c.category_id
LEFT JOIN silver.dim_languages l    ON g.language_id = l.language_id
LEFT JOIN silver.dim_regions r      ON g.region_id = r.region_id
LEFT JOIN silver.dim_regions_geo rg ON g.region_id = rg.region_id
  