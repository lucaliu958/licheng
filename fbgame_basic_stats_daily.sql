CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fbgame_basic_stats_daily`(run_date DATE, history_day INT64, hitory_end_day INT64)
begin




delete gzdw2024.fbgame_01_basic.fbgame_basic_stats_daily
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<date_add(run_date,interval -hitory_end_day day);


insert `gzdw2024.fbgame_01_basic.fbgame_basic_stats_daily`
	select
	a.stats_date
	,'fb.ai.avatar' as package_name
	,'fb_ai_avatar' as app_name
	,sum(active_uv) as active_uv
	,sum(new_uv) as new_uv
	,sum(total_revenue) as total_revenue
	,sum(vip_revenue) as vip_revenue
 	, sum(ad_revenue) as ad_revenue
 	, sum(cost) as cost
FROM
	(
	select 
	event_date as stats_date
	,active_uv
	,new_uv
	,0 as total_revenue
	,0 as vip_revenue
	,0 as ad_revenue
	,0 as cost 
	FROM fb-ai-avatar-puzzle.fb_dw.dws_user_active_report
	WHERE country_code='TOTAL'
	AND event_date>=date_add(run_date,interval -history_day day) 
	AND event_date<date_add(run_date,interval -hitory_end_day day) 
	and platform='TOTAL' 
	UNION ALL 
	SELECT   
		stats_date
		,0 as active_uv
		,0 as new_uv	
		,sum(revenue) as total_revenue
		,0 as vip_revenue
		,sum(revenue) as ad_revenue
		,sum(cost) as cost 
	FROM `gzdw2024.fbgame_03_bi.dws_fb_daily_reports` 
	WHERE 1=1
	AND stats_date>=date_add(run_date,interval -history_day day) 
	AND stats_date<date_add(run_date,interval -hitory_end_day day)
	and country_code='TOTAL'
	and stats_date>='2024-09-08'
	and platform='TOTAL' 
	group by stats_date
	 union all 
	SELECT 
	PARSE_DATE('%Y-%m-%d', string_field_0) AS stats_date
		,0 as active_uv
		,0 as new_uv	
		,safe_CAST(REPLACE(REPLACE(string_field_7, '$', ''), ',', '') AS FLOAT64) as total_revenue
		,0 as vip_revenue
		,safe_CAST(REPLACE(REPLACE(string_field_7, '$', ''), ',', '') AS FLOAT64) as ad_revenue
		--,safe_CAST(REPLACE(REPLACE(string_field_9, '$', ''), ',', '') AS FLOAT64) AS fb_revenue
		,safe_CAST(REPLACE(REPLACE(string_field_2, '$', ''), ',', '') AS FLOAT64) AS cost
	 FROM `gzdw2024.revenue.fb_daily_data_google_sheet` 
	 WHERE length(string_field_0)>6
	 and PARSE_DATE('%Y-%m-%d', string_field_0) >= date_add(run_date,interval -history_day day)
	 and PARSE_DATE('%Y-%m-%d', string_field_0) < date_add(run_date,interval -hitory_end_day day)
	 and PARSE_DATE('%Y-%m-%d', string_field_0)<'2024-09-08'
	)a 
	

	group by stats_date
	order by stats_date;


	end;
