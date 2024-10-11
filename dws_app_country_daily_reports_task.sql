CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.dws_app_country_daily_reports_task`(run_date DATE, history_day INT64, end_day INT64)
begin

delete `gzdw2024.gz_bi.dws_app_country_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
  and package_name not in ('fb.ai.avatar.puzzle', 'fb.ai.avatar');


insert `gzdw2024.gz_bi.dws_app_country_daily_reports`
------日活	
SELECT
	stats_date
	,a.package_name
	,app_name
	,country_code
	,sum(active_uv) as active_uv
	,sum(new_uv) as new_uv
	,sum(ratio) as ratio
	,sum(vip_revenue)+ sum(ad_revenue) as total_revenue
	,sum(asa_cost)+sum(ga_cost) as total_cost
	,sum(vip_revenue) as vip_revenue
	,sum(ad_revenue) as ad_revenue
	,sum(asa_cost) as asa_cost
	,sum(ga_cost) as ga_cost
	,sum(new_retain_uv) as ga_cost
	,0 as fb_cost
FROM
	(
	SELECT
		stats_date
		,package_name
		--,app_name
		,country_code
		,active_uv
		,new_uv
		,ratio
		,0 as vip_revenue
		,0 as ad_revenue
		,0 as asa_cost
		,0 as ga_cost
		,new_retain_uv
	FROM `gzdw2024.gz_bi.dws_app_daily_reports` 
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
   and package_name not in ('fb.ai.avatar.puzzle', 'fb.ai.avatar')
	--and app_name is not null
	union all 
	------订阅收入
	SELECT
		stats_date
		,package_name
		--,app_name
		,country_code
		,0 as active_uv
		,0 as new_uv
		,0 as ratio
		,vip_revenue
		,0 as ad_revenue
		,0 as asa_cost
		,0 as ga_cost
		,0 as new_retain_uv
	FROM gzdw2024.revenue.dws_app_country_vip_income 
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
	union all 
	---广告收入
	SELECT
		stats_date
		,package_name
		--,app_name
		,country_code
		,0 as active_uv
		,0 as new_uv
		,0 as ratio
		,0 as vip_revenue
		,ad_revenue
		,0 as asa_cost
		,0 as ga_cost
		,0 as new_retain_uv
	FROM gzdw2024.ad_platform_data.dws_ad_country_daily_reports
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
  union all
	---asa投放成本
	SELECT
		stats_date
		,package_name
		--,app_name
		,country_code
		,0 as active_uv
		,0 as new_uv
		,0 as ratio
		,0 as vip_revenue
		,0 as ad_revenue
		,asa_cost
		,0 as ga_cost
		,0 as new_retain_uv
	FROM gzdw2024.cost_data.dws_asa_cost_daily
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)	
    union all
	-----ga投放
	SELECT
		stats_date
		,package_name
		--,app_name
		,country_code
		,0 as active_uv
		,0 as new_uv
		,0 as ratio
		,0 as vip_revenue
		,0 as ad_revenue
		,0 as asa_cost
		,ga_cost
		,0 as new_retain_uv
	FROM gzdw2024.cost_data.dws_ga_cost_daily
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
	)a
	  left join
    (
    SELECT package_name,app_name FROM `gzdw2024.gz_dim.app_info`

      )c 
    on a.package_name=c.package_name
	WHERE 1=1 
	--AND country_code='TOTAL'
	group by stats_date
	,package_name
	,app_name
	,country_code
	ORDER BY stats_date,total_revenue desc ;

end;
