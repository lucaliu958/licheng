CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.ad_platform_country_daily_reports`(run_date DATE, history_day INT64, end_day INT64)
begin



delete `gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day  day);


insert    gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
SELECT 
	cast(stats_date as date) stats_date
	,a.package_name
	,app_name
	,upper(country) as country_code ----ID
	,'admob' as ad_platform
	,sum(estimated_earnings) ad_revenue
	,sum(case when lower(ad_unit_display_label) like '%tradplus%' then estimated_earnings end) admob_tradplus_revenue
	,sum(case when lower(ad_unit_display_label) like '%topon%' then estimated_earnings end) admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `gzdw2024.admob.prefect_admob_normal_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.package_name=b.package_name
 WHERE 1=1
 and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
 --and country='TOTAL'
group by stats_date
,package_name
,country,app_name
order by ad_revenue desc ;


insert    gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
SELECT 
	cast(stats_date as date) stats_date
	,a.package_name
	,app_name
	,country ----ID
	,'admob' as ad_platform
	,sum(estimated_earnings) ad_revenue
	,sum(case when lower(ad_unit_display_label) like '%tradplus%' then estimated_earnings end) admob_tradplus_revenue
	,sum(case when lower(ad_unit_display_label) like '%topon%' then estimated_earnings end) admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `hzdw2024.admob.prefect_admob_normal_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.package_name=b.package_name
 WHERE 1=1
 and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
 --and country='TOTAL'
group by stats_date
,package_name
,country,app_name
order by ad_revenue desc ;




-----top on 


insert    gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
SELECT 
	parse_date('%Y%m%d',_table_suffix) stats_date
	,app.pkg_name as package_name
	,app_name
	,area country ----ID
	,'topon' as ad_platform
	,sum(revenue) ad_revenue
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `hzdw2024.topon.prefect_topon_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.app.pkg_name=b.package_name
 WHERE  1=1 
  and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
and area<>'TOTAL'
group by stats_date
,package_name
,country,app_name
union all 
SELECT 
	parse_date('%Y%m%d',_table_suffix) event_date
	,app.pkg_name as package_name
	,app_name
	,'TOTAL' country ----ID
	,'topon' as ad_platform
	,sum(revenue) ad_revenue
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `hzdw2024.topon.prefect_topon_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.app.pkg_name=b.package_name
 WHERE  1=1 
  and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
 AND app.pkg_name  not in ('vidma.screenrecorder.videorecorder.videoeditor.pro')
and area<>'TOTAL'
group by event_date
,package_name
,country,app_name
order by ad_revenue desc ;

/*
insert    gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
SELECT 
	cast(stats_date as date) as stats_date
	,a.package_name
	,app_name
	,country
	,'topon' as ad_platform
	,sum(revenue) ad_revenue
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `gzdata.topon.prefect_topon_revenue_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.package_name=b.package_name
 WHERE 1=1
  and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
group by stats_date
	,package_name
	,app_name
	,country
order by ad_revenue desc ;
*/


-------applovin
insert    gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
SELECT 
	date(day) as stats_date
	,a.package_name
	,app_name
	,upper(country) as country
	,'applovin' as ad_platform
	,sum(estimated_revenue) as ad_revenue
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `gzdw2024.applovin.prefect_applovin_max_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.package_name=b.package_name
 WHERE 1=1
   and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
 group by package_name ,country,stats_date,app_name
union all 
SELECT 
	date(day) as stats_date
	,a.package_name
	,app_name
	,'TOTAL' as country
	,'applovin' as ad_platform
	,sum(estimated_revenue) as ad_revenue
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `gzdw2024.applovin.prefect_applovin_max_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.package_name=b.package_name
 WHERE 1=1
  and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
 group by package_name ,country,stats_date,app_name
 order by ad_revenue desc ;


insert    gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
 SELECT 
	date(day) as stats_date
	,a.package_name
	,app_name
	,upper(country) as country
	,'applovin' as ad_platform
	,sum(estimated_revenue) as ad_revenue
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `hzdw2024.applovin.prefect_applovin_max_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.package_name=b.package_name
 WHERE 1=1
  and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
 group by package_name ,country,stats_date,app_name
union all 
SELECT 
	date(day) as stats_date
	,a.package_name
	,app_name
	,'TOTAL' as country
	,'applovin' as ad_platform
	,sum(estimated_revenue) as ad_revenue
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
 FROM `hzdw2024.applovin.prefect_applovin_max_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.package_name=b.package_name
 WHERE 1=1
  and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
 group by package_name ,country,stats_date,app_name
 order by ad_revenue desc ;







 ---------appodeal
 insert    gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
 SELECT parse_date('%Y%m%d',_table_suffix)  stats_date
	,a.package_name
	,b.app_name
	,'TOTAL' as country
	,'appodeal' as ad_platform
	,sum(revenue) as ad_revenue 
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,sum(case when network='admob' then revenue else null end) as appodeal_admob_revenue 
	,sum(case when network='applovin' then revenue else null end) as appodeal_applovin_revenue 
	,sum(case when network='vungle' then revenue else null end) as appodeal_vungle_revenue 
	,sum(case when network='appodeal_exchange' then revenue else null end) as appodeal_appodeal_exchange_revenue 
 FROM `hzdw2024.appodeal.prefect_appodeal_report_*` a 
 join  `gzdw2024.gz_dim.app_info`  b  
 on a.package_name=b.package_name
where 1=1
 and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
group by stats_date,a.package_name,country,ad_platform,b.app_name;




-----tradplusad
insert    gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
SELECT parse_date('%Y%m%d',_table_suffix)  stats_date
	,a.packageName
	,app_name
	,upper(area) country ----ID
	,'tradplusad' as ad_platform
	,sum(Revenue) as ad_revenue 
	 ,0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
FROM `hzdw2024.tradplusad.prefect_tradplusad_report_*` a 
join  `gzdw2024.gz_dim.app_info`  b  
 on a.packageName=b.package_name
 where 1=1
 and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
group by stats_date,a.packageName,country,ad_platform,app_name
union all 
SELECT parse_date('%Y%m%d',_table_suffix)  stats_date
	,a.packageName
  ,app_name
	,'TOTAL' country ----ID
	,'tradplusad' as ad_platform
	,sum(Revenue) as ad_revenue 
	, 0 as admob_tradplus_revenue
	,0 as admob_topon_revenue
	,0 as appodeal_admob_revenue
	,0 as appodeal_applovin_revenue
	,0 as appodeal_vungle_revenue
	,0 as appodeal_appodeal_exchange_revenue
FROM `hzdw2024.tradplusad.prefect_tradplusad_report_*` a 
join  `gzdw2024.gz_dim.app_info`  b  
 on a.packageName=b.package_name
 where 1=1
 and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
 and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -end_day  day) as string),'-','')
 and _TABLE_SUFFIX>='20240901'
group by stats_date,a.packageName,country,ad_platform,app_name
ORDER BY ad_revenue desc ;



delete `gzdw2024.ad_platform_data.dws_ad_country_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day  day);


insert    gzdw2024.ad_platform_data.dws_ad_country_daily_reports
SELECT
	stats_date
	,package_name
	,app_name
	,country_code
	,sum(case when app_name='Recorder Pro' and ad_platform!='tradplusad' then 0 else ad_revenue end) as ad_revenue
FROM gzdw2024.ad_platform_data.dws_ad_platform_country_daily_reports
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day day)
group by stats_date,package_name,app_name,country_code;







end;
