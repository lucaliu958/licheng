CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.dws_app_country_daily_reports_task`(run_date DATE, history_day INT64, end_day INT64)
begin

delete `gzdw2024.gz_bi.dws_app_country_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
	and stats_date<=date_add(run_date,interval -end_day day)
 and package_name not like 'fb%'
  and package_name not in ('co.springtech.scanner','com.textNumber.phone');


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
	,sum(conversions) as conversions
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
		,0 as conversions
	FROM `gzdw2024.gz_bi.dws_app_daily_reports` 
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
	and stats_date<=date_add(run_date,interval -end_day day)
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
		,0 as conversions
	FROM gzdw2024.revenue.dws_app_country_vip_income 
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
	and stats_date<=date_add(run_date,interval -end_day day)
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
		,0 as conversions
	FROM gzdw2024.ad_platform_data.dws_ad_country_daily_reports
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
	and stats_date <= DATE_SUB(run_date, INTERVAL end_day DAY)
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
		,0 as conversions
	FROM gzdw2024.cost_data.dws_asa_cost_daily
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
	and stats_date<=date_add(run_date,interval -end_day day)
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
		,conversions
	FROM gzdw2024.cost_data.dws_ga_cost_daily
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL history_day DAY)
	and stats_date<=date_add(run_date,interval -end_day day)
	AND campaign_name='TOTAL'
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




delete `gzdw2024.gz_bi.dws_app_country_daily_reports`
where stats_date>=date_add(run_date,interval -(history_day+100) day)
	and stats_date<=date_add(run_date,interval -end_day day)
  and package_name not like 'fb%'
  and package_name  in ('co.springtech.scanner','com.textNumber.phone');


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
	,sum(conversions) as conversions
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
		,0 as conversions
	FROM `gzdw2024.gz_bi.dws_app_daily_reports` 
	WHERE stats_date>=date_add(run_date,interval -(history_day+100) day)
	and stats_date<=date_add(run_date,interval -end_day day)
   and package_name  in ('co.springtech.scanner','com.textNumber.phone')
	--and app_name is not null
	union all 
	------googlesheet订阅收入
		SELECT
		PARSE_DATE('%Y-%m-%d', string_field_0) AS  stats_date
		,'com.textNumber.phone' as package_name
		--,app_name
		,'TOTAL' AS  country_code
		,0 as active_uv
		,0 as new_uv
		,0 as ratio
		,sum(safe_CAST(REPLACE(REPLACE(string_field_1, '$', ''), ',', '') AS FLOAT64)) vip_revenue
		,0 as ad_revenue
		,0 as asa_cost
		,0 as ga_cost
		,0 as new_retain_uv
		,0 as conversions
	FROM  `gzdw2024.appstoreconnect.p_sales_textnumber` 
	where string_field_0!='stats_date'
	and PARSE_DATE('%Y-%m-%d', string_field_0) >= date_add(run_date,interval -(history_day+100) day)
	and PARSE_DATE('%Y-%m-%d', string_field_0) <= date_add(run_date,interval -end_day day)
      group by stats_date,country_code
      union all 
	------googlesheet订阅收入
		SELECT
		PARSE_DATE('%Y-%m-%d', string_field_0) AS  stats_date
		,'co.springtech.scanner' as package_name
		--,app_name
		,'TOTAL' AS  country_code
		,0 as active_uv
		,0 as new_uv
		,0 as ratio
		,sum(safe_CAST(REPLACE(REPLACE(string_field_1, '$', ''), ',', '') AS FLOAT64)) vip_revenue
		,0 as ad_revenue
		,0 as asa_cost
		,0 as ga_cost
		,0 as new_retain_uv
		,0 as conversions
	FROM  `gzdw2024.appstoreconnect.p_sales_scanner_chenlan` 
	where string_field_0!='stats_date'
	and PARSE_DATE('%Y-%m-%d', string_field_0) >= date_add(run_date,interval -(history_day+100) day)
	and PARSE_DATE('%Y-%m-%d', string_field_0) <= date_add(run_date,interval -end_day day)
      group by stats_date,country_code
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
		,0 as conversions
	FROM gzdw2024.ad_platform_data.dws_ad_country_daily_reports
	WHERE stats_date >= DATE_SUB(run_date, INTERVAL (history_day+100) DAY)
	and stats_date <= DATE_SUB(run_date, INTERVAL end_day DAY)
	and package_name  in ('com.textNumber.phone')
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



delete `gzdw2024.gz_bi.dws_daily_app_reports`
where stats_date>=date_add(run_date,interval -(history_day+20) day)
and stats_date<=date_add(run_date,interval -end_day day);



insert `gzdw2024.gz_bi.dws_daily_app_reports`


--drop table if exists  `gzdw2024.gz_bi.dws_daily_app_reports`;
--create table `gzdw2024.gz_bi.dws_daily_app_reports`
-- PARTITION BY stats_date as 
    SELECT
        stats_date
        ,package_name
        ,SUM(revenue) as revenue
        ,SUM(vip_revenue) as vip_revenue
        ,SUM(ad_revenue) as ad_revenue
        ,SUM(new_users) as new_users
        ,SUM(new_users_organic) as new_users_organic
        ,SUM(active_users) as active_users
        ,SUM(cost) as cost 
    FROM
        (
        SELECT 
            stats_date
            ,package_name
            ,total_revenue revenue
            ,vip_revenue
            ,ad_revenue
            ,new_uv as new_users
            ,0 as new_users_organic
            ,active_uv as active_users
            ,total_cost as cost 
        FROM gzdw2024.gz_bi.dws_app_country_daily_reports
        WHERE 1=1
        and (
        (stats_date>='2024-09-01') 
        or (  package_name in ('fb.ai.avatar.puzzle',
        'com.talknow.free.text.me.now.second.phone.number.burner.app',
        'fb.zp',
        'fb.otme.fate.quest',
         'fb.fruit.bubble',
         'fb.ai.aha',
         'fb.block.juggle',
         'fb.bubble.shoot.pro',
         'fb.save.dog',
         'fb.egg.bubble',
         'com.textNumber.phone'))
        )
        and country_code='TOTAL'
        and stats_date>=date_add(run_date,interval -(history_day+20) day)
        and stats_date<=date_add(run_date,interval -end_day day)
        union all 
        SELECT 
            stats_date
            ,package_name
            ,0 as  revenue
            ,0 as vip_revenue
            ,0 as ad_revenue
            ,0 as   new_users
            ,new_uv as new_users_organic
            ,0 as active_users
            ,0 as cost 
        FROM `gzdw2024.scanner_01_basic.dws_user_active_report`
        WHERE stats_date >='2024-09-01'
        and country_code='TOTAL'
        and traffic_source_type='nature'
         and stats_date>=date_add(run_date,interval -(history_day+20) day)
        and stats_date<=date_add(run_date,interval -end_day day)
           union all 
        SELECT 
            stats_date
            ,package_name
            ,0 as  revenue
            ,0 as vip_revenue
            ,0 as ad_revenue
            ,0 as   new_users
            ,new_uv as new_users_organic
            ,0 as active_users
            ,0 as cost 
        FROM `gzdw2024.downloader_01_basic.dws_user_active_report`
        WHERE stats_date >='2024-09-01'
        and country_code='TOTAL'
        and traffic_source_type='nature'
         and stats_date>=date_add(run_date,interval -(history_day+20) day)
        and stats_date<=date_add(run_date,interval -end_day day)
           union all 
        SELECT 
            stats_date
            ,package_name
            ,0 as  revenue
            ,0 as vip_revenue
            ,0 as ad_revenue
            ,0 as   new_users
            ,new_uv as new_users_organic
            ,0 as active_users
            ,0 as cost 
        FROM `gzdw2024.recorder_lite_01_basic.dws_user_active_report`
        WHERE stats_date >='2024-09-01'
        and country_code='TOTAL'
        and traffic_source_type='nature'
         and stats_date>=date_add(run_date,interval -(history_day+20) day)
        and stats_date<=date_add(run_date,interval -end_day day)
           union all 
        SELECT 
            stats_date
            ,package_name
            ,0 as  revenue
            ,0 as vip_revenue
            ,0 as ad_revenue
            ,0 as   new_users
            ,new_uv as new_users_organic
            ,0 as active_users
            ,0 as cost 
        FROM `gzdw2024.recorder_pro_01_basic.dws_user_active_report`
        WHERE stats_date >='2024-09-01'
        and country_code='TOTAL'
        and traffic_source_type='nature'
         and stats_date>=date_add(run_date,interval -(history_day+20) day)
        and stats_date<=date_add(run_date,interval -end_day day)
           union all 
        SELECT 
            stats_date
            ,package_name
            ,0 as  revenue
            ,0 as vip_revenue
            ,0 as ad_revenue
            ,0 as   new_users
            ,new_uv as new_users_organic
            ,0 as active_users
            ,0 as cost 
        FROM `gzdw2024.vidma_editor_android_01_basic.dws_user_active_report`
        WHERE stats_date >='2024-09-01'
        and country_code='TOTAL'
        and traffic_source_type='nature'
         and stats_date>=date_add(run_date,interval -(history_day+20) day)
        and stats_date<=date_add(run_date,interval -end_day day)
              union all 
        SELECT 
            stats_date
            ,package_name
            ,0 as  revenue
            ,0 as vip_revenue
            ,0 as ad_revenue
            ,0 as   new_users
            ,new_uv as new_users_organic
            ,0 as active_users
            ,0 as cost 
        FROM `gzdw2024.vidma_editor_ios_01_basic.dws_user_active_report`
        WHERE stats_date >='2024-09-01'
        and country_code='TOTAL'
        and traffic_source_type='nature'
         and stats_date>=date_add(run_date,interval -(history_day+20) day)
        and stats_date<=date_add(run_date,interval -end_day day)
               union all 
        SELECT 
            event_date stats_date
            ,package_name
            ,0 as  revenue
            ,0 as vip_revenue
            ,0 as ad_revenue
            ,0 as   new_users
            ,new_uv - new_ad_uv as new_users_organic
            ,0 as active_users
            ,0 as cost 
        FROM `gzdw2024.fbgame_01_basic.dws_common_game_user_active_report`
        WHERE event_date >='2024-09-01'
        and country_code='TOTAL'
        and platform='TOTAL'
         and event_date>=date_add(run_date,interval -(history_day+20) day)
        and event_date<=date_add(run_date,interval -end_day day)
        /*
        union all 
        -----老产品：2024年9月1日之前：
            select  
            cast(stats_date as date) stats_date
            ,package_name
            ,sum(revenue) revenue
            ,sum(revenue_vip) vip_revenue
            ,sum(revenue_ad) ad_revenue
            ,0 as   new_users
            ,0 as new_users_organic
            ,0 as active_users
            ,0 as cost 
            from `gzdata.revenue.prefect_daily_revenue_*`
            where _TABLE_SUFFIX between '20240101' and  '20240831'
            group by stats_date
            ,package_name

            ----2024年7-8月数据 缺失数据补充
            union all 
            SELECT 
                stats_date
                ,package_name
                ,sum(revenue_usd) revenue
                ,sum(revenue_usd) as vip_revenue
                ,0 as ad_revenue
                ,0 as   new_users
                ,0 as new_users_organic
                ,0 as active_users
                ,0 as cost
            FROM `gzdw2024.appstoreconnect.p_sales_vidma`
            WHERE begin_date between '2024-07-17' and  '2024-08-31'
            group by package_name,stats_date
            union all 
            select 
                PARSE_DATE('%Y%m%d', stats_date) stats_date
                ,package_name
                ,0 as  revenue
                ,0 as vip_revenue
                ,0 as ad_revenue
                ,new_users
                ,new_users_organic
                ,active_users
                ,0 as cost
            from `gzdata.firebase.prefect_total_metrics_20*`
            where _TABLE_SUFFIX>='240101'
            and country='TOTAL'
            and _TABLE_SUFFIX<='240831'
            union all 
            select 
            event_date
            ,package_name
            ,0 as  revenue
            ,0 as vip_revenue
            ,0 as ad_revenue
            ,0 as   new_users
            ,0 as new_users_organic
            ,0 as active_users
            ,sum(cost) cost
            from hzdw2024.delivery.dws_delivery_cost_di
            where event_date>='2024-01-01'
            and event_date<='2024-08-31'
            and package_name in (SELECT 
                        package_name 
                    FROM `gzdw2024.gz_dim.app_info` 
                    group by package_name)
            group by event_date,package_name
            union all 
            SELECT 
            stats_date
            ,package_name
            ,revenue
            ,vip_revenue
            ,ad_revenue
            ,new_users
            ,new_users_organic
            ,active_users
            ,cost
            FROM `gzdw2024.gz_bi.dws_downloader_daily_app_reports`
            where stats_date>='2024-01-01'
            and stats_date<='2024-08-31'
            */
        )a 
group by stats_date,package_name;



end;
