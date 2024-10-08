CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fbaiavatar_bi_roi_event`(run_date DATE, history_day INT64, hitory_end_day INT64)
begin

delete `gzdw2024.fbgame_03_bi.dws_fb_cost_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -hitory_end_day day);

insert `gzdw2024.fbgame_03_bi.dws_fb_cost_daily_reports`
	SELECT
		stats_date
		,'fb.ai.avatar.puzzle' as package_name
		,platform
		,country_code
		,sum(cost) as cost
		,sum(install) as install
	FROM
		(
		SELECT
			stats_date
			,array['TOTAL',platform] as platform
			,campaign_name
			,array['TOTAL',country] as country_code
			,cost
			,install
		FROM
			(
			SELECT 
				stats_date
				,case when lower(campaign_name) like '%ios%' then 'iOS' else 'Android' end as platform
				,campaign_name
				,country
			  	,sum(cost) as cost 
			  	,sum(install) as install
			 from 
			   (
				SELECT 
					date(stats_date) as stats_date
					,campaign_name
					,country
					,MAX(safe_cast(spend as float64)) as cost 
					,0 as install
				FROM `fb-ai-avatar-puzzle.analytics_439907691.delivery_fb_country_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -hitory_end_day day) as string),'-','')
				group by campaign_name,country ,stats_date
				union all 
				SELECT 
					date(stats_date) as stats_date
					,campaign_name
					,country
					,0 as cost
					,MAX(safe_cast(action_count as float64)) as install 
				FROM `fb-ai-avatar-puzzle.analytics_439907691.delivery_fb_country_*` 
				WHERE action_type='mobile_app_install'
				and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -hitory_end_day day) as string),'-','')
				group by campaign_name,country ,stats_date
			  )a 
			   group by stats_date,campaign_name,country,platform
			)b 
		)c 
		,UNNEST(country_code) as country_code
		,UNNEST(platform) as platform
		group by stats_date
		,platform
		,country_code
		order by stats_date desc ,cost desc;




delete `gzdw2024.fbgame_03_bi.dws_fb_ad_revenue_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -hitory_end_day day);

insert `gzdw2024.fbgame_03_bi.dws_fb_ad_revenue_daily_reports`

				SELECT 
					parse_date('%Y%m%d',_table_suffix) as stats_date
					,'fb.ai.avatar.puzzle' as package_name
					,'TOTAL' as platform
					,'TOTAL' as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_hour_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -hitory_end_day day) as string),'-','')
				--and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date
				union all 
					SELECT 
					date(start_timestamp) as stats_date
					,'fb.ai.avatar.puzzle' as package_name
					,case when platform='ios' then 'iOS' when platform='android' then 'Android' else platform end  as platform
					,upper(country) as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_detail_day_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -hitory_end_day day) as string),'-','')
				and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date,platform,country_code
				union all 
				SELECT 
					date(start_timestamp) as stats_date
						,'fb.ai.avatar.puzzle' as package_name
					,case when platform='ios' then 'iOS' when platform='android' then 'Android' else platform end  as platform
					,'TOTAL' as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_detail_day_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -hitory_end_day day) as string),'-','')
				and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date,platform,country_code
					union all 
					SELECT 
					date(start_timestamp) as stats_date
						,'fb.ai.avatar.puzzle' as package_name
					,'TOTAL'  as platform
					,upper(country) as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_detail_day_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -hitory_end_day day) as string),'-','')
				and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date,platform,country_code;






delete `gzdw2024.fbgame_03_bi.dws_fb_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -hitory_end_day day);

insert `gzdw2024.fbgame_03_bi.dws_fb_daily_reports`
	SELECT
			stats_date
			,package_name
			,platform
			,country_code
			,active_uv
			,new_uv
			,retain_uv2
			,cost
			,install
			,requests
			,filled_requests
			,impressions
			,revenue
			,clicks
			,arpu
			,last_arpu
			,case when arpu is null or arpu<0.00001 then last_arpu*install else arpu*install end as first_day_revenue
	FROM
		(
		SELECT
			stats_date
			,package_name
			,platform
			,country_code
			,active_uv
			,new_uv
			,retain_uv2
			,cost
			,install
			,requests
			,filled_requests
			,impressions
			,revenue
			,clicks
			,safe_divide(revenue,active_uv) as arpu 
			,
				safe_divide(lag(revenue)  over(PARTITION by package_name,platform,country_code order by stats_date )
					,lag(active_uv)  over(PARTITION by package_name,platform,country_code order by stats_date )) as last_arpu
		FROM  
			(
			SELECT
				stats_date
				,'fb.ai.avatar.puzzle' as package_name
				,platform
				,country_code
				,sum(active_uv) as active_uv
				,sum(new_uv) as new_uv
				,sum(retain_uv2) as retain_uv2
				,sum(cost) as cost
				,sum(install) as install
				,sum(requests) as requests
				,sum(filled_requests) as filled_requests
				,sum(impressions) as impressions
				,sum(revenue) as revenue
				,sum(clicks) as clicks
			FROM
				(
			    SELECT 
					event_date as stats_date
					,platform
					,country_code	 
					,active_uv
					,new_uv
					,retain_uv2	 
					,0 as cost
			    	,0 as install
			    	,0 as requests
			    	,0 as filled_requests
			    	,0 as impressions
			    	,0 as revenue
			    	,0 as clicks
			    FROM `fb-ai-avatar-puzzle.fb_dw.dws_user_active_report` 
			    WHERE event_date >= date_add(run_date,interval -history_day day)
			    and event_date <= date_add(run_date,interval -hitory_end_day day)
			    union all
			    SELECT
			    	stats_date
			    	,platform
			    	,country_code
			    	,0 as active_uv
			    	,0 as new_uv
			    	,0 as retain_uv2
			    	,cost
			    	,install
			    	,0 as requests
			    	,0 as filled_requests
			    	,0 as impressions
			    	,0 as revenue
			    	,0 as clicks
			    FROM `gzdw2024.fbgame_03_bi.dws_fb_cost_daily_reports`
			      WHERE stats_date >= date_add(run_date,interval -history_day day)
			    and stats_date <= date_add(run_date,interval -hitory_end_day day)
			      union all
			    SELECT
			    	stats_date
			    	,platform
			    	,country_code
			    	,0 as active_uv
			    	,0 as new_uv
			    	,0 as retain_uv2
			    	,0 as cost
			    	,0 as install
			    	,requests
			    	,filled_requests
			    	,impressions
			    	,revenue
			    	,clicks
			    FROM `gzdw2024.fbgame_03_bi.dws_fb_ad_revenue_daily_reports`
			      WHERE stats_date >= date_add(run_date,interval -history_day day)
			    and stats_date <= date_add(run_date,interval -hitory_end_day day)
			    )a 
			    group by stats_date,platform,country_code
			    )b 
    )c;


delete `gzdw2024.gz_bi.dws_app_country_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -hitory_end_day day)
and package_name  in ('fb.ai.avatar.puzzle','fb.ai.avatar');


insert `gzdw2024.gz_bi.dws_app_country_daily_reports`
	SELECT
	stats_date
	,package_name
	,'fb_ai_avatar' as app_name
	,country_code
	,sum(active_uv) as active_uv
	,sum(new_uv) as new_uv
	,safe_divide(sum(retain_uv2),sum(new_uv)) as ratio
	,sum(revenue) as total_revenue
	,sum(cost) as total_cost
	,0 as vip_revenue
	,sum(revenue) as ad_revenue
	,0 as asa_cost
	,0 as ga_cost
	,sum(retain_uv2) as new_retain_uv
	,sum(cost) as fb_cost
FROM
	`gzdw2024.fbgame_03_bi.dws_fb_daily_reports`
	 WHERE stats_date >= date_add(run_date,interval -history_day day)
    and stats_date <= date_add(run_date,interval -hitory_end_day day)
    and platform='TOTAL'
    group by stats_date,app_name,package_name,country_code;
end;
