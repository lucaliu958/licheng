CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fbaiavatar_bi_roi_event`(run_date DATE, history_day INT64, hitory_end_day INT64)
begin

delete `gzdw2024.fbgame_03_bi.dws_fb_cost_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -hitory_end_day day)
and stats_date>='2024-09-08';



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
		where stats_date>='2024-09-08'
		group by stats_date
		,platform
		,country_code
		order by stats_date desc ,cost desc;




delete `gzdw2024.fbgame_03_bi.dws_fb_ad_revenue_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -hitory_end_day day)
and stats_date>='2024-09-08';

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
				UNION ALL 
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
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_day_*` 
				where _TABLE_SUFFIX >='20240908'
				and _TABLE_SUFFIX <='20240923'
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




delete  `gzdw2024.fbgame_03_bi.dws_fb_rention_roi_reports`
where stats_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 9 DAY)
and stats_date <= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 1 DAY);


insert gzdw2024.fbgame_03_bi.dws_fb_rention_roi_reports
with a as (						
		SELECT
			platform
			,country_code
			,avg_ratio2
			,avg_ratio3
			,avg_ratio4
			,avg_ratio5
			,avg_ratio6
			,avg_ratio7
			,avg_ratio8
			,avg_ratio2 + avg_ratio3  as total_bili_3
			,safe_divide(avg_ratio2 + avg_ratio3  ,avg_ratio2) as bili_3
			,avg_ratio2 + avg_ratio3 + avg_ratio4 + avg_ratio5 + avg_ratio6 + avg_ratio7 as total_bili_7
			,safe_divide(avg_ratio2 + avg_ratio3 + avg_ratio4 + avg_ratio5 + avg_ratio6 + avg_ratio7 ,avg_ratio2) as bili_7
		FROM
			(
			SELECT
				platform
				,country_code
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 3 DAY) then ratio_2 else null end)	as avg_ratio2
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 4 DAY) then ratio_3 else null end)	as avg_ratio3
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 5 DAY) then ratio_4 else null end)	as avg_ratio4
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 6 DAY) then ratio_5 else null end)	as avg_ratio5
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 7 DAY) then ratio_6 else null end)	as avg_ratio6
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 8 DAY) then ratio_7 else null end)	as avg_ratio7				
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 9 DAY) then ratio_8 else null end)	as avg_ratio8
			FROM 
				(
				SELECT 
					platform
					,event_date
					,country_code	 
					,new_uv
					,retain_uv2	 
					,retain_uv3 
			    	,retain_uv4 
			    	,retain_uv5 
			    	,retain_uv6 
			    	,retain_uv7 
			    	,retain_uv8 
			    	,safe_divide(retain_uv2,new_uv) as ratio_2
			    	,safe_divide(retain_uv3,new_uv) as ratio_3
			    	,safe_divide(retain_uv4,new_uv) as ratio_4
			    	,safe_divide(retain_uv5,new_uv) as ratio_5
			    	,safe_divide(retain_uv6,new_uv) as ratio_6
			    	,safe_divide(retain_uv7,new_uv) as ratio_7
			    	,safe_divide(retain_uv8,new_uv) as ratio_8
			    FROM `fb-ai-avatar-puzzle.fb_dw.dws_user_active_report` 
			    WHERE event_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 9 DAY)
			    and event_date <= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 1 DAY)
				--and platform='TOTAL'
				--and country_code='TOTAL'
        		order by event_date
				)a 
				group by platform,country_code
			)b 
		),
		b as (
				SELECT 
					event_date as stats_date
					,platform
					,country_code	 
					,active_uv
					,new_uv
					,new_ad_uv
					,new_liebian_uv
					,retain_uv2	 
					,retain_uv3 
			    	,retain_uv4 
			    	,retain_uv5 
			    	,retain_uv6 
			    	,retain_uv7 
			    	,retain_uv8 
			    	,safe_divide(retain_uv2,new_uv) as ratio_2
			    	,safe_divide(retain_uv3,new_uv) as ratio_3
			    	,safe_divide(retain_uv4,new_uv) as ratio_4
			    	,safe_divide(retain_uv5,new_uv) as ratio_5
			    	,safe_divide(retain_uv6,new_uv) as ratio_6
			    	,safe_divide(retain_uv7,new_uv) as ratio_7
			    	,safe_divide(retain_uv8,new_uv) as ratio_8
			    FROM `fb-ai-avatar-puzzle.fb_dw.dws_user_active_report` 
			    WHERE event_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 9 DAY)
			    and event_date <= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 1 DAY)
				--and platform='TOTAL'
				--and country_code='TOTAL'
				order by stats_date
				)
		SELECT
			stats_date
			,c.platform
			,c.country_code	
			,active_uv
			,new_uv
			,new_ad_uv
			,new_liebian_uv
			,new_ad_liebian_uv 
			,case when stats_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 2 DAY)  then total_bili_3 
			  when stats_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 3 DAY)  then ratio_2*bili_3
			    else ratio_2+ratio_3 end as total_bili_3
			,case when stats_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 2 DAY)  then total_bili_7 
			  when stats_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 8 DAY)  then ratio_2*bili_7
			    else ratio_2+ratio_3 + ratio_4  + ratio_5  + ratio_6  + ratio_7 end as total_bili_7
			 ,ratio_2
			,ratio_3
			,ratio_4
			,ratio_5
			,ratio_6
			,ratio_7
			,ratio_8
		FROM 	
		(
		SELECT
			stats_date
			,platform
			,country_code	
			,active_uv
			,new_uv
			,new_ad_uv
			,new_liebian_uv
			,new_ad_uv+new_liebian_uv as new_ad_liebian_uv 
			,ratio_2
			,ratio_3
			,ratio_4
			,ratio_5
			,ratio_6
			,ratio_7
			,ratio_8
		FROM b  
		)c 
		left join 
		(
		SELECT
			platform
			,country_code
			,avg_ratio2
			,avg_ratio3
			,avg_ratio4
			,avg_ratio5
			,avg_ratio6
			,avg_ratio7
			,avg_ratio8
			,total_bili_3
			,bili_3
			,total_bili_7
			,bili_7
		FROM a 
			)d 
		on c.platform=d.platform
		and c.country_code=d.country_code
		order by stats_date desc ;

delete  `gzdw2024.fbgame_03_bi.dws_fb_daily_roi_total_reports`
where  stats_date >= date_add(run_date,interval -history_day day)
  and stats_date <= date_add(run_date,interval -hitory_end_day day);



insert `gzdw2024.fbgame_03_bi.dws_fb_daily_roi_total_reports`
with a as (
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
	,max_stats_date
	,new_ad_uv
	,new_liebian_uv
	,new_ad_liebian_uv 
	,total_bili_3
	,total_bili_7
	,arpu 
	,new_arpu
	,new_ratio
	,lag(new_ratio) over(partition by platform,country_code order by stats_date) as last_new_ratio
	,lag(new_arpu) over(partition by platform,country_code order by stats_date) as last_new_arpu
	,lag(arpu) over(partition by platform,country_code order by stats_date) as last_arpu
	,lag(total_bili_3) over(partition by platform,country_code order by stats_date) as last_total_bili_3
	,lag(total_bili_7) over(partition by platform,country_code order by stats_date) as last_total_bili_7
	--,new_ad_liebian_uv*new_arpu as first_day_revenue
	--,new_ad_liebian_uv*new_arpu*(1+total_bili_3) as first_3day_revenue
	--,new_ad_liebian_uv*new_arpu*(1+total_bili_3) + new_ad_liebian_uv*arpu*(total_bili_7- total_bili_3) as first_7day_revenue
FROM
	(
 	SELECT
 		a.stats_date
 		,a.package_name
 		,a.platform
 		,a.country_code
		,a.active_uv
		,a.new_uv
		,retain_uv2
		,cost
		,install
		,requests
		,filled_requests
		,impressions
		,revenue
		,clicks
		,max_stats_date
		,new_ad_uv
		,new_liebian_uv
		,new_ad_liebian_uv 
		,total_bili_3
		,total_bili_7
    	,arpu
		,case when new_bili>=0.8 then arpu*1.03 
		when new_bili>=0.7 and new_bili<0.8 then arpu*1.13
		when new_bili>=0.5 and new_bili<0.7 then arpu*1.23 
		when new_bili>=0.3 and new_bili<0.5 then arpu*1.4 
		when  new_bili<0.3 then arpu*1.5 else arpu end as new_arpu
		,safe_divide(new_ad_liebian_uv,install) as new_ratio
	FROM
		(
		SELECT
			stats_date
			,package_name
			,platform
			,country_code
			,active_uv
			,new_uv
			,safe_divide(new_uv,active_uv) as new_bili
			,arpu
			,retain_uv2
			,cost
			,install
			,requests
			,filled_requests
			,impressions
			,revenue
			,clicks
			,max(stats_date) over() as max_stats_date
		FROM  `gzdw2024.fbgame_03_bi.dws_fb_daily_reports`
		WHERE 1=1
		--and platform='TOTAL'
		--and country_code='TOTAL'
		and stats_date >= date_add(run_date,interval -history_day day)
    	and stats_date <= date_add(run_date,interval - hitory_end_day day)
		)a 
		left join 
		(
		SELECT
			stats_date
			,platform
			,country_code	
			,active_uv
			,new_uv
			,new_ad_uv
			,new_liebian_uv
			,new_ad_liebian_uv 
			,total_bili_3
			,total_bili_7
			 ,ratio_2
			,ratio_3
			,ratio_4
			,ratio_5
			,ratio_6
			,ratio_7
			,ratio_8
		FROM `gzdw2024.fbgame_03_bi.dws_fb_rention_roi_reports`
		)b 
		on a.stats_date=b.stats_date
		and a.platform=b.platform
		and a.country_code=b.country_code
	)c 
	order by stats_date desc
	)
	SELECT 
		* 
	,case when new_ad_uv is not null  then  new_ad_liebian_uv*new_arpu  
		else install*last_new_ratio*last_new_arpu end as first_day_revenue
	,case when new_ad_uv is not null then  new_ad_liebian_uv*new_arpu*(1+total_bili_3)
	else install*last_new_ratio*last_new_arpu*(1+ last_total_bili_3) end as first_3day_revenue

	,case when new_ad_uv is not null then  new_ad_liebian_uv*new_arpu*(1+total_bili_3) + new_ad_liebian_uv*arpu*(total_bili_7- total_bili_3)
	else install*last_new_ratio*last_new_arpu*(1+last_total_bili_3) + install*last_new_ratio*last_arpu*(last_total_bili_7- last_total_bili_3) end as first_7day_revenue

    FROM a 
	WHERE 1=1;




end;
