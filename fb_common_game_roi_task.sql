
CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fb_common_game_roi_task`(run_date DATE, history_day INT64, history_end_day INT64)
begin
			------google sheet成本数据
			CREATE OR REPLACE VIEW `gzdw2024.cost_data.all_external_data` AS
			SELECT string_field_0,string_field_1,string_field_3,string_field_6 ,'fb.otme.fate.quest'  as package_name
			FROM `gzdw2024.cost_data.fb_game_fq_cost_data`
			UNION ALL
			SELECT string_field_0,string_field_1,string_field_3,string_field_6 ,'fb.zp'  as package_name
			FROM `gzdw2024.cost_data.fb_game_slt_cost_data`
			UNION ALL
			SELECT string_field_0,string_field_1,string_field_3,string_field_6 ,'fb.ai.aha'  as package_name
			FROM `gzdw2024.cost_data.fb_game_aha_cost_data`
			UNION ALL
			SELECT string_field_0,string_field_1,string_field_3,string_field_6 ,'fb.fruit.bubble'  as package_name
			FROM `gzdw2024.cost_data.fb_game_bbpf_cost_data`
			UNION ALL
			SELECT string_field_0,string_field_1,string_field_3,string_field_6,'fb.ai.avatar.puzzle'  as package_name
			FROM `gzdw2024.cost_data.fb_game_oho_cost_data`
			UNION ALL
			SELECT string_field_0,string_field_1,string_field_3,string_field_6,'fb.save.dog'  as package_name
			FROM `gzdw2024.cost_data.fb_game_dog_cost_data`
			UNION ALL
			SELECT string_field_0,string_field_1,string_field_3,string_field_6,'fb.egg.bubble'  as package_name
			FROM `gzdw2024.cost_data.fb_game_egg_cost_data`;


			CREATE OR REPLACE VIEW `gzdw2024.cost_data.all_external_data_platform` AS
			SELECT string_field_0,string_field_1,string_field_3,string_field_6 ,'fb.save.dog'  as package_name
			FROM `gzdw2024.cost_data.fb_game_platform_dog_cost_data`;




			delete `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_google_sheet`
			where stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			and stats_date>='2024-11-19';


			insert `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_google_sheet`
		    --drop table if exists `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_google_sheet`;
			--create table `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_google_sheet`
			--PARTITION BY stats_date as 
			SELECT  
				PARSE_DATE('%Y-%m-%d', string_field_0) AS stats_date
				,package_name
				,'TOTAL' AS platform
				,'TOTAL' AS country_code
				,sum(safe_CAST(REPLACE(REPLACE(string_field_3, '$', ''), ',', '') AS FLOAT64)) AS cost
				,sum(safe_CAST(REPLACE(REPLACE(string_field_6, '$', ''), ',', '') AS FLOAT64)) AS install
			FROM `gzdw2024.cost_data.all_external_data` 
			where length(string_field_0)>6
			and PARSE_DATE('%Y-%m-%d', string_field_0) >= date_add(run_date,interval -history_day day)
			and PARSE_DATE('%Y-%m-%d', string_field_0) <= date_add(run_date,interval -history_end_day day)
			and  PARSE_DATE('%Y-%m-%d', string_field_0)>='2024-11-19'
			group by PARSE_DATE('%Y-%m-%d', string_field_0),package_name
			union all 
			SELECT  
				PARSE_DATE('%Y-%m-%d', string_field_0) AS stats_date
				,package_name
				,'TOTAL' AS platform
				,upper(string_field_1) AS country_code
				,sum(safe_CAST(REPLACE(REPLACE(string_field_3, '$', ''), ',', '') AS FLOAT64)) AS cost
				,sum(safe_CAST(REPLACE(REPLACE(string_field_6, '$', ''), ',', '') AS FLOAT64)) AS install
			FROM `gzdw2024.cost_data.all_external_data` 
			where length(string_field_0)>6
			and PARSE_DATE('%Y-%m-%d', string_field_0) >= date_add(run_date,interval -history_day day)
			and PARSE_DATE('%Y-%m-%d', string_field_0) <= date_add(run_date,interval -history_end_day day)
			and  PARSE_DATE('%Y-%m-%d', string_field_0)>='2024-11-19'
			group by PARSE_DATE('%Y-%m-%d', string_field_0),upper(string_field_1),package_name
			union all 
			SELECT  
				PARSE_DATE('%Y-%m-%d', string_field_0) AS stats_date
				,package_name
				,case when lower(string_field_1) like '%android%' then 'Android'
				when lower(string_field_1) like '%iphone%' then 'iOS' 
				when lower(string_field_1) like '%ios%' then 'iOS' 
				else 'web' end AS platform
				,'TOTAL' AS country_code
				,sum(safe_CAST(REPLACE(REPLACE(string_field_3, '$', ''), ',', '') AS FLOAT64)) AS cost
				,sum(safe_CAST(REPLACE(REPLACE(string_field_6, '$', ''), ',', '') AS FLOAT64)) AS install
			FROM `gzdw2024.cost_data.all_external_data_platform` 
			where length(string_field_0)>6
			and PARSE_DATE('%Y-%m-%d', string_field_0) >= date_add(run_date,interval -history_day day)
			and PARSE_DATE('%Y-%m-%d', string_field_0) <= date_add(run_date,interval -history_end_day day)
			and  PARSE_DATE('%Y-%m-%d', string_field_0)>='2024-11-19'
			group by PARSE_DATE('%Y-%m-%d', string_field_0),platform,package_name,country_code;



			--------api成本数据
		  EXECUTE IMMEDIATE FORMAT("""
		    CREATE OR REPLACE VIEW `gzdw2024.cost_data.all_api_data` AS
		    SELECT stats_date, campaign_name, country, spend, action_count, action_type, 'fb.ai.avatar.puzzle' AS package_name
		    FROM `fb-ai-avatar-puzzle.analytics_439907691.delivery_fb_country_*` 
		    WHERE _TABLE_SUFFIX >= '%s'
		      AND _TABLE_SUFFIX <= '%s'
		    UNION ALL
		    SELECT stats_date, campaign_name, country, spend, action_count, action_type, 'fb.zp' AS package_name
		    FROM `fb-ai-avatar-puzzle.analytics_439907691.delivery_fb_country_SLT_*` 
		    WHERE _TABLE_SUFFIX >= '%s'
		      AND _TABLE_SUFFIX <= '%s';
		  """,
		    REPLACE(CAST(DATE_ADD(run_date, INTERVAL -history_day DAY) AS STRING), '-', ''),
		    REPLACE(CAST(DATE_ADD(run_date, INTERVAL -history_end_day DAY) AS STRING), '-', ''),
		    REPLACE(CAST(DATE_ADD(run_date, INTERVAL -history_day DAY) AS STRING), '-', ''),
		    REPLACE(CAST(DATE_ADD(run_date, INTERVAL -history_end_day DAY) AS STRING), '-', '')
		  );


			delete `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_api`
			where stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			and stats_date>='2024-11-19';


			insert `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_api`
		    --drop table if exists `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_api`;
			--create table `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_api`
			--PARTITION BY stats_date as 
				SELECT
					stats_date
					,package_name
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
						,package_name
					FROM
						(
						SELECT 
							stats_date
							,package_name
							,case when lower(campaign_name) like '%ios%' then 'iOS' else 'Android' end as platform
							,campaign_name
							,country
						  	,sum(cost) as cost 
						  	,sum(install) as install
						 from 
						   (
							SELECT 
								date(stats_date) as stats_date
								,package_name
								,campaign_name
								,country
								,MAX(safe_cast(spend as float64)) as cost 
								,0 as install
							FROM `gzdw2024.cost_data.all_api_data` 
							group by campaign_name,country ,stats_date,package_name
							union all 
							SELECT 
								date(stats_date) as stats_date
								,package_name
								,campaign_name
								,country
								,0 as cost
								,MAX(safe_cast(action_count as float64)) as install 
							FROM `gzdw2024.cost_data.all_api_data` 
							WHERE action_type='mobile_app_install'
						
							group by campaign_name,country ,stats_date,package_name
						  )a 
						   group by stats_date,campaign_name,country,platform,package_name
						)b 
					)c 
					,UNNEST(country_code) as country_code
					,UNNEST(platform) as platform
					where stats_date>='2024-09-08'
					group by stats_date
					,platform
					,campaign_name
					,country_code
					,package_name;



			-------成本数据汇总
			delete `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports`
			where stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			and stats_date>='2024-11-19';

			insert `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports`
		    --drop table if exists `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports`;
			--create table `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports`
			--PARTITION BY stats_date as 
			SELECT
				stats_date
				,package_name
				,platform
				,country_code
				,max(cost) as cost 
				,max(install) as install
			FROM
				(
				SELECT
					stats_date
					,package_name
					,platform
					,country_code
					,max(cost) as cost 
					,max(install) as install
				FROM `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_google_sheet`
				WHERE 1=1
				and stats_date>=date_add(run_date,interval -history_day day)
				and  stats_date<=date_add(run_date,interval -history_end_day day)
				group by stats_date
					,package_name
					,platform
					,country_code
				union all 
				SELECT
					stats_date
					,package_name
					,platform
					,country_code
					,max(cost) as cost 
					,max(install) as install
				FROM `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports_api`
				WHERE 1=1
				and stats_date>=date_add(run_date,interval -history_day day)
				and  stats_date<=date_add(run_date,interval -history_end_day day)
				group by stats_date
					,package_name
					,platform
					,country_code
					)a 
				group by stats_date,package_name,platform,country_code;




--------FB收入数据

delete `gzdw2024.fbgame_03_bi.dws_fb_common_game_ad_revenue_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -history_end_day day)
and stats_date>='2024-09-08';

insert `gzdw2024.fbgame_03_bi.dws_fb_common_game_ad_revenue_daily_reports`
--create table  `gzdw2024.fbgame_03_bi.dws_fb_common_game_ad_revenue_daily_reports`
	--	PARTITION BY stats_date as 
		SELECT
				stats_date
				,package_name
				,platform
				,country_code
				,max(requests)  as requests
				,max(filled_requests)  as filled_requests
				,max(impressions)  as impressions
				,max(revenue)  as revenue
				,max(clicks)  as clicks
			FROM
				(
				SELECT 
					parse_date('%Y%m%d',_table_suffix) as stats_date
					,case when app_name='Solitaire' then 'fb.zp' 
						when app_name='OHO' then 'fb.ai.avatar.puzzle' 
						when app_name='Bubble Pop Fruit' then 'fb.fruit.bubble' 
						when app_name='AHA' then 'fb.ai.aha' 
						when app_name='Fate Quest' then 'fb.otme.fate.quest' 
						when app_name='Block Juggle' then 'fb.block.juggle' 
						when app_name='Bubble Shoot Pro' then 'fb.bubble.shoot.pro'
						when app_name='Save The Dog' then 'fb.save.dog' 
						else 'other' end as package_name
					,'TOTAL' as platform
					,'TOTAL' as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_hour_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
				and _TABLE_SUFFIX!='20241103'
				--and app_name='Solitaire'
				--and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date,package_name
				UNION ALL 
				SELECT 
					parse_date('%Y%m%d',_table_suffix) as stats_date
					,case when app_name='Solitaire' then 'fb.zp' 
						when app_name='OHO' then 'fb.ai.avatar.puzzle' 
						when app_name='Bubble Pop Fruit' then 'fb.fruit.bubble' 
						when app_name='AHA' then 'fb.ai.aha' 
						when app_name='Fate Quest' then 'fb.otme.fate.quest' 
						when app_name='Block Juggle' then 'fb.block.juggle' 
						when app_name='Bubble Shoot Pro' then 'fb.bubble.shoot.pro'
						when app_name='Save The Dog' then 'fb.save.dog' 
						else 'other' end as package_name
					,'TOTAL' as platform
					,'TOTAL' as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_day_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
				--and app_name='Solitaire'
					and _TABLE_SUFFIX!='20241103'
				--and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date,package_name
				union all 
					SELECT 
					date(stats_date) as stats_date
					,case when app_name='Solitaire' then 'fb.zp' 
						when app_name='OHO' then 'fb.ai.avatar.puzzle' 
						when app_name='Bubble Pop Fruit' then 'fb.fruit.bubble' 
						when app_name='AHA' then 'fb.ai.aha' 
						when app_name='Fate Quest' then 'fb.otme.fate.quest' 
						when app_name='Block Juggle' then 'fb.block.juggle' 
						when app_name='Bubble Shoot Pro' then 'fb.bubble.shoot.pro'
						when app_name='Save The Dog' then 'fb.save.dog' 
						else 'other' end as package_name
					,case when lower(platform)='ios' then 'iOS' when lower(platform)='android' then 'Android' else 'web' end  as platform
					,upper(country) as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.facebook_ad_backup_detail_day` 
				where stats_date >= date_add(run_date,interval -history_day day)
			    and stats_date <= date_add(run_date,interval -history_end_day day)	
				--	and app_name='Solitaire'
				--and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date,platform,country_code,package_name
				union all 
				SELECT 
					date(start_timestamp) as stats_date
					,case when app_name='Solitaire' then 'fb.zp' 
						when app_name='OHO' then 'fb.ai.avatar.puzzle' 
						when app_name='Bubble Pop Fruit' then 'fb.fruit.bubble' 
						when app_name='AHA' then 'fb.ai.aha' 
						when app_name='Fate Quest' then 'fb.otme.fate.quest' 
						when app_name='Block Juggle' then 'fb.block.juggle' 
						when app_name='Bubble Shoot Pro' then 'fb.bubble.shoot.pro'
						when app_name='Save The Dog' then 'fb.save.dog' 
						else 'other' end as package_name
					,case when lower(platform)='ios' then 'iOS' when lower(platform)='android' then 'Android' else 'web' end  as platform
					,'TOTAL' as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
					FROM `fb-ai-avatar-puzzle.analytics_439907691.facebook_ad_backup_detail_day` 
				where stats_date >= date_add(run_date,interval -history_day day)
			    and stats_date <= date_add(run_date,interval -history_end_day day)		
					--and app_name='Solitaire'
					--and app_name='Solitaire'
				--and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date,platform,country_code,package_name
					union all 
					SELECT 
					date(start_timestamp) as stats_date
					,case when app_name='Solitaire' then 'fb.zp' 
						when app_name='OHO' then 'fb.ai.avatar.puzzle' 
						when app_name='Bubble Pop Fruit' then 'fb.fruit.bubble' 
						when app_name='AHA' then 'fb.ai.aha' 
						when app_name='Fate Quest' then 'fb.otme.fate.quest' 
						when app_name='Block Juggle' then 'fb.block.juggle' 
						when app_name='Bubble Shoot Pro' then 'fb.bubble.shoot.pro'
						when app_name='Save The Dog' then 'fb.save.dog' 
						else 'other' end as package_name
					,'TOTAL'  as platform
					,upper(country) as country_code
					,sum(requests)  as requests
					,sum(filled_requests)  as filled_requests
					,sum(impressions)  as impressions
					,sum(revenue)  as revenue
					,sum(clicks)  as clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_detail_day_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
				and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				group by stats_date,platform,country_code,package_name
				)a 
		group by stats_date,platform,country_code,package_name;






----------fb游戏日报

delete `gzdw2024.fbgame_03_bi.dws_fb_common_game_daily_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -history_end_day day);

insert `gzdw2024.fbgame_03_bi.dws_fb_common_game_daily_reports`
	--create table  `gzdw2024.fbgame_03_bi.dws_fb_common_game_daily_reports`
	--PARTITION BY stats_date as 
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
			,safe_divide(lag(revenue)  over(PARTITION by package_name,platform,country_code order by stats_date )
			,lag(active_uv)  over(PARTITION by package_name,platform,country_code order by stats_date )) as last_arpu
		FROM  
			(
			SELECT
				stats_date
				,package_name
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
					,package_name
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
			    FROM  `gzdw2024.fbgame_01_basic.dws_common_game_user_active_report`
			    WHERE event_date >= date_add(run_date,interval -history_day day)
			    and event_date <= date_add(run_date,interval -history_end_day day)
			    union all
			    SELECT
			    	stats_date
			    	,package_name
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
			    FROM `gzdw2024.fbgame_03_bi.dws_fb_common_game_cost_daily_reports`
			      WHERE stats_date >= date_add(run_date,interval -history_day day)
			    and stats_date <= date_add(run_date,interval -history_end_day day)
			      union all
			    SELECT
			    	stats_date
			    	,package_name
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
			    FROM `gzdw2024.fbgame_03_bi.dws_fb_common_game_ad_revenue_daily_reports`
			      WHERE stats_date >= date_add(run_date,interval -history_day day)
			    and stats_date <= date_add(run_date,interval -history_end_day day)
			    )a 
			    group by stats_date,platform,country_code,package_name
			    )b 
    )c;



------fb游戏数据入整体表
			delete `gzdw2024.gz_bi.dws_app_country_daily_reports`
			where stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			and package_name  like 'fb%';


			insert `gzdw2024.gz_bi.dws_app_country_daily_reports`
			SELECT
				stats_date
				,a.package_name
				,case when app_name is not null then app_name else a.package_name end as app_name
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
				,cast(sum(install) as integer) as conversions
			FROM
				`gzdw2024.fbgame_03_bi.dws_fb_common_game_daily_reports` a 
			left join  `gzdw2024.gz_dim.app_info` b 
			on a.package_name=b.package_name
			 WHERE stats_date >= date_add(run_date,interval -history_day day)
		    and stats_date <= date_add(run_date,interval -history_end_day day)
		    and a.platform='TOTAL'
		    --and country_code='TOTAL'
		    group by stats_date,app_name,a.package_name,country_code
		    order by a.package_name,stats_date;








delete  `gzdw2024.fbgame_03_bi.dws_fb_common_game_rention_roi_reports`
where stats_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 9 DAY)
and stats_date <= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 1 day);


insert gzdw2024.fbgame_03_bi.dws_fb_common_game_rention_roi_reports
--	create table  `gzdw2024.fbgame_03_bi.dws_fb_common_game_rention_roi_reports`
	--	PARTITION BY stats_date as 

with a as (						
		SELECT
			platform
			,package_name
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
				,package_name
				,country_code
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 3 DAY) and event_date>=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 3+7 DAY) then ratio_2 else null end)	as avg_ratio2
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 4 DAY) and event_date>=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 4+7 DAY) then ratio_3 else null end)	as avg_ratio3
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 5 day) and event_date>=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 5+7 DAY) then ratio_4 else null end)	as avg_ratio4
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 6 DAY) and event_date>=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 6+7 DAY) then ratio_5 else null end)	as avg_ratio5
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 7 DAY) and event_date>=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 7+7 DAY) then ratio_6 else null end)	as avg_ratio6
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 8 DAY) and event_date>=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 8+7 DAY) then ratio_7 else null end)	as avg_ratio7				
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 9 DAY) and event_date>=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 9+7 DAY) then ratio_8 else null end)	as avg_ratio8
				,avg(case when event_date<=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 15 DAY) and event_date>=DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 15+7 DAY) then ratio_8 else null end)	as avg_ratio14
			FROM 
				(
				SELECT 
					platform
					,package_name
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
			    FROM `gzdw2024.fbgame_01_basic.dws_common_game_user_active_report` 
			    WHERE event_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 18 DAY)
			    and event_date <= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 1 day)
			    AND new_uv>10
				--and platform='TOTAL'
				--and country_code='TOTAL'
        		order by event_date
				)a 
				group by platform,country_code,package_name
			)b 
		),
		b as (
				SELECT 
					event_date as stats_date
          ,package_name
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
			    FROM `gzdw2024.fbgame_01_basic.dws_common_game_user_active_report` 
			    WHERE event_date >= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 9 DAY)
			    and event_date <= DATE_SUB(CURRENT_DATE('America/Los_Angeles') , INTERVAL 1 day)
				--and platform='TOTAL'
				--and country_code='TOTAL'
				order by stats_date
				)
		SELECT
			stats_date
			,package_name
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
			,case when total_bili_7<=0.3 then 1.28*total_bili_7
			when total_bili_7>0.3 and total_bili_7<=0.4 then 1.48*total_bili_7
			when total_bili_7>0.4 and total_bili_7<=0.6 then 1.68*total_bili_7
			when total_bili_7>0.6  then 1.88*total_bili_7 end as total_bili_14
		FROM
			(
				SELECT
					stats_date
					,c.package_name
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
					,package_name
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
					,package_name
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
				and c.package_name=d.package_name
			)e 
			--where platform='TOTAL'
			--and country_code='TOTAL'
			--	order by stats_date desc 
			;

delete  `gzdw2024.fbgame_03_bi.dws_fb_common_game_daily_roi_total_reports`
where  stats_date >= date_add(run_date,interval -history_day day)
  and stats_date <= date_add(run_date,interval -history_end_day day);


insert `gzdw2024.fbgame_03_bi.dws_fb_common_game_daily_roi_total_reports`
	---create table  `gzdw2024.fbgame_03_bi.dws_fb_common_game_daily_roi_total_reports`
	---	PARTITION BY stats_date as 


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
	,lag(new_ratio) over(partition by package_name,platform,country_code order by stats_date) as last_new_ratio
	,lag(new_arpu) over(partition by package_name,platform,country_code order by stats_date) as last_new_arpu
	,lag(arpu) over(partition by package_name,platform,country_code order by stats_date) as last_arpu
	,lag(new_ad_ratio) over(partition by package_name,platform,country_code order by stats_date) as last_new_ad_ratio
	,lag(total_bili_3) over(partition by package_name,platform,country_code order by stats_date) as last_total_bili_3
	,lag(total_bili_7) over(partition by package_name,platform,country_code order by stats_date) as last_total_bili_7
	,lag(total_bili_14) over(partition by package_name,platform,country_code order by stats_date) as last_total_bili_14
	,new_ad_ratio
	,total_bili_14
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
		,case when new_ad_ratio<1.1 then 1.05
		when new_ad_ratio<1.2 then 1.1
		when new_ad_ratio<1.3 then 1.2
		when new_ad_ratio<1.4 then 1.3
		when new_ad_ratio<1.5 then 1.4
		when new_ad_ratio<1.6 then 1.5
		else new_ad_ratio-0.1 end as new_ad_ratio
		,total_bili_14

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
		FROM  `gzdw2024.fbgame_03_bi.dws_fb_common_game_daily_reports`
		WHERE 1=1
		--and platform='TOTAL'
		--and country_code='TOTAL'
		and stats_date >= date_add(run_date,interval -history_day day)
    	and stats_date <= date_add(run_date,interval -history_end_day day)
		)a 
		left join 
		(
		SELECT
			stats_date
			,package_name
			,platform
			,country_code	
			,active_uv
			,new_uv
			,new_ad_uv
			,new_liebian_uv
			,new_ad_liebian_uv 
			,safe_divide(new_uv,new_ad_liebian_uv) as new_ad_ratio
			,total_bili_3
			,total_bili_7
			,ratio_2
			,ratio_3
			,ratio_4
			,ratio_5
			,ratio_6
			,ratio_7
			,ratio_8
			,total_bili_14
		FROM `gzdw2024.fbgame_03_bi.dws_fb_common_game_rention_roi_reports`
		)b 
		on a.stats_date=b.stats_date
		and a.platform=b.platform
		and a.country_code=b.country_code
		and a.package_name=b.package_name
	)c 
	order by stats_date desc
	),
b as 
(
select 
	stats_date
	,a.package_name
	,app_name
	,a.platform
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
	,last_new_ratio
	,last_new_arpu
	,last_arpu
	,last_total_bili_3
	,last_total_bili_7
	,case when first_day_revenue>revenue then revenue*0.88 else first_day_revenue end as first_day_revenue
	,first_3day_revenue
	,first_7day_revenue
	,new_ad_ratio
	,last_new_ad_ratio
	,total_bili_14
	,last_total_bili_14
	,first_14day_revenue

FROM
	(
	SELECT 
		* 
	,case when new_ad_uv is not null  then  install*new_arpu*new_ad_ratio  
		else install*last_new_ratio*last_new_arpu*last_new_ad_ratio  end as first_day_revenue
	,case when new_ad_uv is not null then  install*new_arpu*(1+total_bili_3)*new_ad_ratio 
	else install*last_new_ratio*last_new_arpu*(1+ last_total_bili_3)*last_new_ad_ratio end as first_3day_revenue

	,case when new_ad_uv is not null then  install*new_arpu*(1+total_bili_3)*new_ad_ratio  + new_ad_liebian_uv*arpu*(total_bili_7- total_bili_3)*new_ad_ratio 
	else install*last_new_ratio*last_new_arpu*(1+last_total_bili_3)*last_new_ad_ratio + install*last_new_ratio*last_arpu*(last_total_bili_7- last_total_bili_3)*last_new_ad_ratio end as first_7day_revenue
	,case when new_ad_uv is not null then  install*new_arpu*(1+total_bili_3)*new_ad_ratio  + new_ad_liebian_uv*arpu*(total_bili_7- total_bili_3)*new_ad_ratio  + new_ad_liebian_uv*arpu*(total_bili_14- total_bili_7)*new_ad_ratio 
	else install*last_new_ratio*last_new_arpu*(1+last_total_bili_3)*last_new_ad_ratio + install*last_new_ratio*last_arpu*(last_total_bili_7- last_total_bili_3)*last_new_ad_ratio + install*last_new_ratio*last_arpu*(last_total_bili_14- last_total_bili_7)*last_new_ad_ratio end as first_14day_revenue
    FROM a 
	WHERE 1=1
	)a
	left join  `gzdw2024.gz_dim.app_info` b 
	on a.package_name=b.package_name
)  
SELECT
	stats_date
	,package_name
	,app_name
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
	,last_new_ratio
	,last_new_arpu
	,last_arpu
	,last_total_bili_3
	,last_total_bili_7
	,first_day_revenue
	,first_3day_revenue
	,first_7day_revenue
	,new_ad_ratio
	,last_new_ad_ratio
	,total_bili_14
	,last_total_bili_14
	,first_14day_revenue
FROM b  
UNION all 
SELECT
	stats_date
	,'fb.total' as package_name
	,'FBG_TOTAL' AS app_name
	,platform
	,country_code
	,sum(active_uv) AS active_uv
	,sum(new_uv) AS new_uv
	,sum(retain_uv2) AS retain_uv2
	,sum(cost) AS cost
	,sum(install) AS install
	,sum(requests) AS requests
	,sum(filled_requests) AS filled_requests
	,sum(impressions) AS impressions
	,sum(revenue) AS revenue
	,sum(clicks) AS clicks
	,max(max_stats_date) AS max_stats_date
	,sum(new_ad_uv) AS new_ad_uv
	,sum(new_liebian_uv) AS new_liebian_uv
	,sum(new_ad_liebian_uv ) AS new_ad_liebian_uv
	,avg(total_bili_3) AS total_bili_3
	,avg(total_bili_7) AS total_bili_7
	,avg(arpu ) AS arpu
	,avg(new_arpu) AS new_arpu
	,avg(new_ratio) AS new_ratio
	,avg(last_new_ratio) AS last_new_ratio
	,avg(last_new_arpu) AS last_new_arpu
	,avg(last_arpu) AS last_arpu
	,avg(last_total_bili_3) AS last_total_bili_3
	,avg(last_total_bili_7) AS last_total_bili_7
	,sum(first_day_revenue) AS first_day_revenue
	,sum(first_3day_revenue) AS first_3day_revenue
	,sum(first_7day_revenue) AS first_7day_revenue
	,avg(new_ad_ratio) AS new_ad_ratio
	,avg(last_new_ad_ratio) AS last_new_ad_ratio
	,avg(total_bili_14) AS total_bili_14
	,avg(last_total_bili_14) AS last_total_bili_14
	,sum(first_14day_revenue) AS first_14day_revenue
FROM b  
group by stats_date,package_name,app_name,country_code,platform;




end;
