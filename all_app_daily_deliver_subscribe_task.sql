CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.all_app_daily_deliver_subscribe_task`(run_date DATE, history_day INT64, history_end_day INT64)
begin


------1.套餐价格中间表
delete`gzdw2024.vidma_editor_android_01_basic.dwd_subscribe_product_reports`
where stats_date>=date_add(run_date,interval -history_end_day day)
and  stats_date<=date_add(run_date,interval -history_end_day day);


insert `gzdw2024.vidma_editor_android_01_basic.dwd_subscribe_product_reports`
			SELECT
				date_add(run_date,interval -history_end_day day) as stats_date
				,package_name
				,auto_renew_product_id
				,avg(price_usd) as price_usd
			FROM
				(
				SELECT
					subscirbe_date
					,package_name
					,transaction_id
					,auto_renew_product_id
					,safe_divide(price_amount,rate) as price_usd
				FROM
					(
					SELECT 
					  owner_id
					  ,package_name
					  ,date(logged_at) as subscirbe_date
					  ,event_type
					  ,auto_renew_product_id
					  ,is_trial_period
					  ,transaction_id
					  ,original_transaction_id
					  ,price_amount_micros/1000000 as price_amount
					  ,price_currency_code
					  ,substring(cast(logged_at as string),1,7) as stats_mon
					FROM `mv-editor-4bf54.analytics_289941232.woolong_events_*` 
					WHERE _TABLE_SUFFIX>=replace(cast(date_add(run_date,interval -history_day day) as string),'-','') 
					and _TABLE_SUFFIX<=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','') 
					and event_type in ('INITIAL_BUY','DID_RENEW','DID_RECOVER')
					--and is_trial_period is false
					and price_currency_code not in ('None')
					)c
					left join 
					(
					SELECT 
						stats_mon
						,currency
						,rate 
					FROM `gzdw2024.gz_dim.exchange_rate` 
					)d 
					on c.stats_mon=d.stats_mon
					and c.price_currency_code=d.currency
        		)e
				GROUP by 	date_add(run_date,interval -history_end_day day)
				,package_name
				,auto_renew_product_id;




--create table   `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_reports`
--PARTITION BY subscirbe_date as 


--2.订单对应价格中间表
delete`gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_reports`
where subscirbe_date>=date_add(run_date,interval -(history_end_day+4) day)
and  subscirbe_date<=date_add(run_date,interval -history_end_day day);
 insert `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_reports`
				SELECT
					owner_id
					,subscirbe_date
					,event_type
					,a.auto_renew_product_id
					,is_trial_period
					,a.transaction_id
					,a.original_transaction_id
					,price_amount
					,case when currency is not null then safe_divide(price_amount,rate)
					 when    b.price_usd is not null then b.price_usd
					 when a.auto_renew_product_id like '%year%' then 39.99
					 when a.auto_renew_product_id like '%month%' then 11.99
					 when a.auto_renew_product_id like '%week%' then 6.99
					 else 10.99 end as price_usd
					,a.stats_mon
				FROM
					(
					SELECT 
					  owner_id
					  ,package_name
					  ,date(logged_at) as subscirbe_date
					  ,event_type
					  ,auto_renew_product_id
					  ,is_trial_period
					  ,transaction_id
					  ,original_transaction_id
					  ,price_amount_micros/1000000 as price_amount
					  ,price_currency_code
					  ,substring(cast(logged_at as string),1,7) as stats_mon
					FROM `mv-editor-4bf54.analytics_289941232.woolong_events_*` 
					WHERE _TABLE_SUFFIX>=replace(cast(date_add(run_date,interval -(history_end_day+4) day) as string),'-','') 
					and _TABLE_SUFFIX<=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','') 
					and event_type in ('INITIAL_BUY','DID_RENEW','DID_RECOVER')
					--and is_trial_period is false
					--and transaction_id='GPA.3322-3614-7258-20900..5'
					--and price_currency_code  in ('None')
					)a 
					left join
					(
					SELECT 
					  auto_renew_product_id
					  ,package_name
					  ,stats_date
					  ,max(price_usd) as price_usd
					FROM `gzdw2024.vidma_editor_android_01_basic.dwd_subscribe_product_reports` 
					WHERE stats_date>=date_add(run_date,interval -(history_end_day+4) day)
					and stats_date<=date_add(run_date,interval -history_end_day day)
					group by auto_renew_product_id,package_name,stats_date
					)b 
					on a.auto_renew_product_id=b.auto_renew_product_id
					and a.package_name=b.package_name
					and a.subscirbe_date=b.stats_date
					left join 
					(
					SELECT 
						stats_mon
						,currency
						,rate 
					FROM `gzdw2024.gz_dim.exchange_rate` 
					)d 
					on a.stats_mon=d.stats_mon
					and a.price_currency_code=d.currency;




--drop table if EXISTS `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_trial_detail`;
--create table   `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_trial_detail`
--PARTITION BY stats_date as 
-----3.买量用户试用
delete`gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_trial_detail`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -history_end_day day);
 insert `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_trial_detail`
			SELECT	
				stats_date
				,ifnull(country_code,country) as country_code
				,package_name
				,traffic_source_name
				,a.user_pseudo_id	
				,user_id 
				,subscirbe_date
				,price_usd	
				,auto_renew_product_id	
				,transaction_id
				,original_transaction_id
			FROM
				(
				SELECT  
					event_date as stats_date
					
					,package_name
					,traffic_source_name
					,case when REGEXP_EXTRACT(traffic_source_name, r'-([A-Z]{2})-\[') is null then country 
					 when REGEXP_EXTRACT(traffic_source_name, r'-([A-Z]{2})-\[') in ('EU','WW') THEN country 
					else REGEXP_EXTRACT(traffic_source_name, r'-([A-Z]{2})-\[') end as country
					,user_pseudo_id
				FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date>='2024-12-02'
				and event_date<=date_add(run_date,interval -history_end_day day)
				and traffic_source_medium = "cpc"
				and is_new=1
				and traffic_source_name like 'GA%'
				AND package_name = "vidma.video.editor.videomaker"
				)a 
				left join
				(
				SELECT
					user_pseudo_id
					,event_date
					,max(user_id) as user_id
				FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_userid_di`
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				AND package_name = "vidma.video.editor.videomaker"
				group by user_pseudo_id,event_date
				)b 
				on a.user_pseudo_id=b.user_pseudo_id
				and a.stats_date=b.event_date
				left join
				(
					SELECT 
					  owner_id
					  ,subscirbe_date
					  ,event_type
					  ,auto_renew_product_id
					  ,is_trial_period
					  ,transaction_id
					  ,original_transaction_id
					  ,price_usd
					FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_reports`
					WHERE  subscirbe_date>=date_add(run_date,interval -history_day day)
						and subscirbe_date<=date_add(run_date,interval -history_end_day day)
					and event_type='INITIAL_BUY'
					and is_trial_period is true
					)c 
				on b.user_id=c.owner_id
				left join `gzdw2024.gz_dim.country_info` cc
				on upper(a.country)=upper(cc.country_name);
				--order by user_id,stats_date,subscirbe_date


-----4.买量用户付费
--drop table if EXISTS `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_pay_detail`;
--create table   `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_pay_detail`
--PARTITION BY stats_date as 

delete`gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_pay_detail`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -history_end_day day);
 insert `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_pay_detail`
			SELECT	
				stats_date
				,ifnull(country_code,country) as country_code
				,package_name
				,traffic_source_name
				,a.user_pseudo_id	
				,user_id 
				,subscirbe_date
				,price_usd	
				,auto_renew_product_id	
				,transaction_id
				,original_transaction_id
			FROM
				(
				SELECT  
					event_date as stats_date
					
					,package_name
					,traffic_source_name
					,case when REGEXP_EXTRACT(traffic_source_name, r'-([A-Z]{2})-\[') is null then country 
					 when REGEXP_EXTRACT(traffic_source_name, r'-([A-Z]{2})-\[') in ('EU','WW') THEN country 
					else REGEXP_EXTRACT(traffic_source_name, r'-([A-Z]{2})-\[') end as country
					,user_pseudo_id
				FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				and traffic_source_medium = "cpc"
				and event_date>='2024-12-02'
				and is_new=1
				and traffic_source_name like 'GA%'
				AND package_name = "vidma.video.editor.videomaker"
				)a 
				left join
				(
				SELECT
					user_pseudo_id
					,event_date
					,max(user_id) as user_id
				FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_userid_di`
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				AND package_name = "vidma.video.editor.videomaker"
				group by user_pseudo_id,event_date
				)b 
				on a.user_pseudo_id=b.user_pseudo_id
				and a.stats_date=b.event_date
				left join
				(
					SELECT 
					  owner_id
					  ,subscirbe_date
					  ,event_type
					  ,auto_renew_product_id
					  ,is_trial_period
					  ,transaction_id
					  ,original_transaction_id
					  ,price_usd
					FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_reports`
					WHERE  subscirbe_date>=date_add(run_date,interval -history_day day)
						and subscirbe_date<=date_add(run_date,interval -history_end_day day)
					and  event_type in ('INITIAL_BUY','DID_RENEW','DID_RECOVER')
					and is_trial_period is false
					)c 
				on b.user_id=c.owner_id
				left join `gzdw2024.gz_dim.country_info` cc
				on upper(a.country)=upper(cc.country_name);
				--order by user_id,stats_date,subscirbe_date




-----统计订阅与付费
--drop table if EXISTS `gzdw2024.gz_bi.dwd_deliver_subscribe_reports`;
--create table   `gzdw2024.gz_bi.dwd_deliver_subscribe_reports`
--PARTITION BY stats_date as 

delete`gzdw2024.gz_bi.dwd_deliver_subscribe_reports`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -history_end_day day);
 insert `gzdw2024.gz_bi.dwd_deliver_subscribe_reports`
	SELECT
		stats_date
		,package_name
		,country_code
		,product_id
		,traffic_source_name
		,sum(ga_cost) as ga_cost
		,sum(install_ga) as install_ga
		,sum(trial_counts) as trial_counts
		,sum(pay_counts) as pay_counts
		,sum(price_usd) as price_usd
		,sum(price_usd_7_day) as price_usd_7_day
		,sum(price_usd_1_month) as price_usd_1_month
		,sum(total_revenue) as total_revenue
		,sum(ad_revenue) as ad_revenue
		,sum(vip_revenue) as vip_revenue	
		,sum(conversions) as conversions
		,sum(vip_revenue_7_day_ago)	as vip_revenue_7_day_ago
		,sum(vip_revenue_7_day_ago)+sum(ad_revenue)	as total_revenue_7_day_ago
	FROM
		(
		SELECT
			stats_date
			,package_name
			,country_code
			,'TOTAL' AS product_id
			,traffic_source_name
			,0 as ga_cost
			,COUNT(distinct user_pseudo_id) as install_ga
			,0 as trial_counts
			,0 as pay_counts
			,0 as price_usd
			,0 as price_usd_7_day
			,0 as price_usd_1_month
			,0 as total_revenue
			,0 as ad_revenue
			,0 as vip_revenue	
			,0 as conversions
			,0	as vip_revenue_7_day_ago
		FROM
			(
			SELECT
				stats_date
				,package_name
				,array['TOTAL',country_code] as country_code
				,array['TOTAL',traffic_source_name] traffic_source_name
				,user_pseudo_id
			FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_trial_detail`
			WHERE 1=1
			and stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			)a 
			,unnest(country_code) as country_code
			,unnest(traffic_source_name) as traffic_source_name
			--WHERE country_code='TOTAL'
			group by stats_date,package_name,country_code,traffic_source_name
			union all 
			SELECT
			stats_date
			,package_name
			,country_code
			,product_id
			,traffic_source_name
			,0 as ga_cost
			,0 as install_ga
			,COUNT(distinct transaction_id) as trial_counts
			,0 as pay_counts
			,0 as price_usd
			,0 as price_usd_7_day
			,0 as price_usd_1_month
			,0 as total_revenue
			,0 as ad_revenue
			,0 as vip_revenue	
			,0 as conversions
			,0	as vip_revenue_7_day_ago
		FROM
			(
			SELECT
				stats_date
				,package_name
				,array['TOTAL',country_code] as country_code
				,array['TOTAL',auto_renew_product_id] as product_id
				,array['TOTAL',traffic_source_name] traffic_source_name
				,user_pseudo_id
				,transaction_id
			FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_trial_detail`
			WHERE 1=1
			and stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			and subscirbe_date is not null 
			)a 
			,unnest(country_code) as country_code
			,unnest(product_id) as product_id
			,unnest(traffic_source_name) as traffic_source_name
			group by stats_date,package_name,country_code,product_id,traffic_source_name
			union all 
			SELECT
			stats_date
			,package_name
			,country_code
			,product_id
			,traffic_source_name
			,0 as ga_cost
			,0 as install_ga
			,0 as trial_counts
			,COUNT(distinct transaction_id) as pay_counts
			,sum(price_usd)*0.85 as price_usd
			,sum(case when date_diff(subscirbe_date,stats_date,day)<=8 then  price_usd else 0 end)*0.85 as price_usd_7_day
			,sum(case when date_diff(subscirbe_date,stats_date,day)<=31 then  price_usd else 0 end)*0.85 as price_usd_1_month
			,0 as total_revenue
			,0 as ad_revenue
			,0 as vip_revenue	
			,0 as conversions
			,0	as vip_revenue_7_day_ago
		FROM
			(
			SELECT
				stats_date
				,package_name
				,array['TOTAL',country_code] as country_code
				,array['TOTAL',auto_renew_product_id] as product_id
				,array['TOTAL',traffic_source_name] traffic_source_name
				,user_pseudo_id
				,transaction_id
				,price_usd
				,subscirbe_date
			FROM `gzdw2024.vidma_editor_android_01_basic.dwd_user_subscribe_pay_detail`
			WHERE 1=1
			and stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			and subscirbe_date is not null 
			)a 
			,unnest(country_code) as country_code
			,unnest(product_id) as product_id
			,unnest(traffic_source_name) as traffic_source_name
			WHERE 1=1
			--and country_code='TOTAL'
			--and product_id='TOTAL'
			group by stats_date,package_name,country_code,product_id,traffic_source_name
			union all 
			SELECT  
				stats_date
				,package_name
				,ifnull(b.country_code,a.country_code) as country_code
				,'TOTAL' AS product_id
				,'TOTAL' as traffic_source_name
				,0 as ga_cost
				,0 as install_ga
				,0 as trial_counts
				,0 as pay_counts
				,0 as price_usd
				,0 as price_usd_7_day
				,0 as price_usd_1_month
				,total_revenue
				,ad_revenue
				,vip_revenue	
				,0 as conversions
				,0 as vip_revenue_7_day_ago
			FROM `gzdw2024.gz_bi.dws_app_country_daily_reports`  a
			left join `gzdw2024.gz_dim.country_info` b
			on upper(a.country_code)=upper(b.country_name)
			WHERE stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			union all 
			SELECT  
				date_add(stats_date,interval -7 day)
				,package_name
				,ifnull(b.country_code,a.country_code) as country_code
				,'TOTAL' AS product_id
				,'TOTAL' as traffic_source_name
				,0 as ga_cost
				,0 as install_ga
				,0 as trial_counts
				,0 as pay_counts
				,0 as price_usd
				,0 as price_usd_7_day
				,0 as price_usd_1_month
				,0 as total_revenue
				,0 as ad_revenue
				,0 as vip_revenue	
				,0 as conversions
				,vip_revenue	as vip_revenue_7_day_ago
			FROM `gzdw2024.gz_bi.dws_app_country_daily_reports`  a
			left join `gzdw2024.gz_dim.country_info` b
			on upper(a.country_code)=upper(b.country_name)
			WHERE stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
			union all 
			SELECT  
				stats_date
				,package_name
				,ifnull(b.country_code,a.country_code) as country_code
				,'TOTAL' AS product_id
				,campaign_name as traffic_source_name
				,ga_cost
				,0 as install_ga
				,0 as trial_counts
				,0 as pay_counts
				,0 as price_usd
				,0 as price_usd_7_day
				,0 as price_usd_1_month
				,0 as total_revenue
				,0 as ad_revenue
				,0 as vip_revenue	
				,conversions
				,0 as vip_revenue_7_day_ago
			FROM `gzdw2024.cost_data.dws_ga_cost_daily`  a
			left join `gzdw2024.gz_dim.country_info` b
			on upper(a.country_code)=upper(b.country_name)
			WHERE stats_date>=date_add(run_date,interval -history_day day)
			and  stats_date<=date_add(run_date,interval -history_end_day day)
		)b 
		WHERE 1=1
		and package_name in ("vidma.video.editor.videomaker")
		--and country_code='TOTAL'
		--and product_id='TOTAL'
		group by stats_date
		,package_name
		,country_code
		,product_id,traffic_source_name;


		end 
