CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.ios_asa_convert_task`(run_date DATE, history_day INT64 , history_end_day INT64)
begin


-----ios ASA_keywords 报告
delete `gzdw2024.cost_data.dws_ios_asa_convert_report`
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -history_end_day day);


insert `gzdw2024.cost_data.dws_ios_asa_convert_report`
	--drop table if exists `gzdw2024.cost_data.dws_ios_asa_convert_report`;
--create table  `gzdw2024.cost_data.dws_ios_asa_convert_report`
--	PARTITION BY stats_date as 
	SELECT
		stats_date
		,d.package_name
		,app_name
		,campaign_name
		,campaign_id
		,adgroup_name
		,adgroup_id
		,keyword_id
		,keyword
		,country_code
		,impressions
		,taps
		,total_installs
  		,local_spend
		,view_installs
		,tap_installs
		,total_new_downloads
		,total_redownloads
		,tap_new_downloads
		,tap_redownloads
		,view_new_downloads
		,view_redownloads
	FROM
		(
		SELECT
			a.stats_date
			,case when lower(campaign_name) like '%scanner%' then 'pdf.scanner.app'
			 when lower(campaign_name) like '%text%' then 'second.phone.number.text.free.call.app' 
			 when (campaign_name) like '%US_Search_keywords_20240225%' then 'com.vidma.video.editor'
			 when lower(campaign_name) like '%vidma%' then 'com.vidma.video.editor'
			 else campaign_name end as package_name
			,c.campaign_name
			,c.campaign_id
			,a.adgroup_name
			,a.adgroup_id
			,keyword_id
			,keyword
			,country_code
			,impressions
			,taps
			,total_installs
      		,local_spend
			,view_installs
			,tap_installs
			,total_new_downloads
			,total_redownloads
			,tap_new_downloads
			,tap_redownloads
			,view_new_downloads
			,view_redownloads
		FROM
			(
			SELECT 
				 PARSE_DATE('%Y%m%d',stats_date) stats_date
			
			    ,adgroup_name
			    ,adgroup_id
			    ,keyword_id
				,keyword
				,upper(country_region) as country_code
				,impressions as impressions
				,taps as taps
				,total_installs
        		,local_spend
				,view_installs
				,tap_installs
				,total_new_downloads
				,total_redownloads
				,tap_new_downloads
				,tap_redownloads
				,view_new_downloads
				,view_redownloads
			FROM `gzdw2024.campaigns.prefect_keyword_reports_Scanner-Singapore_*` 
			WHERE 1=1 
			--and  _TABLE_SUFFIX='20250407'
			AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
			AND _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
			union all  
			SELECT 
				 PARSE_DATE('%Y%m%d',stats_date) stats_date
			  
			    ,adgroup_name
			    ,adgroup_id
			    ,keyword_id
				,keyword
				,upper(country_region) as country_code
				,impressions as impressions
				,taps as taps
				,total_installs
        		,local_spend
				,view_installs
				,tap_installs
				,total_new_downloads
				,total_redownloads
				,tap_new_downloads
				,tap_redownloads
				,view_new_downloads
				,view_redownloads
			FROM `gzdw2024.campaigns.prefect_keyword_reports_VIDMA_*` 
			WHERE 1=1 
			--and  _TABLE_SUFFIX='20250407'
			AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
			AND _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
			)a 
			left join 
			(
			SELECT 
				PARSE_DATE('%Y%m%d',stats_date) stats_date
				,adgroup_id as adgroup_id
			    ,max(campaign_id) as campaign_id
			FROM `gzdw2024.campaigns.prefect_adgroups_reports_Scanner-Singapore_*` 
			WHERE 1=1 
			--and  _TABLE_SUFFIX='20250407'
			AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
			AND _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
			group by adgroup_id,stats_date
			union all  
			SELECT 
				PARSE_DATE('%Y%m%d',stats_date) stats_date
				,adgroup_id as adgroup_id
			    ,max(campaign_id) as campaign_id
			FROM `gzdw2024.campaigns.prefect_adgroups_reports_VIDMA_*` 
			WHERE 1=1 
			--and  _TABLE_SUFFIX='20250407'
			AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
			AND _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
			group by adgroup_id,stats_date
			)b 
			on a.adgroup_id=b.adgroup_id
			AND a.stats_date=b.stats_date
			left join 
			(
			SELECT 
				PARSE_DATE('%Y%m%d',stats_date) stats_date
				,cast(campaign_id as string) as campaign_id
			    ,max(campaign_name) as campaign_name
			FROM `gzdw2024.campaigns.prefect_campaigns_reports_Scanner-Singapore_*` 
			WHERE 1=1 
			--and  _TABLE_SUFFIX='20250407'
			AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
			AND _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
			group by campaign_id,stats_date
			union all  
			SELECT 
				PARSE_DATE('%Y%m%d',stats_date) stats_date
				,cast(campaign_id as string) as campaign_id
			    ,max(campaign_name) as campaign_name
			FROM `gzdw2024.campaigns.prefect_campaigns_reports_VIDMA_*` 
			WHERE 1=1 
			--and  _TABLE_SUFFIX='20250407'
			AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
			AND _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
			group by campaign_id,stats_date
			)c 
			on c.campaign_id=b.campaign_id
			AND c.stats_date=b.stats_date
		)d   
	    left join  `gzdw2024.gz_dim.app_info` b 
		on d.package_name=b.package_name;



		end
