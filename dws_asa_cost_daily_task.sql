CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.dws_asa_cost_daily_task`(run_date DATE, history_day INT64, end_day INT64)
begin

delete `gzdw2024.cost_data.dws_asa_cost_daily`
where stats_date>=date_add(run_date,interval -history_day day);


insert `gzdw2024.cost_data.dws_asa_cost_daily`
	SELECT
		stats_date
		,a.package_name
		,app_name
		,country_code
		,asa_cost
	FROM
		(
		SELECT 
			 PARSE_DATE('%Y%m%d',stats_date) stats_date
			 ,case when lower(campaign_name) like '%scanner%' then 'pdf.scanner.app'
			 when lower(campaign_name) like '%text%' then 'second.phone.number.text.free.call.app' else campaign_name end as package_name
			,upper(country_region) as country_code
			,sum(local_spend) as asa_cost
		FROM `gzdw2024.campaigns.prefect_campaigns_reports_Scanner-Singapore_*` 
		WHERE 1=1 
		AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
    	and local_spend>0
    	AND _TABLE_SUFFIX >='20240901'
		group by stats_date,country_code,package_name
		union all 
		SELECT 
			 PARSE_DATE('%Y%m%d',stats_date) stats_date
			 ,case when lower(campaign_name) like '%scanner%' then 'pdf.scanner.app'
			 when lower(campaign_name) like '%text%' then 'second.phone.number.text.free.call.app' else campaign_name end as package_name
			,'TOTAL' as country_code
			,sum(local_spend) as asa_cost
		FROM `gzdw2024.campaigns.prefect_campaigns_reports_Scanner-Singapore_*` 
		WHERE 1=1 
		AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
    and local_spend>0
    AND _TABLE_SUFFIX >='20240901'
		group by stats_date,country_code,package_name
		union all 
		SELECT 
			 PARSE_DATE('%Y%m%d',stats_date) stats_date
			 ,case when (campaign_name) like '%US_Search_keywords_20240225%' then 'com.vidma.video.editor'
			 when lower(campaign_name) like '%vidma%' then 'com.vidma.video.editor' else campaign_name end as package_name
			,upper(country_region) as country_code
			,sum(local_spend) as asa_cost
		FROM `gzdw2024.campaigns.prefect_campaigns_reports_VIDMA_*` 
		WHERE 1=1 
		AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
    and local_spend>0
    AND _TABLE_SUFFIX >='20240901'
		group by stats_date,country_code,package_name
		union all 
		SELECT 
			 PARSE_DATE('%Y%m%d',stats_date) stats_date
			,case when (campaign_name) like '%US_Search_keywords_20240225%' then 'com.vidma.video.editor'
			 when lower(campaign_name) like '%vidma%' then 'com.vidma.video.editor' else campaign_name end as package_name
			,'TOTAL' as country_code
			,sum(local_spend) as asa_cost
		FROM `gzdw2024.campaigns.prefect_campaigns_reports_VIDMA_*` 
		WHERE 1=1 
		AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
    	and local_spend>0
    	AND _TABLE_SUFFIX >='20240901'
		group by stats_date,country_code,package_name
		)a 
		 left join
		    (
		    SELECT package_name,app_name FROM `gzdw2024.gz_dim.app_info`

		      )c 
		    on a.package_name=c.package_name
		    --WHERE a.country_code='TOTAL'
		;


		    end;
