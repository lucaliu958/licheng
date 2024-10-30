CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.all_app_daily_deliver_total_data`(run_date DATE, history_day INT64, history_end_day INT64)
begin

delete gzdw2024.gz_bi.dws_delivery_report_total 
where stats_date>=date_add(run_date,interval -history_day day)
			and stats_date<=date_add(run_date,interval -history_end_day day);
--and package_name in ('vidma.video.editor.videomaker');

INSERT
  gzdw2024.gz_bi.dws_delivery_report_total

	SELECT
		stats_date
		,a.package_name
		,app_name
		,country_code
		,SUM(ga_cost) as ga_cost
		,SUM(install_ga) as install_ga
		,SUM(trial_counts) as trial_counts
		,SUM(total_revenue) as total_revenue
		,SUM(ad_revenue) as ad_revenue
		,SUM(vip_revenue) as vip_revenue
		,SUM(conversions) as conversions
	FROM
		(
		SELECT
			stats_date
			,package_name
			,case when ifnull(country_code,country)='SYRIA' then 'SY'
			when ifnull(country_code,country)='TÜRKIYE' then 'TR'
			when ifnull(country_code,country)='MYANMAR (BURMA)' then 'MM'
			when ifnull(country_code,country)='PALESTINE' then 'PS'
			when ifnull(country_code,country)='AUSTRALIA' then 'AU'
			when ifnull(country_code,country)='CONGO - KINSHASA' then 'CD'
			when ifnull(country_code,country)='BOSNIA & HERZEGOVINA' then 'BA'
			when ifnull(country_code,country)='NORTH MACEDONIA' then 'MK'
			when ifnull(country_code,country)='KOSOVO' then 'XK'
			else ifnull(country_code,country) end as country_code
			,0 as ga_cost
			,install_ga
			,trial_counts
			,0 as total_revenue						
			,0 as ad_revenue
			,0 as vip_revenue
			,0 as conversions
		FROM
			(
			SELECT
				stats_date
				,package_name
				,upper(country) as country
				,install_ga
				,trial_counts
			FROM	gzdw2024.vidma_editor_android_02_event.dws_delivery_report 
			WHERE stats_date>=date_add(run_date,interval -history_day day)
			and stats_date<=date_add(run_date,interval -history_end_day day)
			AND traffic_source_name='TOTAL'
			-- AND package_name in ("vidma.video.editor.videomaker")
			)a 
			left join
			(
			SELECT 
				country_name_2
				,country_name_3 as country_code
			FROM `hzdw2024.hz_dim.dim_country`
			)b 
			on a.country=b.country_name_2
			union all 
			SELECT  
				stats_date
				,package_name
				,country_code
				,ga_cost
				,0 as install_ga
				,0 as trial_counts
				,total_revenue
				,ad_revenue
				,vip_revenue	
				,conversions
			FROM `gzdw2024.gz_bi.dws_app_country_daily_reports` 
			WHERE stats_date>=date_add(run_date,interval -history_day day)
			and stats_date<=date_add(run_date,interval -history_end_day day)
			-- AND package_name in ("vidma.video.editor.videomaker")
			 )a 
			left join
		    (
		    SELECT package_name,app_name FROM `gzdw2024.gz_dim.app_info`
		      )c 
		    on a.package_name=c.package_name
			group by package_name,stats_date,country_code,app_name
			order by ga_cost desc ;


end;
