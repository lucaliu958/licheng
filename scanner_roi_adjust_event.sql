CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.scanner_roi_adjust_event`(run_date DATE, history_day INT64, hitory_retain_day INT64)
begin


----ASA
delete scanner-master-android.scanner_ios_dw.scanner_ios_asa_delivery_data
where stats_date>=date_add(run_date,interval -history_day day)
;


insert    scanner-master-android.scanner_ios_dw.scanner_ios_asa_delivery_data 
SELECT
	PARSE_DATE('%Y%m%d',a.stats_date) stats_date
	,package_name
	,b.camp_name as campaign_name
    ,cast(b.camp_id as string) as campaign_id
    ,a.adgroup_name as ad_group_name
    ,cast(a.adgroup_id as string) as ad_group_id
    ,cast(keyword_id as string) as keyword_id
	,keyword
	,country_code
	,impressions
	,taps
	,total_installs
	,view_installs
	,tap_installs
	,installs
	,local_spend
FROM
	(
	SELECT 
	  stats_date
	  ,adgroup_id
	  ,adgroup_name
	  ,keyword_id
	  ,keyword
	  ,upper(country_region) as country_code
	  ,impressions
	  ,taps
	  ,0 as total_installs
	  ,0 as view_installs
	  ,0 as tap_installs
	  ,installs
	  ,local_spend
	FROM `gzdw2024.campaigns.prefect_keyword_reports_Scanner-Singapore_*` 
	WHERE 1=1 
	AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
	)a 
	join
	(
	SELECT  
	   package_name
	  ,camp_name
	  ,camp_id
	  ,ad_group
	  ,CAST(ad_group_id as string) as ad_group_id
	FROM `scanner-master-android.scanner_ios_dw.camp_upload_schema` 
	)b 
	on a.adgroup_id=b.ad_group_id
	union all 
	SELECT
		stats_date
		,b.package_name
		,campaign_name
		,a.campaign_id
		,ad_group_name
		,ad_group_id
		,keyword_id
		,keyword
		,country_code
		,impressions
		,taps
		,total_installs
		,view_installs
		,tap_installs
    ,installs
		,local_spend
	FROM  
		(
		SELECT 
			 PARSE_DATE('%Y%m%d',stats_date) stats_date

			,campaign_name
		    ,cast(campaign_id as string) as campaign_id
		    ,'undefined' as ad_group_name
		    ,'undefined' as ad_group_id
		    ,'undefined' as keyword_id
			,'undefined' as keyword
			,upper(country_region) as country_code
			,0 as impressions
			,0 as taps
			,total_installs
			,view_installs
			,tap_installs
			,0 as installs
			,0 as local_spend
		FROM `gzdw2024.campaigns.prefect_campaigns_reports_Scanner-Singapore_*` 
		WHERE 1=1 
		AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
		)a 
		join
		(
		SELECT  
		   package_name		  
		  ,cast(camp_id as string) as camp_id		  
		FROM `scanner-master-android.scanner_ios_dw.camp_upload_schema` 
		group by package_name,camp_id
		)b 
		on a.campaign_id=b.camp_id;




-----google ads camp信息


delete scanner-master-android.scanner_ios_dw.scanner_ios_asa_delivery_data
where stats_date=run_date
;

insert    scanner-master-android.scanner_ios_dw.camp_ad_keyword_detail 
SELECT
	date(run_date) as stats_date
	, package_name
	,a.campaign_name
	,cast(a.campaign_id as string) as campaign_id
	,cast(b.ad_group_id as string) as ad_group_id
	,ad_group_name
	,cast(ad_group_criterion_criterion_id as string) as keyword_id
	,ad_group_criterion_keyword_text as keywords
FROM
	(
	SELECT
	    DISTINCT campaign_id,
	    campaign_name, 
	    'pdf.scanner.app' as package_name
	  FROM
	    hzdw2024.all.p_ads_Campaign_5964298848
	    where    LOWER(campaign_name) LIKE '%scannner-ios%'
	    OR LOWER(campaign_name) LIKE '%scanner-ios%'
	)a 
	left join
	(
	    SELECT 
		ad_group_id
		,campaign_id
		,ad_group_criterion_keyword_text
		,ad_group_criterion_criterion_id
	FROM `hzdw2024.all.p_ads_Keyword_5964298848` 
	WHERE 1=1
	--and TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("2024-08-25") 
	group by ad_group_id,campaign_id,ad_group_criterion_keyword_text,ad_group_criterion_criterion_id
	)b 
	on a.campaign_id=b.campaign_id
	left join 
	(
	SELECT 
		ad_group_id,ad_group_name ,campaign_id
	FROM `hzdw2024.all.p_ads_AdGroup_5964298848` 
	WHERE 1=1
	--and TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("2024-08-27")
	group by ad_group_id,ad_group_name ,campaign_id
	)c  
	on b.ad_group_id=c.ad_group_id;



-----分关键词的展示点击花费
delete scanner-master-android.scanner_ios_dw.scanner_ios_googleads_keywords_delivery_data
where stats_date>=date_add(run_date,interval -history_day day)
;

insert    scanner-master-android.scanner_ios_dw.scanner_ios_googleads_keywords_delivery_data 
SELECT
	a.stats_date
	,package_name
	,case when c.campaign_id is not null then c.campaign_id else a.campaign_id end as campaign_id
	,ifnull(campaign_name,'undefined') as campaign_name
	,case when c.ad_group_id is not null then c.ad_group_id else a.ad_group_id end as ad_group_id
	,ifnull(ad_group_name,'undefined') as ad_group_name
	,case when c.keyword_id is not null then c.keyword_id else a.keyword_id end as keyword_id
	,ifnull(keywords,'undefined') as keywords
	,sum(metrics_impressions) as impressions
	,sum(metrics_clicks) as clicks
	,sum(metrics_cost_micros)/1000000*max(case when exchange_rate is not null then exchange_rate else 0.128 end ) as cost_usd
FROM
(
SELECT 
	cast(ad_group_criterion_criterion_id as string) as keyword_id
	,cast(ad_group_id as string) as ad_group_id
	,cast(campaign_id as string) as campaign_id
	,metrics_cost_micros
	,metrics_impressions
	,metrics_clicks
	,date(_PARTITIONTIME) as stats_date
FROM `hzdw2024.all.p_ads_KeywordBasicStats_5964298848`
WHERE 1=1
and date(_PARTITIONTIME) >= date_add(run_date,interval -history_day day)
--and _DATA_DATE='2024-08-15'
--where campaign_id in (21459680538 )
)a 
 join
(SELECT 
	campaign_id,package_name
from scanner-master-android.scanner_ios_dw.camp_ad_keyword_detail
WHERE package_name='pdf.scanner.app'
and stats_date=run_date
group by campaign_id,package_name
)b 
on a.campaign_id=b.campaign_id
left join
(SELECT 
	keyword_id,keywords,campaign_id,campaign_name,ad_group_name,ad_group_id
from scanner-master-android.scanner_ios_dw.camp_ad_keyword_detail
WHERE package_name='pdf.scanner.app'
and stats_date=run_date
group by keyword_id,keywords,campaign_id,campaign_name,ad_group_name,ad_group_id,package_name
)c
on a.keyword_id=c.keyword_id
and a.ad_group_id=c.ad_group_id
and a.campaign_id=c.campaign_id
left join
(
SELECT stats_date,exchange_rate 
FROM `scanner-master-android.scanner_ios_dw.exchange_rete` 
where 1=1
)d 
on a.stats_date=d.stats_date
group by stats_date,keyword_id,keywords,campaign_id,campaign_name,ad_group_name,ad_group_id,package_name;

-----分campaign和国家的展示点击花费
delete scanner-master-android.scanner_ios_dw.scanner_ios_googleads_country_delivery_data
where stats_date>=date_add(run_date,interval -history_day day)
;

insert    scanner-master-android.scanner_ios_dw.scanner_ios_googleads_country_delivery_data 
SELECT
	a.stats_date
	,package_name
	,case when b.campaign_id is not null then b.campaign_id else a.campaign_id end as campaign_id
	,ifnull(campaign_name,'undefined') as campaign_name
	,case when c.country_code is not null then c.country_code else a.country_criterion_id end as country_code
	,sum(metrics_impressions) as impressions
	,sum(metrics_clicks) as clicks
	,sum(metrics_cost_micros)/1000000*max(case when exchange_rate is not null then exchange_rate else 0.128 end ) as cost_usd
FROM
(
SELECT 
	cast(geographic_view_country_criterion_id as string) as country_criterion_id
	,cast(campaign_id as string) as campaign_id
	,metrics_cost_micros
	,metrics_impressions
	,metrics_clicks
	,date(_PARTITIONTIME) as stats_date
FROM `hzdw2024.all.p_ads_GeoStats_5964298848`
WHERE 1=1
and date(_PARTITIONTIME) >= date_add(run_date,interval -history_day day)
--and _DATA_DATE='2024-08-15'
--where campaign_id in (21459680538 )
)a 
 join
(SELECT 
	campaign_id,package_name,max(campaign_name) as campaign_name
from scanner-master-android.scanner_ios_dw.camp_ad_keyword_detail
WHERE package_name='pdf.scanner.app'
and stats_date=run_date
group by campaign_id,package_name
)b 
on a.campaign_id=b.campaign_id
left join
(
SELECT cast(Parent_ID as string) as country_criterion_id,upper(max(Country_Code)) as country_code
FROM `scanner-master-android.scanner_ios_dw.geotargets20240813`
group by Parent_ID
)c
on a.country_criterion_id=c.country_criterion_id
left join
(
SELECT stats_date,exchange_rate 
FROM `scanner-master-android.scanner_ios_dw.exchange_rete` 
where 1=1
)d 
on a.stats_date=d.stats_date
group by stats_date,campaign_id,campaign_name,country_code,package_name;


--------以上三个表汇总


delete scanner-master-android.scanner_ios_dw.scanner_ios_keywords_delivery_data
where stats_date>=date_add(run_date,interval -history_day day)
;

insert    scanner-master-android.scanner_ios_dw.scanner_ios_keywords_delivery_data 
SELECT 
	stats_date
	,package_name
	,'Apple Search Ads' as channel
	,'Apple Search Ads' as network
	,country_code
	,campaign_name
	,campaign_id
	,ad_group_name
	,ad_group_id
	,keywords
	,keyword_id
	,impressions
	,taps as clicks
	,total_installs as total_installs
	,view_installs as view_installs
	,tap_installs
	,installs as installs_ads
	,local_spend as cost_usd
FROM `scanner-master-android.scanner_ios_dw.scanner_ios_asa_delivery_data`
WHERE stats_date >= date_add(run_date,interval -history_day day)
union all 
SELECT 
	stats_date
	,package_name
	,'google ads' as channel
	,case when LOWER(campaign_name) like '%search%' then 'google ads search w2a'
	   when LOWER(campaign_name) like '%uac%' then 'google ads uac'  end as network
	,case when campaign_name like '%US%' then 'US' else 'undefined' end country_code
	,campaign_name
	,campaign_id
	,ad_group_name
	,ad_group_id
	,keywords
	,keyword_id
	,impressions
	,clicks
	,0 as total_installs
	,0 as view_installs
  ,0 as tap_installs
	,0 as installs_ads
	,cost_usd as cost_usd
FROM `scanner-master-android.scanner_ios_dw.scanner_ios_googleads_keywords_delivery_data`
WHERE stats_date >= date_add(run_date,interval -history_day day);



--------adjust  安装追踪



delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail
where install_date>=date_add(run_date,interval -history_day day);

insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail 
with c as 
(
	SELECT 
		adid
	FROM `scanner-master-android.analytics_196427335.daily_adjust_report_*` 
	WHERE 1=1
	AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval - history_day day) as string),'-','')
	and activity_kind='event'
	and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -0 day) as string),'-','')
	and tracker_name like 'Google Ads Search::%'
	group by adid
),
  a as(
SELECT
	package_name
	,adid 
	,created_at
	,safe_cast(install_time as TIMESTAMP) as install_time
	,install_date
	,country_code
	,channel
	,network
	,activity_kind
	,external_device_id_md5
	,safe_cast(revenue_usd as float64) as revenue_usd
	,tracker_name
	,case when network like 'google ads%' and c.campaign_name is not null then c.campaign_name else a.campaign_name end as campaign_name
	,a.campaign_id as campaign_id
	,case when network like 'google ads%' and c.ad_group_name is not null then c.ad_group_name else a.ad_group_name end as ad_group_name
	,a.ad_group_id as ad_group_id
	,a.keywords
	,case when network like 'google ads%' and c.keyword_id is not null then c.keyword_id else a.keyword_id end as keyword_id
FROM 
	(
	SELECT
		app_name as package_name
		,a.adid
		,created_at
		,FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(cast(created_at as integer))) AS install_time
		,date(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(cast(created_at as integer)))) AS install_date
		,upper(country) as country_code
		,case when tracker_name like '%Apple Search Ads%' then 'Apple Search Ads'
		    when lower(tracker_name) like '%google ads%' then 'google ads' 
		    when tracker_name like '%Organic%' then 'Organic' 
		    else 'undefined' end as channel
		,case when tracker_name like '%Apple Search Ads%' then 'Apple Search Ads'
			when lower(tracker_name)   like '%google ads search w2a%'  and c.adid is  null  then 'google ads search w2a' 
			when (lower(tracker_name)   like 'google ads search::%'  or c.adid is not null)  then 'google ads search' 
			when tracker_name like '%Organic%' then 'Organic' 
			else 'undefined' end as network
		,activity_kind
		,external_device_id_md5
		,revenue_usd
		,tracker_name
		,case when (tracker_name like '%Apple Search Ads%' or lower(tracker_name)   like 'google ads search::%')  then  REGEXP_EXTRACT(SPLIT(tracker_name, "::")[SAFE_OFFSET(1)], r'^(.*?) \(\d+\)$') 
		    when tracker_name like '%google ads%' then 'undefined' 
		    when tracker_name like '%Organic%' then 'Organic' 
			end AS campaign_name  
		,case when (tracker_name like '%Apple Search Ads%' or lower(tracker_name)   like 'google ads search::%')  then  REGEXP_EXTRACT(SPLIT(tracker_name, "::")[SAFE_OFFSET(1)], r'\((\d+)\)')
		    when tracker_name like '%google ads%' then SPLIT(tracker_name, "::")[SAFE_OFFSET(1)]
		    when tracker_name like '%Organic%' then 'Organic' 
		    end AS campaign_id  
		,case when (tracker_name like '%Apple Search Ads%' or lower(tracker_name)   like 'google ads search::%')  then  REGEXP_EXTRACT(SPLIT(tracker_name, "::")[SAFE_OFFSET(2)], r'^(.*?) \(\d+\)$') 
		    when tracker_name like '%google ads%' then 'undefined' 
		    when tracker_name like '%Organic%' then 'Organic' 
			end AS ad_group_name  
		,case when (tracker_name like '%Apple Search Ads%' or lower(tracker_name)   like 'google ads search::%')  then  REGEXP_EXTRACT(SPLIT(tracker_name, "::")[SAFE_OFFSET(2)], r'\((\d+)\)')
		    when tracker_name like '%google ads%' then SPLIT(tracker_name, "::")[SAFE_OFFSET(2)]
		    when tracker_name like '%Organic%' then 'Organic' 
		    end AS ad_group_id  
		,case when tracker_name like '%Apple Search Ads%'   then  REGEXP_EXTRACT(SPLIT(tracker_name, "::")[SAFE_OFFSET(3)], r'^(.*?) \(\d+\)$') 
		when lower(tracker_name)   like 'google ads search::%'  then  REGEXP_EXTRACT(SPLIT(tracker_name, "::")[SAFE_OFFSET(3)], r'^(\w+)(?: \(\w+\))?$') 
		    when tracker_name like '%google ads%' then SPLIT(tracker_name, "::")[SAFE_OFFSET(3)]
		    when tracker_name like '%Organic%' then 'Organic' 
			end AS keywords  
		,case when (tracker_name like '%Apple Search Ads%' or lower(tracker_name)   like 'google ads search::%')  then  REGEXP_EXTRACT(SPLIT(tracker_name, "::")[SAFE_OFFSET(3)], r'\((\d+)\)')
		    when tracker_name like '%google ads%' then 'undefined'
		    when tracker_name like '%Organic%' then 'Organic' 
		    end AS keyword_id  

	FROM `scanner-master-android.analytics_196427335.daily_adjust_report_*` a
	left join c on a.adid=c.adid
	WHERE 1=1
	AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
	and activity_kind='install'
	--	and a.adid='9f5eb194e55893b442c907ccec66920e'
	--and _TABLE_SUFFIX <replace(cast(date_add(run_date,interval -0 day) as string),'-','')
	)a 
	left join
	(
	SELECT 
		max(keyword_id) as keyword_id,keywords,campaign_id,campaign_name,ad_group_name,ad_group_id
	from scanner-master-android.scanner_ios_dw.camp_ad_keyword_detail
	WHERE package_name='pdf.scanner.app'
	and stats_date=run_date
	group by keywords,campaign_id,campaign_name,ad_group_name,ad_group_id,package_name
	)c
	on a.keywords=c.keywords
	and a.ad_group_id=c.ad_group_id
	and a.campaign_id=c.campaign_id
	)
SELECT
	package_name
	,adid 
	,created_at
	,install_time
	,install_date
	,country_code
	,channel
	,network
	,activity_kind
	,external_device_id_md5
	,revenue_usd
	,tracker_name
	,CASE when a.campaign_name like '%unknown%' then b.campaign_name else a.campaign_name end as campaign_name
	,a.campaign_id
	,CASE when a.ad_group_name like '%unknown%' then b.ad_group_name else a.ad_group_name end as ad_group_name
	,a.ad_group_id
	,CASE when a.keywords like '%unknown%' then b.keywords else a.keywords end as keywords
	,a.keyword_id
FROM 
	(
	select package_name
		,adid 
		,created_at
		,install_time
		,install_date
		,country_code
		,channel
		,network
		,activity_kind
		,external_device_id_md5
		,revenue_usd
		,tracker_name
		,campaign_name
		,campaign_id
		,ad_group_name
		,ad_group_id
		,keywords
		,keyword_id
	FROM a  
	)a
  left join
	(
	select 
		campaign_name
		,campaign_id
		,ad_group_name
		,ad_group_id
		,keywords
		,keyword_id
	from a 
	where campaign_name not like '%unknown%' 
	and ad_group_name not like '%unknown%'
	and keywords not like '%unknown%'
	group by campaign_name
		,campaign_id
		,ad_group_name
		,ad_group_id
		,keywords
		,keyword_id
	)b 
	on a.keyword_id=b.keyword_id;



----------adjust事件明细

delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_event_detail
where event_date>=date_add(run_date,interval -history_day day);

insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_event_detail 
SELECT
	package_name
	,a.adid 
	,created_at
	,safe_cast(event_time as TIMESTAMP) as event_time
	,event_date
	,event_name
	,product_id
	,revenue_usd
FROM
	(
	SELECT 
		app_name as package_name
		,adid
		,created_at
		,FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(cast(created_at as integer))) AS event_time
		,date(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(cast(created_at as integer)))) AS event_date
		--,activity_kind
		,event_name 
		,product_id 
		,safe_cast(revenue_usd as float64) as revenue_usd
	FROM `scanner-master-android.analytics_196427335.daily_adjust_report_*` 
	WHERE 1=1
	AND _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
	and _TABLE_SUFFIX>='20240819'
	and _TABLE_SUFFIX <replace(cast(date_add(run_date,interval -0 day) as string),'-','')
	and activity_kind='event'
	)a 
	left join 
	(
	SELECT
		adid
		,install_date
	FROM	scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail 
	)b 
	on a.adid=b.adid
	ORDER by a.adid;




------adjust汇总 关键词统计


delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_keywords_delivery_data
where stats_date>='2024-08-19';


		insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_keywords_delivery_data 
		SELECT
			a.install_date stats_date
			,a.package_name
			,a.channel
			,a.network
			,a.country_code
			,a.campaign_name
			,a.campaign_id
			,a.ad_group_name
			,a.ad_group_id
			,a.keywords
			,a.keyword_id
			,a.install_uv
			,a.try_uv
			,a.pay_uv
			,a.try_convert_uv
			,a.real_revenue
			,trial_nocanceled_6h_uv
		FROM
			(
		SELECT
			install_date 
			,package_name
			,channel
			,network
			,country_code
			,campaign_name
			,campaign_id
			,ad_group_name
			,ad_group_id
			,keywords
			,keyword_id
			,count(distinct adid) as install_uv 
			,count(distinct case when event_name='trial_started' then adid else null end) as try_uv
			,count(distinct case when event_name='payoff' then adid else null end) as pay_uv 
			,count(distinct case when event_name='trial_started' and next_event_name='payoff' then adid else null end) as try_convert_uv 
			--,count(distinct case when event_name='trial_converted'  then adid else null end) as try_convert_uv 
			,sum(revenue_usd) as real_revenue
		FROM
			(
			SELECT
				a.install_date
        		,package_name
				,a.adid
				,event_date
				,event_name
				,revenue_usd
				,product_id
				,row_number() over(partition by a.adid,event_name order by event_date) as rn 
				,row_number() over(partition by a.adid,event_name order by event_date desc) as rn_desc 
				,lead(event_name) over(partition by a.adid order by event_date,event_name desc) as next_event_name
				,lag(event_name) over(partition by a.adid order by event_date,event_name desc) as last_event_name
				,channel
				,network
				,country_code
				,campaign_name
				,campaign_id
				,ad_group_name
				,ad_group_id
				,keywords
				,keyword_id
			FROM
				(
				SELECT
					adid
					,package_name
					,min(install_date) as install_date
					,max(channel) as channel
					,max(network) as network
					,max(country_code) as country_code
					,max(campaign_name) as campaign_name
					,max(campaign_id) as campaign_id
					,max(ad_group_name) as ad_group_name
					,max(ad_group_id) as ad_group_id
					,max(keywords) as keywords
					,max(keyword_id) as keyword_id
				FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail 
				WHERE 1=1 
				AND  install_date>= '2024-08-19'
			  	group by adid,package_name
				)a 
				left join
				(
				SELECT
					event_date
					,adid
					,event_name 
					,revenue_usd
					,product_id
				FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_event_detail 
				WHERE 1=1 
				AND  event_date>= '2024-08-19'
				--and adid='15ef0d7106e9fc995c48f1bfe9acb107'
				and event_name in ('trial_started','payoff')
				group by event_date,adid,event_name,revenue_usd,product_id
				)b 
				on a.adid=b.adid 
				WHERE 1=1
				and (b.event_date is null or event_date>=install_date)
				order by a.adid,rn
			)c 
			group by install_date,package_name,channel
			,network
				,country_code
				,campaign_name
				,campaign_id
				,ad_group_name
				,ad_group_id
				,keywords
				,keyword_id
				)a 
			left join
			(
					SELECT
					install_date 
					,package_name
					,channel
					,network
					,country_code
					,campaign_name
					,campaign_id
					,ad_group_name
					,ad_group_id
					,keywords
					,keyword_id
					,count(distinct adid) as install_uv 
					,count(distinct case when event_name='trial_started' then adid else null end) as try_uv
					,count(distinct case when event_name='payoff' then adid else null end) as pay_uv 
					,count(distinct case when event_name='trial_started' and next_event_name='trial_nocanceled_6h' then adid else null end) as trial_nocanceled_6h_uv 
					--,count(distinct case when event_name='trial_converted'  then adid else null end) as try_convert_uv 
					,sum(revenue_usd) as real_revenue
				FROM
					(
					SELECT
						a.install_date
		        		,package_name
						,a.adid
						,event_date
						,event_name
						,revenue_usd
						,product_id
						,row_number() over(partition by a.adid,event_name order by event_date) as rn 
						,row_number() over(partition by a.adid,event_name order by event_date desc) as rn_desc 
						,lead(event_name) over(partition by a.adid order by event_date,event_name desc) as next_event_name
						,lag(event_name) over(partition by a.adid order by event_date,event_name desc) as last_event_name
						,channel
						,network
						,country_code
						,campaign_name
						,campaign_id
						,ad_group_name
						,ad_group_id
						,keywords
						,keyword_id
					FROM
						(
						SELECT
							adid
							,package_name
							,min(install_date) as install_date
							,max(channel) as channel
							,max(network) as network
							,max(country_code) as country_code
							,max(campaign_name) as campaign_name
							,max(campaign_id) as campaign_id
							,max(ad_group_name) as ad_group_name
							,max(ad_group_id) as ad_group_id
							,max(keywords) as keywords
							,max(keyword_id) as keyword_id
						FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail 
						WHERE 1=1 
						AND  install_date>= '2024-08-19'
					  	group by adid,package_name
						)a 
						left join
						(
						SELECT
							event_date
							,adid
							,event_name 
							,revenue_usd
							,product_id
						FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_event_detail 
						WHERE 1=1 
						AND  event_date>= '2024-08-19'
						--and adid='15ef0d7106e9fc995c48f1bfe9acb107'
						and event_name in ('trial_started','trial_nocanceled_6h')
						group by event_date,adid,event_name,revenue_usd,product_id
						)b 
						on a.adid=b.adid 
						WHERE 1=1
						and (b.event_date is null or event_date>=install_date)
						order by a.adid,rn
					)c 
					group by install_date,package_name,channel
					,network
						,country_code
						,campaign_name
						,campaign_id
						,ad_group_name
						,ad_group_id
						,keywords
						,keyword_id
					)b 
			on a.install_date=b.install_date
			and a.package_name=b.package_name
			and a.channel=b.channel
			and a.network=b.network
			and a.country_code=b.country_code
			and a.campaign_name=b.campaign_name
			and a.campaign_id=b.campaign_id
			and a.ad_group_name=b.ad_group_name
			and a.ad_group_id=b.ad_group_id
			and a.keywords=b.keywords
			and a.keyword_id=b.keyword_id;




------adjust及广告端汇总 关键词统计


delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_delivery_data
where stats_date>='2024-08-19';




insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_delivery_data 
	SELECT
 		stats_date
		,package_name
		,channel
		,network
		,country_code
		,campaign_name
		,campaign_id
		,ad_group_name
		,ad_group_id
		,keywords
		,keyword_id
		,sum(impressions) as impressions
		,sum(clicks) as clicks
		,sum(total_installs) as total_installs
		,sum(view_installs) as view_installs
		,sum(tap_installs) as tap_installs
		,sum(installs_ads) as installs_ads
		,sum(cost_usd) as cost_usd
		,sum(adjust_install_uv) as adjust_install_uv
		,sum(try_uv) as try_uv
		,sum(pay_uv) as pay_uv
		,sum(try_convert_uv) as try_convert_uv
		,sum(real_revenue)*0.7 as real_revenue
		,sum(trial_nocanceled_6h_uv) as trial_nocanceled_6h_uv
	FROM
		(
		SELECT
			 stats_date
			,package_name
			,channel
			,network
			,country_code
			,campaign_name
			,campaign_id
			,ad_group_name
			,ad_group_id
			,keywords
			,keyword_id
			,impressions
			,clicks
			,total_installs
			,view_installs
			,tap_installs
			,installs_ads
			,cost_usd
			,0 as adjust_install_uv
			,0 as try_uv
			,0 as pay_uv
			,0 as try_convert_uv
			,0 as real_revenue
			,0 as trial_nocanceled_6h_uv
		FROM scanner-master-android.scanner_ios_dw.scanner_ios_keywords_delivery_data 
		WHERE stats_date>='2024-08-19'
		union all 
		SELECT
			 stats_date
			,package_name
			,channel
			,network
			,country_code
			,campaign_name
			,campaign_id
			,ad_group_name
			,ad_group_id
			,keywords
			,keyword_id
			,0 as impressions
			,0 as clicks
			,0 as total_installs
			,0 as view_installs
			,0 as tap_installs
			,0 as installs
			,0 as cost_usd
			,install_uv as adjust_install_uv
			,try_uv
			,pay_uv
			,try_convert_uv
			,real_revenue
			,trial_nocanceled_6h_uv
		FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_keywords_delivery_data
	)b
		group by stats_date
		,package_name
		,channel
		,network
		,country_code
		,campaign_name
		,campaign_id
		,ad_group_name
		,ad_group_id
		,keywords
		,keyword_id;


	


---------周包预测收入





delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_week_forcast_data
where install_date>='2024-08-19';




			insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_week_forcast_data 
			SELECT
				install_date
				,package_name
				,channel
				,network
				,country_code
				,campaign_name
				,campaign_id
				,ad_group_name
				,ad_group_id
				,keywords
				,keyword_id
				,SUM(real_revenue) AS real_revenue
				,SUM(month_1_revenue) AS month_1_revenue
				,SUM(month_3_revenue) AS month_3_revenue
				,SUM(month_6_revenue) AS month_6_revenue
				,SUM(month_9_revenue) AS month_9_revenue
				,SUM(month_12_revenue) AS month_12_revenue
				,SUM(month_13_revenue) AS month_13_revenue
				,SUM(month_15_revenue) AS month_15_revenue
			FROM
				(
				SELECT
					install_date
					,package_name
					,channel
					,network
					,country_code
					,campaign_name
					,campaign_id
					,ad_group_name
					,ad_group_id
					,keywords
					,keyword_id
					,product_id
					,remain_days
					,max_week_num
					,real_revenue
					,new_week_revenue
					,last_week_revenue
					,forecast_real_revunue
					,ratio_1_month
					,ratio_3_month
					,ratio_6_month
					,ratio_9_month
					,ratio_12_month
					,ratio_13_month
					,ratio_15_month
					,true_1_month_revune
					,true_3_month_revune
					,true_6_month_revune
					,true_9_month_revune
					,true_12_month_revune
					,true_13_month_revune
					,true_15_month_revune
					,case when ratio_1_month>1 then forecast_real_revunue*ratio_1_month else true_1_month_revune end as month_1_revenue
					,case when ratio_3_month>1 then forecast_real_revunue*ratio_3_month else true_3_month_revune end as month_3_revenue
					,case when ratio_6_month>1 then forecast_real_revunue*ratio_6_month else true_6_month_revune end as month_6_revenue
					,case when ratio_9_month>1 then forecast_real_revunue*ratio_9_month else true_9_month_revune end as month_9_revenue
					,case when ratio_12_month>1 then forecast_real_revunue*ratio_12_month else true_12_month_revune end as month_12_revenue
					,case when ratio_13_month>1 then forecast_real_revunue*ratio_13_month else true_13_month_revune end as month_13_revenue
					,case when ratio_15_month>1 then forecast_real_revunue*ratio_15_month else true_15_month_revune end as month_15_revenue
				FROM
					(
					SELECT
						install_date
						,package_name
						,product_id
						,channel
						,network
						,country_code
						,campaign_name
						,campaign_id
						,ad_group_name
						,ad_group_id
						,keywords
						,keyword_id
						,remain_days
						,max_week_num
						,real_revenue
						,new_week_revenue
						,last_week_revenue
						,case when new_week_revenue>last_week_revenue*new_week_revenue_ratio then real_revenue 
						when new_week_revenue<=last_week_revenue*new_week_revenue_ratio and remain_days>2 then real_revenue - new_week_revenue + last_week_revenue*0.9
						else real_revenue end as forecast_real_revunue
						,ratio_1_month
						,ratio_3_month
						,ratio_6_month
						,ratio_9_month
						,ratio_12_month
						,ratio_13_month
						,ratio_15_month
						,true_1_month_revune
						,true_3_month_revune
						,true_6_month_revune
						,true_9_month_revune
						,true_12_month_revune
						,true_13_month_revune
						,true_15_month_revune
					FROM
						(
						SELECT
							install_date
							,c.package_name
							,c.product_id
							,channel
							,network
							,country_code
							,campaign_name
							,campaign_id
							,ad_group_name
							,ad_group_id
							,keywords
							,keyword_id
							,remain_days
							,max_week_num
							,real_revenue
							,new_week_revenue
							,last_week_revenue

							,max(case when ren_num=max_week_num then ren_ratio else null end ) as new_week_revenue_ratio
							,safe_divide(sum(case when ren_num<=5 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_1_month
							,safe_divide(sum(case when ren_num<=13 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_3_month
							,safe_divide(sum(case when ren_num<=26 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_6_month
							,safe_divide(sum(case when ren_num<=39 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_9_month
							,safe_divide(sum(case when ren_num<=52 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_12_month
							,safe_divide(sum(case when ren_num<=57 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_13_month
							,safe_divide(sum(case when ren_num<=65 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_15_month
							,true_1_month_revune
							,true_3_month_revune
							,true_6_month_revune
							,true_9_month_revune
							,true_12_month_revune
							,true_13_month_revune
							,true_15_month_revune
						FROM  
							(
							SELECT
								install_date
								,c.package_name
								,c.product_id
								,channel
								,network
								,country_code
								,campaign_name
								,campaign_id
								,ad_group_name
								,ad_group_id
								,keywords
								,keyword_id
								,max_week_num
								,week_days_num
								,6 - week_days_num as remain_days
								,sum(revenue_usd) as real_revenue
								,ifnull(sum(case when week_num=max_week_num then revenue_usd else null end),0) as new_week_revenue
								,ifnull(sum(case when week_num=max_week_num-1 then revenue_usd else null end),0) as last_week_revenue
								,ifnull(sum(case when week_num=max_week_num-2 then revenue_usd else null end),0) as last_2week_revenue
								,sum(case when days_cha<=30 then revenue_usd else null end) as true_1_month_revune
								,sum(case when days_cha<=30*3 then revenue_usd else null end) as true_3_month_revune
								,sum(case when days_cha<=30*6 then revenue_usd else null end) as true_6_month_revune
								,sum(case when days_cha<=30*9 then revenue_usd else null end) as true_9_month_revune
								,sum(case when days_cha<=30*12 then revenue_usd else null end) as true_12_month_revune
								,sum(case when days_cha<=30*13 then revenue_usd else null end) as true_13_month_revune
								,sum(case when days_cha<=30*15 then revenue_usd else null end) as true_15_month_revune
							FROM
								(
								SELECT
									a.install_date
									,package_name
									,a.adid
									,channel
									,network
									,country_code
									,campaign_name
									,campaign_id
									,ad_group_name
									,ad_group_id
									,keywords
									,keyword_id
									,event_date
									,event_name
									,revenue_usd
									,product_id
									,date_diff(date_add(run_date,interval 0 day),install_date,day) as days_cha_today
									,date_diff(event_date,install_date,day) as days_cha
									,floor(date_diff(event_date,install_date,day)/7)+1 as week_num
									,floor(date_diff(date_add(run_date,interval 0 day),install_date,day)/7)+1 as max_week_num
									,mod(date_diff(date_add(run_date,interval 0 day),install_date,day),7) as week_days_num
								FROM
									(
									SELECT
										adid
										,package_name
										,min(install_date) as install_date
										,max(channel) as channel
										,max(network) as network
										,max(country_code) as country_code
										,max(campaign_name) as campaign_name
										,max(campaign_id) as campaign_id
										,max(ad_group_name) as ad_group_name
										,max(ad_group_id) as ad_group_id
										,max(keywords) as keywords
										,max(keyword_id) as keyword_id
									FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail 
									WHERE 1=1 
									AND  install_date>= '2024-08-19'
								  	group by adid,package_name
									)a 
									join
									(
									SELECT
										event_date
										,adid
										,event_name 
										,revenue_usd*0.7 as revenue_usd
										,product_id
									FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_event_detail 
									WHERE 1=1 
									AND  event_date>= '2024-08-19'
			            			and product_id like '%weekly%'
			            			--and product_id='weekly_scanner_app_premium'
									--and adid='03573a7804bc94626c54113e019c8917'
									and event_name in ('payoff')
									group by event_date,adid,event_name,revenue_usd,product_id
									)b 
									on a.adid=b.adid 
									WHERE 1=1
									and (b.event_date is null or event_date>=install_date)

								)c 
								group by 	install_date,package_name,max_week_num,week_days_num,product_id	,channel
											,network
											,country_code
											,campaign_name
											,campaign_id
											,ad_group_name
											,ad_group_id
											,keywords
											,keyword_id
								)c
								left join
								(
								SELECT 
								  package_name
								  ,product_id
								  ,ren_num
								  ,ren_ratio
								  ,first_ren_ratio
								FROM `scanner-master-android.scanner_ios_dw.product_renew_data_2` 
								)d 
								on c.package_name=d.package_name
								and c.product_id=d.product_id
								group by install_date,package_name,product_id,remain_days,max_week_num,real_revenue	,new_week_revenue
							,last_week_revenue	,true_1_month_revune
							,true_3_month_revune
							,true_6_month_revune
							,true_9_month_revune
							,true_12_month_revune
							,true_13_month_revune
							,true_15_month_revune
							,channel
							,network
							,country_code
							,campaign_name
							,campaign_id
							,ad_group_name
							,ad_group_id
							,keywords
							,keyword_id
            			)e
					)f 
				)g 
			group by install_date
				,package_name
				,channel
				,network
				,country_code
				,campaign_name
				,campaign_id
				,ad_group_name
				,ad_group_id
				,keywords
				,keyword_id;





--------月包预测收入





delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_month_forcast_data
where install_date>='2024-08-19';




			insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_month_forcast_data 
			SELECT
				install_date
				,package_name
				,channel
				,network
				,country_code
				,campaign_name
				,campaign_id
				,ad_group_name
				,ad_group_id
				,keywords
				,keyword_id
				,SUM(real_revenue) AS real_revenue
				,SUM(month_1_revenue) AS month_1_revenue
				,SUM(month_3_revenue) AS month_3_revenue
				,SUM(month_6_revenue) AS month_6_revenue
				,SUM(month_9_revenue) AS month_9_revenue
				,SUM(month_12_revenue) AS month_12_revenue
				,SUM(month_13_revenue) AS month_13_revenue
				,SUM(month_15_revenue) AS month_15_revenue
			FROM
				(
				SELECT
					install_date
					,package_name
					,channel
					,network
					,country_code
					,campaign_name
					,campaign_id
					,ad_group_name
					,ad_group_id
					,keywords
					,keyword_id
					,product_id
					,remain_days
					,max_week_num
					,real_revenue
					,new_week_revenue
					,last_week_revenue
					,forecast_real_revunue
					,ratio_1_month
					,ratio_3_month
					,ratio_6_month
					,ratio_9_month
					,ratio_12_month
					,ratio_13_month
					,ratio_15_month
					,true_1_month_revune
					,true_3_month_revune
					,true_6_month_revune
					,true_9_month_revune
					,true_12_month_revune
					,true_13_month_revune
					,true_15_month_revune
					,case when ratio_1_month>1 then forecast_real_revunue*ratio_1_month else true_1_month_revune end as month_1_revenue
					,case when ratio_3_month>1 then forecast_real_revunue*ratio_3_month else true_3_month_revune end as month_3_revenue
					,case when ratio_6_month>1 then forecast_real_revunue*ratio_6_month else true_6_month_revune end as month_6_revenue
					,case when ratio_9_month>1 then forecast_real_revunue*ratio_9_month else true_9_month_revune end as month_9_revenue
					,case when ratio_12_month>1 then forecast_real_revunue*ratio_12_month else true_12_month_revune end as month_12_revenue
					,case when ratio_13_month>1 then forecast_real_revunue*ratio_13_month else true_13_month_revune end as month_13_revenue
					,case when ratio_15_month>1 then forecast_real_revunue*ratio_15_month else true_15_month_revune end as month_15_revenue
				FROM
					(
					SELECT
						install_date
						,package_name
						,product_id
						,channel
						,network
						,country_code
						,campaign_name
						,campaign_id
						,ad_group_name
						,ad_group_id
						,keywords
						,keyword_id
						,remain_days
						,max_week_num
						,real_revenue
						,new_week_revenue
						,last_week_revenue
						,case when new_week_revenue>last_week_revenue*new_week_revenue_ratio then real_revenue 
						when new_week_revenue<=last_week_revenue*new_week_revenue_ratio and remain_days>10 then real_revenue - new_week_revenue + last_week_revenue
						else real_revenue end as forecast_real_revunue
						,ratio_1_month
						,ratio_3_month
						,ratio_6_month
						,ratio_9_month
						,ratio_12_month
						,ratio_13_month
						,ratio_15_month
						,true_1_month_revune
						,true_3_month_revune
						,true_6_month_revune
						,true_9_month_revune
						,true_12_month_revune
						,true_13_month_revune
						,true_15_month_revune
					FROM
						(
						SELECT
							install_date
							,c.package_name
							,c.product_id
							,channel
							,network
							,country_code
							,campaign_name
							,campaign_id
							,ad_group_name
							,ad_group_id
							,keywords
							,keyword_id
							,remain_days
							,max_week_num
							,real_revenue
							,new_week_revenue
							,last_week_revenue

							,max(case when ren_num=max_week_num then ren_ratio else null end ) as new_week_revenue_ratio
							,safe_divide(sum(case when ren_num<=1 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_1_month
							,safe_divide(sum(case when ren_num<=3 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_3_month
							,safe_divide(sum(case when ren_num<=6 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_6_month
							,safe_divide(sum(case when ren_num<=9 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_9_month
							,safe_divide(sum(case when ren_num<=12 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_12_month
							,safe_divide(sum(case when ren_num<=13 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_13_month
							,safe_divide(sum(case when ren_num<=15 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_15_month
							,true_1_month_revune
							,true_3_month_revune
							,true_6_month_revune
							,true_9_month_revune
							,true_12_month_revune
							,true_13_month_revune
							,true_15_month_revune
						FROM  
							(
							SELECT
								install_date
								,c.package_name
								,c.product_id
								,channel
								,network
								,country_code
								,campaign_name
								,campaign_id
								,ad_group_name
								,ad_group_id
								,keywords
								,keyword_id
								,max_week_num
								,week_days_num
								,31 - week_days_num as remain_days
								,sum(revenue_usd) as real_revenue
								,ifnull(sum(case when week_num=max_week_num then revenue_usd else null end),0) as new_week_revenue
								,ifnull(sum(case when week_num=max_week_num-1 then revenue_usd else null end),0) as last_week_revenue
								,ifnull(sum(case when week_num=max_week_num-2 then revenue_usd else null end),0) as last_2week_revenue
								,sum(case when days_cha<=31 then revenue_usd else null end) as true_1_month_revune
								,sum(case when days_cha<=31*3 then revenue_usd else null end) as true_3_month_revune
								,sum(case when days_cha<=31*6 then revenue_usd else null end) as true_6_month_revune
								,sum(case when days_cha<=31*9 then revenue_usd else null end) as true_9_month_revune
								,sum(case when days_cha<=31*12 then revenue_usd else null end) as true_12_month_revune
								,sum(case when days_cha<=31*13 then revenue_usd else null end) as true_13_month_revune
								,sum(case when days_cha<=31*15 then revenue_usd else null end) as true_15_month_revune
							FROM
								(
								SELECT
									a.install_date
									,package_name
									,a.adid
									,channel
									,network
									,country_code
									,campaign_name
									,campaign_id
									,ad_group_name
									,ad_group_id
									,keywords
									,keyword_id
									,event_date
									,event_name
									,revenue_usd
									,product_id
									,date_diff(date_add(run_date,interval 0 day),install_date,day) as days_cha_today
									,date_diff(event_date,install_date,day) as days_cha
									,floor(date_diff(event_date,install_date,day)/31)+1 as week_num
									,floor(date_diff(date_add(run_date,interval 0 day),install_date,day)/31)+1 as max_week_num
									,mod(date_diff(date_add(run_date,interval 0 day),install_date,day),31) as week_days_num
								FROM
									(
									SELECT
										adid
										,package_name
										,min(install_date) as install_date
										,max(channel) as channel
										,max(network) as network
										,max(country_code) as country_code
										,max(campaign_name) as campaign_name
										,max(campaign_id) as campaign_id
										,max(ad_group_name) as ad_group_name
										,max(ad_group_id) as ad_group_id
										,max(keywords) as keywords
										,max(keyword_id) as keyword_id
									FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail 
									WHERE 1=1 
									AND  install_date>= '2024-08-19'
								  	group by adid,package_name
									)a 
									join
									(
									SELECT
										event_date
										,adid
										,event_name 
										,revenue_usd*0.7 as revenue_usd
										,product_id
									FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_event_detail 
									WHERE 1=1 
									AND  event_date>= '2024-08-19'
			            			and product_id like '%month%'
									and event_name in ('payoff')
									group by event_date,adid,event_name,revenue_usd,product_id
									)b 
									on a.adid=b.adid 
									WHERE 1=1
									and (b.event_date is null or event_date>=install_date)

								)c 
								group by 	install_date,package_name,max_week_num,week_days_num,product_id	,channel
											,network
											,country_code
											,campaign_name
											,campaign_id
											,ad_group_name
											,ad_group_id
											,keywords
											,keyword_id
								)c
								left join
								(
								SELECT 
								  package_name
								  ,product_id
								  ,ren_num
								  ,ren_ratio
								  ,first_ren_ratio
								FROM `scanner-master-android.scanner_ios_dw.product_renew_data_2` 
								)d 
								on c.package_name=d.package_name
								and c.product_id=d.product_id
								group by install_date,package_name,product_id,remain_days,max_week_num,real_revenue	,new_week_revenue
							,last_week_revenue	,true_1_month_revune
							,true_3_month_revune
							,true_6_month_revune
							,true_9_month_revune
							,true_12_month_revune
							,true_13_month_revune
							,true_15_month_revune
							,channel
							,network
							,country_code
							,campaign_name
							,campaign_id
							,ad_group_name
							,ad_group_id
							,keywords
							,keyword_id
            			)e
					)f 
				)g 
			group by install_date
				,package_name
				,channel
				,network
				,country_code
				,campaign_name
				,campaign_id
				,ad_group_name
				,ad_group_id
				,keywords
				,keyword_id;




--------年包预测收入





delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_year_forcast_data
where install_date>='2024-08-19';




			insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_year_forcast_data 
			SELECT
				install_date
				,package_name
				,channel
				,network
				,country_code
				,campaign_name
				,campaign_id
				,ad_group_name
				,ad_group_id
				,keywords
				,keyword_id
				,SUM(real_revenue) AS real_revenue
				,SUM(month_1_revenue) AS month_1_revenue
				,SUM(month_3_revenue) AS month_3_revenue
				,SUM(month_6_revenue) AS month_6_revenue
				,SUM(month_9_revenue) AS month_9_revenue
				,SUM(month_12_revenue) AS month_12_revenue
				,SUM(month_13_revenue) AS month_13_revenue
				,SUM(month_15_revenue) AS month_15_revenue
			FROM
				(
				SELECT
					install_date
					,package_name
					,channel
					,network
					,country_code
					,campaign_name
					,campaign_id
					,ad_group_name
					,ad_group_id
					,keywords
					,keyword_id
					,product_id
					,remain_days
					,max_week_num
					,real_revenue
					,new_week_revenue
					,last_week_revenue
					,forecast_real_revunue
					,ratio_1_month
					,ratio_3_month
					,ratio_6_month
					,ratio_9_month
					,ratio_12_month
					,ratio_13_month
					,ratio_15_month
					,true_1_month_revune
					,true_3_month_revune
					,true_6_month_revune
					,true_9_month_revune
					,true_12_month_revune
					,true_13_month_revune
					,true_15_month_revune
					,case when ratio_1_month>1 then forecast_real_revunue*ratio_1_month else true_1_month_revune end as month_1_revenue
					,case when ratio_3_month>1 then forecast_real_revunue*ratio_3_month else true_3_month_revune end as month_3_revenue
					,case when ratio_6_month>1 then forecast_real_revunue*ratio_6_month else true_6_month_revune end as month_6_revenue
					,case when ratio_9_month>1 then forecast_real_revunue*ratio_9_month else true_9_month_revune end as month_9_revenue
					,case when ratio_12_month>1 then forecast_real_revunue*ratio_12_month else true_12_month_revune end as month_12_revenue
					,case when ratio_13_month>1 then forecast_real_revunue*ratio_13_month else true_13_month_revune end as month_13_revenue
					,case when ratio_15_month>1 then forecast_real_revunue*ratio_15_month else true_15_month_revune end as month_15_revenue
				FROM
					(
					SELECT
						install_date
						,package_name
						,product_id
						,channel
						,network
						,country_code
						,campaign_name
						,campaign_id
						,ad_group_name
						,ad_group_id
						,keywords
						,keyword_id
						,remain_days
						,max_week_num
						,real_revenue
						,new_week_revenue
						,last_week_revenue
						,case when new_week_revenue>last_week_revenue*new_week_revenue_ratio then real_revenue 
						when new_week_revenue<=last_week_revenue*new_week_revenue_ratio and remain_days>2 then real_revenue - new_week_revenue + last_week_revenue
						else real_revenue end as forecast_real_revunue
						,ratio_1_month
						,ratio_3_month
						,ratio_6_month
						,ratio_9_month
						,ratio_12_month
						,ratio_13_month
						,ratio_15_month
						,true_1_month_revune
						,true_3_month_revune
						,true_6_month_revune
						,true_9_month_revune
						,true_12_month_revune
						,true_13_month_revune
						,true_15_month_revune
					FROM
						(
						SELECT
							install_date
							,c.package_name
							,c.product_id
							,channel
							,network
							,country_code
							,campaign_name
							,campaign_id
							,ad_group_name
							,ad_group_id
							,keywords
							,keyword_id
							,remain_days
							,max_week_num
							,real_revenue
							,new_week_revenue
							,last_week_revenue

							,max(case when ren_num=max_week_num then ren_ratio else null end ) as new_week_revenue_ratio
							,safe_divide(sum(case when ren_num<=1 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_1_month
							,safe_divide(sum(case when ren_num<=1 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_3_month
							,safe_divide(sum(case when ren_num<=1 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_6_month
							,safe_divide(sum(case when ren_num<=1 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_9_month
							,safe_divide(sum(case when ren_num<=1 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_12_month
							,safe_divide(sum(case when ren_num<=2 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_13_month
							,safe_divide(sum(case when ren_num<=2 then first_ren_ratio else 0 end ),sum(case when ren_num<=max_week_num then first_ren_ratio else 0 end )) as ratio_15_month
							,true_1_month_revune
							,true_3_month_revune
							,true_6_month_revune
							,true_9_month_revune
							,true_12_month_revune
							,true_13_month_revune
							,true_15_month_revune
						FROM  
							(
							SELECT
								install_date
								,c.package_name
								,c.product_id
								,channel
								,network
								,country_code
								,campaign_name
								,campaign_id
								,ad_group_name
								,ad_group_id
								,keywords
								,keyword_id
								,max_week_num
								,week_days_num
								,31 - week_days_num as remain_days
								,sum(revenue_usd) as real_revenue
								,ifnull(sum(case when week_num=max_week_num then revenue_usd else null end),0) as new_week_revenue
								,ifnull(sum(case when week_num=max_week_num-1 then revenue_usd else null end),0) as last_week_revenue
								,ifnull(sum(case when week_num=max_week_num-2 then revenue_usd else null end),0) as last_2week_revenue
								,sum(case when days_cha<=31 then revenue_usd else null end) as true_1_month_revune
								,sum(case when days_cha<=31*3 then revenue_usd else null end) as true_3_month_revune
								,sum(case when days_cha<=31*6 then revenue_usd else null end) as true_6_month_revune
								,sum(case when days_cha<=31*9 then revenue_usd else null end) as true_9_month_revune
								,sum(case when days_cha<=31*12 then revenue_usd else null end) as true_12_month_revune
								,sum(case when days_cha<=31*13 then revenue_usd else null end) as true_13_month_revune
								,sum(case when days_cha<=31*15 then revenue_usd else null end) as true_15_month_revune
							FROM
								(
								SELECT
									a.install_date
									,package_name
									,a.adid
									,channel
									,network
									,country_code
									,campaign_name
									,campaign_id
									,ad_group_name
									,ad_group_id
									,keywords
									,keyword_id
									,event_date
									,event_name
									,revenue_usd
									,product_id
									,date_diff(date_add(run_date,interval 0 day),install_date,day) as days_cha_today
									,date_diff(event_date,install_date,day) as days_cha
									,floor(date_diff(event_date,install_date,day)/365)+1 as week_num
									,floor(date_diff(date_add(run_date,interval 0 day),install_date,day)/365)+1 as max_week_num
									,mod(date_diff(date_add(run_date,interval 0 day),install_date,day),365) as week_days_num
								FROM
									(
									SELECT
										adid
										,package_name
										,min(install_date) as install_date
										,max(channel) as channel
										,max(network) as network
										,max(country_code) as country_code
										,max(campaign_name) as campaign_name
										,max(campaign_id) as campaign_id
										,max(ad_group_name) as ad_group_name
										,max(ad_group_id) as ad_group_id
										,max(keywords) as keywords
										,max(keyword_id) as keyword_id
									FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail 
									WHERE 1=1 
									AND  install_date>= '2024-08-19'
								  	group by adid,package_name
									)a 
									join
									(
									SELECT
										event_date
										,adid
										,event_name 
										,revenue_usd*0.7 as revenue_usd
										,product_id
									FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_event_detail 
									WHERE 1=1 
									AND  event_date>= '2024-08-19'
			            			and product_id like '%year%'
									and event_name in ('payoff')
									group by event_date,adid,event_name,revenue_usd,product_id
									)b 
									on a.adid=b.adid 
									WHERE 1=1
									and (b.event_date is null or event_date>=install_date)

								)c 
								group by 	install_date,package_name,max_week_num,week_days_num,product_id	,channel
											,network
											,country_code
											,campaign_name
											,campaign_id
											,ad_group_name
											,ad_group_id
											,keywords
											,keyword_id
								)c
								left join
								(
								SELECT 
								  package_name
								  ,product_id
								  ,ren_num
								  ,ren_ratio
								  ,first_ren_ratio
								FROM `scanner-master-android.scanner_ios_dw.product_renew_data_2` 
								)d 
								on c.package_name=d.package_name
								and c.product_id=d.product_id
								group by install_date,package_name,product_id,remain_days,max_week_num,real_revenue	,new_week_revenue
							,last_week_revenue	,true_1_month_revune
							,true_3_month_revune
							,true_6_month_revune
							,true_9_month_revune
							,true_12_month_revune
							,true_13_month_revune
							,true_15_month_revune
							,channel
							,network
							,country_code
							,campaign_name
							,campaign_id
							,ad_group_name
							,ad_group_id
							,keywords
							,keyword_id
            			)e
					)f 
				)g 
			group by install_date
				,package_name
				,channel
				,network
				,country_code
				,campaign_name
				,campaign_id
				,ad_group_name
				,ad_group_id
				,keywords
				,keyword_id;





---------单次套餐
delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_lifetime_forcast_data
where install_date>='2024-08-19';




			insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_lifetime_forcast_data 
	
								SELECT
									install_date
									,package_name
									,channel
									,network
									,country_code
									,campaign_name
									,campaign_id
									,ad_group_name
									,ad_group_id
									,keywords
									,keyword_id
									,SUM(revenue_usd) AS real_revenue
									,SUM(revenue_usd) AS month_1_revenue
									,SUM(revenue_usd) AS month_3_revenue
									,SUM(revenue_usd) AS month_6_revenue
									,SUM(revenue_usd) AS month_9_revenue
									,SUM(revenue_usd) AS month_12_revenue
									,SUM(revenue_usd) AS month_13_revenue
									,SUM(revenue_usd) AS month_15_revenue
								FROM
									(
									SELECT
										adid
										,package_name
										,min(install_date) as install_date
										,max(channel) as channel
										,max(network) as network
										,max(country_code) as country_code
										,max(campaign_name) as campaign_name
										,max(campaign_id) as campaign_id
										,max(ad_group_name) as ad_group_name
										,max(ad_group_id) as ad_group_id
										,max(keywords) as keywords
										,max(keyword_id) as keyword_id
									FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_install_detail 
									WHERE 1=1 
									AND  install_date>= '2024-08-19'
								  	group by adid,package_name
									)a 
									join
									(
									SELECT
										event_date
										,adid
										,event_name 
										,revenue_usd*0.7 as revenue_usd
										,product_id
									FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_event_detail 
									WHERE 1=1 
									AND  event_date>= '2024-08-19'
			            			and product_id not like '%year%'
			            			and product_id not like '%weekly%'
			            			and product_id not like '%month%'
									and event_name in ('payoff')
									group by event_date,adid,event_name,revenue_usd,product_id
									)b 
									on a.adid=b.adid 
									WHERE 1=1
									and (b.event_date is null or event_date>=install_date)

					
								group by 	install_date,package_name,channel
											,network
											,country_code
											,campaign_name
											,campaign_id
											,ad_group_name
											,ad_group_id
											,keywords
											,keyword_id;

----------所有预测收入
delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_all_forcast_data
where install_date>='2024-08-19';




			insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_all_forcast_data 
								SELECT
									install_date
									,package_name
									,channel
									,network
									,country_code
									,campaign_name
									,campaign_id
									,ad_group_name
									,ad_group_id
									,keywords
									,keyword_id
									,SUM(real_revenue) AS real_revenue
									,SUM(month_1_revenue) AS month_1_revenue
									,SUM(month_3_revenue) AS month_3_revenue
									,SUM(month_6_revenue) AS month_6_revenue
									,SUM(month_9_revenue) AS month_9_revenue
									,SUM(month_12_revenue) AS month_12_revenue
									,SUM(month_13_revenue) AS month_13_revenue
									,SUM(month_15_revenue) AS month_15_revenue
								FROM
									(
										SELECT 
										* 
										FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_week_forcast_data
										UNION ALL 
										SELECT 
										* 
										FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_month_forcast_data
										UNION ALL 
										SELECT 
										* 
										FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_year_forcast_data
										UNION ALL 
										SELECT 
										* 
										FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_lifetime_forcast_data

										)a 
									group by install_date
									,package_name
									,channel
									,network
									,country_code
									,campaign_name
									,campaign_id
									,ad_group_name
									,ad_group_id
									,keywords
									,keyword_id;


									-------大表合并
delete scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_all_delivery_forcast_data
where install_date>='2024-08-19';




			insert    scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_all_delivery_forcast_data 
 	SELECT
 		stats_date
		,package_name
		,channel
		,network
		,country_code
		,campaign_name
		,campaign_id
		,ad_group_name
		,ad_group_id
		,keywords
		,keyword_id
		,sum(impressions) as impressions
		,sum(clicks) as clicks
		,sum(total_installs) as total_installs
		,sum(view_installs) as view_installs
		,sum(tap_installs) as tap_installs
		,sum(installs_ads) as installs_ads
		,sum(cost_usd) as cost_usd
		,sum(adjust_install_uv) as adjust_install_uv
		,sum(try_uv) as try_uv
		,sum(pay_uv) as pay_uv
		,sum(try_convert_uv) as try_convert_uv
		,sum(real_revenue) as real_revenue
		,SUM(month_1_revenue) AS month_1_revenue
		,SUM(month_3_revenue) AS month_3_revenue
		,SUM(month_6_revenue) AS month_6_revenue
		,SUM(month_9_revenue) AS month_9_revenue
		,SUM(month_12_revenue) AS month_12_revenue
		,SUM(month_13_revenue) AS month_13_revenue
		,SUM(month_15_revenue) AS month_15_revenue
		,SUM(trial_nocanceled_6h_uv) AS trial_nocanceled_6h_uv
	FROM
		(
		SELECT
			 stats_date
			,package_name
			,channel
			,network
			,country_code
			,campaign_name
			,campaign_id
			,ad_group_name
			,ad_group_id
			,keywords
			,keyword_id
			,impressions
			,clicks
			,total_installs
			,view_installs
			,tap_installs
			,installs_ads
			,cost_usd
			,adjust_install_uv
			,try_uv
			,pay_uv
			,try_convert_uv
			,real_revenue
			,0 as month_1_revenue
			,0 as month_3_revenue
			,0 as month_6_revenue
			,0 as month_9_revenue
			,0 as month_12_revenue
			,0 as month_13_revenue
			,0 as month_15_revenue
			,trial_nocanceled_6h_uv
		FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_delivery_data 
		WHERE stats_date>='2024-08-19'
		union all 
		SELECT
			install_date as  stats_date
			,package_name
			,channel
			,network
			,country_code
			,campaign_name
			,campaign_id
			,ad_group_name
			,ad_group_id
			,keywords
			,keyword_id
			,0 as impressions
			,0 as clicks
			,0 as total_installs
			,0 as view_installs
			,0 as tap_installs
			,0 as installs_ads
			,0 as cost_usd
			,0 as adjust_install_uv
			,0 as try_uv
			,0 as pay_uv
			,0 as try_convert_uv
			,0 as real_revenue
			,month_1_revenue
			,month_3_revenue
			,month_6_revenue
			,month_9_revenue
			,month_12_revenue
			,month_13_revenue
			,month_15_revenue
			,0 as trial_nocanceled_6h_uv
		FROM scanner-master-android.scanner_ios_dw.scanner_ios_adjust_ads_keywords_all_forcast_data 
		WHERE install_date>='2024-08-19'
		)a 
		group by stats_date
		,package_name
		,channel
		,network
		,country_code
		,campaign_name
		,campaign_id
		,ad_group_name
		,ad_group_id
		,keywords
		,keyword_id;


												end;
