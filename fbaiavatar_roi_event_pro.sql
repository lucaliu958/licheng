CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fbaiavatar_roi_event_pro`(run_date DATE, history_day INT64, hitory_retain_day INT64)
begin


-------1.dwd_user_event_di
delete `fb-ai-avatar-puzzle.fb_dw.dwd_user_event_di`
where event_date>=date_add(run_date,interval -history_day day)
;
insert `fb-ai-avatar-puzzle.fb_dw.dwd_user_event_di`
	SELECT 
		event_name
		,PARSE_DATE('%Y%m%d',event_date) event_date
		,user_pseudo_id
		,event_timestamp
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='fbUserID') fbUserID 
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='isFirst') isFirst 
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='type') type 
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='from') fromon 
		,'fb.ai.avatar.puzzle' as package_name
		,geo.country
		,device.category as device_category
		,device.mobile_brand_name
		,device.mobile_model_name
		,device.mobile_marketing_name
		,device.operating_system_version
		,device.language
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='fromUser') fromUser 
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='abtestVersion') abtestVersion
		,device.operating_system as operating_system
	,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='placement') placement
		,(SELECT value.int_value FROM UNNEST(event_params) WHERE key='timeuse') as timeuse
	FROM `recorder-pro-50451.analytics_250268757.events_*`
	WHERE 1=1
	 and  stream_id='9692329810'
	and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','');
	


-------2.dwd_user_active_di
delete `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_di`
where event_date>=date_add(run_date,interval -history_day day);


insert `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_di`
SELECT 
	event_date
	,user_pseudo_id
	,fbUserID
	,package_name
	,MIN_BY(country, event_timestamp) AS country
	,max(fromUser) as fromUser
	,max(case when abtestVersion is null then 'A' else abtestVersion end  ) as abtestVersion
	,MIN_BY(operating_system, event_timestamp) AS operating_system
FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_event_di`
WHERE 1=1
and event_date>=date_add(run_date,interval -history_day day)
group by event_date
	,user_pseudo_id
	,fbUserID
	,package_name;


-------3.dwd_user_active_profile_di
delete `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_profile_di`
where event_date>=date_add(run_date,interval -hitory_retain_day day);

insert `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_profile_di`
SELECT
	a.fbUserID
	,a.event_date	
	,max(is_launch) as is_launch
	,max(case when a.event_date=event_date_min  then 1 else 0 end ) as is_new
	,max(is_ad) as is_ad
	,max(is_liebian) as is_liebian
	,a.package_name
	,a.country
	,a.operating_system
FROM
	(
	SELECT
		fbUserID
		,event_date
		,package_name
		,max(country) as country
		,max(operating_system) as operating_system
	FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_di`
	WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
	group by fbUserID,package_name,event_date
	)a 
	join
	(
	 SELECT 
	 	fbUserID
	 	,package_name
	 	,event_date
	 	,max(case when event_name  in ('fb_app_launch')  then 1 else 0 end) as is_launch
	 	,max(case when event_name  in ('fb_app_launch') and isFirst='true' then 1 else 0 end) as is_new
	 	,max(case when event_name  in ('fb_app_launch') and fromon='ad' then 1 else 0 end) as is_ad
	 	,max(case  when event_name  in ('fb_app_launch') and fromon in ('shareable_link','feed') then 1 else 0 end) as is_liebian
	 FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_event_di` 
	 where 1=1
	 and event_date>=date_add(run_date,interval -hitory_retain_day day)
	 group by fbUserID,event_date,package_name
	)b 
	on a.fbUserID=b.fbUserID
	and a.event_date=b.event_date
	left join 
	(
	SELECT
		fbUserID
		,min(event_date) as event_date_min
		,package_name
	FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_di`
	WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
	group by fbUserID,package_name
	)c 
	on a.fbUserID=c.fbUserID
	and a.package_name=c.package_name
	group by a.fbUserID,a.event_date,a.package_name,a.country,a.operating_system;

----------活跃、新增、广告新增、裂变新增


delete `fb-ai-avatar-puzzle.fb_dw.dws_user_active_report`
where event_date>=date_add(run_date,interval -hitory_retain_day day);

insert `fb-ai-avatar-puzzle.fb_dw.dws_user_active_report`

SELECT
	a.event_date	
	,count(distinct case when is_launch=1 then a.fbUserID else null end) as active_uv 
	,count(distinct case when is_launch=1 and is_new=1 then a.fbUserID else null end) as new_uv
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1 then a.fbUserID else null end) as new_ad_uv
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as new_liebian_uv 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=1 and is_new=1 then a.fbUserID else null end) as retain_uv2 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=2 and is_new=1 then a.fbUserID else null end) as retain_uv3 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=3 and is_new=1 then a.fbUserID else null end) as retain_uv4 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=4 and is_new=1 then a.fbUserID else null end) as retain_uv5 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=5 and is_new=1 then a.fbUserID else null end) as retain_uv6 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=6 and is_new=1 then a.fbUserID else null end) as retain_uv7 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=7 and is_new=1 then a.fbUserID else null end) as retain_uv8 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=14 and is_new=1 then a.fbUserID else null end) as retain_uv15 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=28 and is_new=1 then a.fbUserID else null end) as retain_uv29
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=1 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv2 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=2 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv3 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=3 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv4 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=4 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv5 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=5 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv6 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=6 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv7 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=7 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv8 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=14 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv15 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=28 and is_new=1  and is_ad=1 then a.fbUserID else null end) as retain_ad_uv29
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=1 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv2 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=2 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv3 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=3 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv4 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=4 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv5 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=5 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv6 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=6 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv7 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=7 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv8 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=14 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv15 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=28 and is_new=1 and is_ad=0 and  is_liebian=1  then a.fbUserID else null end) as retain_liebian_uv29
,case when country_code='SYRIA' then 'SY'
		when country_code='TÜRKIYE' then 'TR'
		when country_code='MYANMAR (BURMA)' then 'MM'
		when country_code='PALESTINE' then 'PS'
		when country_code='AUSTRALIA' then 'AU'
		when country_code='CONGO - KINSHASA' then 'CD'
		when country_code='BOSNIA & HERZEGOVINA' then 'BA'
					when country_code='NORTH MACEDONIA' then 'MK'
					when country_code='KOSOVO' then 'XK'
		else country_code end as country_code
		,platform
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1    then c.fbUserID else null end) as source_liebian_uv_ad 
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1 and date_diff(c.event_date_min,a.event_date,day)<=0  then c.fbUserID else null end) as source_liebian_uv_ad_0_day 
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1 and date_diff(c.event_date_min,a.event_date,day)<=1  then c.fbUserID else null end) as source_liebian_uv_ad_1_day
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1 and  date_diff(c.event_date_min,a.event_date,day)<=2  then c.fbUserID else null end) as source_liebian_uv_ad_2_day 
FROM
	(

			SELECT
				event_date
				,fbUserID
				,is_launch
				,is_new
				,is_ad
				,is_liebian
				,array[ifnull(country_code,country),'TOTAL'] as country_code
				,array[platform,'TOTAL'] as platform
			FROM
				(
				SELECT
					event_date
					,fbUserID
					,is_launch
					,is_new
					,is_ad
					,is_liebian
					,upper(country) as country
					,case when operating_system ='iOS' then 'iOS'
					when operating_system ='Android' then 'Android' 
					else 'web' end as platform
				FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
				)a 
				left join
			    (
			    SELECT 
			      upper(country_name_2) as country_name_2
			      ,country_name_3 as country_code
			    FROM `hzdw2024.hz_dim.dim_country`
			    )c 
			    on a.country=c.country_name_2
			  )a 
			left join
			(
			SELECT
				event_date
				,fbUserID
			FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_profile_di`
			WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
			group by event_date,fbUserID
			)b 
			on a.fbUserID=b.fbUserID
			left join
			(
			SELECT
				fbUserID
				
				,MIN_BY(fromUser, event_date) AS fromUser
				,min(event_date) as event_date_min
			FROM
				(
				SELECT
					event_date
					,fbUserID
					,max(fromUser) as fromUser
	 			FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
				group by event_date,fbUserID
				)a 
				group by fbUserID
			
			)c
			on a.fbUserID=c.fromUser
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
			group by a.event_date,country_code,platform;



--------------生成时间小时级数据

delete `fb-ai-avatar-puzzle.fb_dw.dws_user_export_time_report`
where ((event_date=date_add(run_date,interval -history_day  day) and event_hour>='16') 
	or event_date>date_add(run_date,interval -history_day  day)) ;


insert `fb-ai-avatar-puzzle.fb_dw.dws_user_export_time_report`

with a as (
SELECT
		a.event_date	
		,case when country_code='SYRIA' then 'SY'
			when country_code='TÜRKIYE' then 'TR'
			when country_code='MYANMAR (BURMA)' then 'MM'
			when country_code='PALESTINE' then 'PS'
			when country_code='AUSTRALIA' then 'AU'
			when country_code='CONGO - KINSHASA' then 'CD'
			when country_code='BOSNIA & HERZEGOVINA' then 'BA'
						when country_code='NORTH MACEDONIA' then 'MK'
						when country_code='KOSOVO' then 'XK'
			else country_code end as country_code
			,platform
			,timeuse
			,package_name
			,event_name
	,event_day_hour
	,event_hour
	,user_pseudo_id
	FROM
		(

			SELECT
				event_date
				,user_pseudo_id
				,array[ifnull(country_code,country),'TOTAL'] as country_code
				,array[platform,'TOTAL'] as platform
				,event_day_hour
				,event_hour
				,timeuse
				,event_name
				,package_name
			FROM
				(
				SELECT
					date(format_timestamp("%Y-%m-%d %H:%M:%S", timestamp_seconds( cast ((event_timestamp/1000000) as int64)),'Asia/Shanghai')) AS event_date
					,user_pseudo_id
					,upper(country) as country
					,case when operating_system ='iOS' then 'iOS'
					when operating_system ='Android' then 'Android' 
					else 'web' end as platform						
					,safe_divide(timeuse,1000) as timeuse
					,package_name
					,event_name
					,format_timestamp("%Y-%m-%d %H:00:00", timestamp_seconds( cast ((event_timestamp/1000000) as int64)),'Asia/Shanghai') as event_day_hour
					,substr(cast(format_timestamp("%Y-%m-%d %H:%M:%S", timestamp_seconds( cast ((event_timestamp/1000000) as int64)),'Asia/Shanghai')as string),12,2) as event_hour
				FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day  day)
				and event_name in ('fb_templ_res_export','fb_templ_res_click','fb_temp_export_fail')
				)a 
				left join
			    (
			    SELECT 
			      upper(country_name_2) as country_name_2
			      ,country_name_3 as country_code
			    FROM `hzdw2024.hz_dim.dim_country`
			    )c 
			    on a.country=c.country_name_2
			    --where timeuse<10000
			  )a 
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
		),
 b as (
SELECT
	event_date
	,package_name
	,platform
	,country_code
	,event_day_hour
	,event_hour
	,max(percentile_50) as percentile_50
	,max(percentile_75) as percentile_75
	,max(percentile_90) as percentile_90
	,max(percentile_95) as percentile_95
	,max(percentile_975) as percentile_975
	,avg(timeuse) as avg_timeuse
	,count(1) as export_pv 

FROM
	(
SELECT
	event_date
	,package_name
	,platform
	,country_code
	,timeuse
	,event_day_hour
	,event_hour
	,PERCENTILE_CONT(timeuse,0.25) over (partition by event_date,country_code,platform,event_day_hour) AS percentile_25
	,PERCENTILE_CONT(timeuse,0.50) over (partition by event_date,country_code,platform,event_day_hour) AS percentile_50
	,PERCENTILE_CONT(timeuse,0.75) over (partition by event_date,country_code,platform,event_day_hour) AS percentile_75
	,PERCENTILE_CONT(timeuse,0.90) over (partition by event_date,country_code,platform,event_day_hour) AS percentile_90
	,PERCENTILE_CONT(timeuse,0.95) over (partition by event_date,country_code,platform,event_day_hour) AS percentile_95
	,PERCENTILE_CONT(timeuse,0.975) over (partition by event_date,country_code,platform,event_day_hour) AS percentile_975
FROM
	(
		select * 
		from a  
		where 1=1
		and event_name='fb_templ_res_export'
		and timeuse<3600
		)d 
	)e 
			group by event_date,country_code,platform,package_name,event_date
	,event_day_hour
	,event_hour
	),
 c as 
(
	SELECT
	event_date
	,package_name
	,platform
	,country_code
	,event_day_hour
	,event_hour
	,count(case when event_name='fb_templ_res_click' then user_pseudo_id else null end) as click_pv
,count(case when event_name='fb_temp_export_fail' then user_pseudo_id else null end) as fail_pv

FROM
	(

		select * 
		from a  
		where 1=1
		and event_name in ('fb_templ_res_click','fb_temp_export_fail')
		
	)e 
			group by event_date,country_code,platform,package_name,event_date
	,event_day_hour
	,event_hour
)
SELECT
	event_date
	,package_name
	,platform
	,country_code
	,event_day_hour
	,event_hour
	,max(click_pv) as click_pv
	,max(fail_pv) as fail_pv
	,max(export_pv ) as export_pv
	,max(percentile_50) as percentile_50
	,max(percentile_75) as percentile_75
	,max(percentile_90) as percentile_90
	,max(percentile_95) as percentile_95
	,max(percentile_975) as percentile_975
	,max(avg_timeuse) as avg_timeuse
	
FROM
	(
	SELECT
		event_date
		,package_name
		,platform
		,country_code
		,event_day_hour
		,event_hour
		,click_pv
		,fail_pv
		,0  as percentile_50
		,0  as percentile_75
		,0  as percentile_90
		,0  as percentile_95
		,0  as percentile_975
		,0 as avg_timeuse
		,0 as export_pv 
	FROM c 
	union all 
	SELECT
		event_date
		,package_name
		,platform
		,country_code
		,event_day_hour
		,event_hour
		,0 as click_pv
		,0 as export_pv
		,percentile_50
		,percentile_75
		,percentile_90
		,percentile_95
		,percentile_975
		,avg_timeuse
		,export_pv 
	FROM b 
	)d 
	group by event_date
	,package_name
	,platform
	,country_code
	,event_day_hour
	,event_hour;


----------广告统计表
delete `fb-ai-avatar-puzzle.fb_dw.dws_user_ad_report`
where event_date>=date_add(run_date,interval -history_day  day);

insert `fb-ai-avatar-puzzle.fb_dw.dws_user_ad_report`
with a as (
SELECT
		a.event_date	
		,case when country_code='SYRIA' then 'SY'
			when country_code='TÜRKIYE' then 'TR'
			when country_code='MYANMAR (BURMA)' then 'MM'
			when country_code='PALESTINE' then 'PS'
			when country_code='AUSTRALIA' then 'AU'
			when country_code='CONGO - KINSHASA' then 'CD'
			when country_code='BOSNIA & HERZEGOVINA' then 'BA'
						when country_code='NORTH MACEDONIA' then 'MK'
						when country_code='KOSOVO' then 'XK'
			else country_code end as country_code
			,platform
			,placement
			,package_name
			,event_name	
	,user_pseudo_id
  ,ad_type
	FROM
		(

			SELECT
				a.event_date
				,ifnull(fbUserID,a.user_pseudo_id) as user_pseudo_id
				,array[ifnull(country_code,country),'TOTAL'] as country_code
				,array[platform,'TOTAL'] as platform

				,array[placement,'TOTAL'] as placement
				,array[ad_type,'TOTAL'] as ad_type
				,event_name
				,package_name
			FROM
				(
				SELECT
					 event_date
					,user_pseudo_id
					,upper(country) as country
					,case when operating_system ='iOS' then 'iOS'
					when operating_system ='Android' then 'Android' 
					else 'web' end as platform						
					,safe_divide(timeuse,1000) as timeuse
					,package_name
					,event_name
					,placement
					,case when lower(placement) like '%banner%' then 'banner'
					when lower(placement) like '%interstitial%' then 'Interstitial'
					else 'other' end as ad_type
				FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day  day)
				and event_name in ('ad_load_c','ad_load_fail_c','ad_load_success_c','ad_impression_c')
				)a 
				left join
			    (
			    SELECT 
			      upper(country_name_2) as country_name_2
			      ,country_name_3 as country_code
			    FROM `hzdw2024.hz_dim.dim_country`
			    )c 
			    on a.country=c.country_name_2
			    left	join 
				(
				SELECT
					event_date
					,user_pseudo_id
					,max(fbUserID) as fbUserID
				FROM `fb-ai-avatar-puzzle.fb_dw.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_day  day)
				
				group by event_date,user_pseudo_id
			   )b 
				on a.user_pseudo_id=b.user_pseudo_id
				and a.event_date=b.event_date
			  )a 
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
			,UNNEST(placement) as placement
      ,UNNEST(ad_type) as ad_type
		),
 c as 
(
	SELECT
	event_date
	,package_name
	,platform
	,country_code
	,placement
	,ad_type
	,count(case when event_name='ad_load_c' then user_pseudo_id else null end) as load_pv
	,count(case when event_name='ad_load_success_c' then user_pseudo_id else null end) as load_succ_pv
	,count(case when event_name='ad_load_fail_c' then user_pseudo_id else null end) as load_fail_pv
	,count(case when event_name='ad_impression_c' then user_pseudo_id else null end) as impression_pv
	,count(distinct case when event_name='ad_load_c' then user_pseudo_id else null end) as load_uv
	,count(distinct case when event_name='ad_load_success_c' then user_pseudo_id else null end) as load_succ_uv
	,count(distinct case when event_name='ad_load_fail_c' then user_pseudo_id else null end) as load_fail_uv
	,count(distinct case when event_name='ad_impression_c' then user_pseudo_id else null end) as impression_uv

FROM
	(

		select * 
		from a  
		where 1=1
		--and event_name in ('fb_templ_res_click','fb_temp_export_fail')
		
	)e 
			group by event_date,country_code,platform,package_name,placement,ad_type
)
SELECT
	c0.event_date
	,c0.package_name
	,c0.platform
	,c0.country_code
	,placement
	,load_pv
	,load_succ_pv
	,load_fail_pv
	,impression_pv
	,load_uv
	,load_succ_uv 
	,load_fail_uv
	,impression_uv
	,active_uv
	,ad_type
FROM
	(
	SELECT
		*
	FROM c 
	)c0 
	left join
	(
	SELECT
		event_date
		,platform
		,country_code
		,active_uv
	FROM	`fb-ai-avatar-puzzle.fb_dw.dws_user_active_report`
	WHERE event_date>=date_add(run_date,interval -history_day  day)

		)c1 
	on c0.event_date=c1.event_date
	and c0.platform=c1.platform
	and c0.country_code=c1.country_code;


--------6.fb后台广告统计表

delete `fb-ai-avatar-puzzle.fb_dw.dws_user_fb_ad_report`
where event_date>=date_add(run_date,interval -history_day  day);

insert `fb-ai-avatar-puzzle.fb_dw.dws_user_fb_ad_report`
	--create table  `fb-ai-avatar-puzzle.fb_dw.dws_user_fb_ad_report`
		--PARTITION BY stats_date as 
		SELECT
			stats_date
			,package_name
			,c0.platform
			,c0.country_code
			,ad_type
			,requests
			,filled_requests
			,impressions
			,revenue
			,clicks
			,active_uv
		FROM
			(
			SELECT
				stats_date
				,package_name
				,platform
				,country_code
				,ad_type
				,sum(requests)  as requests
				,sum(filled_requests)  as filled_requests
				,sum(impressions)  as impressions
				,sum(revenue)  as revenue
				,sum(clicks)  as clicks
			FROM
				(
				SELECT 
					date(start_timestamp) as stats_date
						,'fb.ai.avatar.puzzle' as package_name
					,array['TOTAL',case when platform='ios' then 'iOS' when platform='android' then 'Android' else platform end]  as platform
					,array['TOTAL',upper(country)] as country_code
					,array['TOTAL',case when lower(placement_name) like '%banner%' then 'banner'
					else 'Interstitial' end ] as ad_type
					,requests
					,filled_requests
					,impressions
					,revenue
					,clicks
				FROM `fb-ai-avatar-puzzle.analytics_43927691.ad_analytics_detail_day_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -2 day) as string),'-','')
				and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
					--and _TABLE_SUFFIX!='20241103'
				)c 
				,UNNEST(platform) as platform
				,UNNEST(country_code) as country_code
				,UNNEST(ad_type) as ad_type
				group by stats_date,platform,country_code,ad_type,package_name
			)c0
			left join
			(
			SELECT
				event_date
				,platform
				,country_code
				,active_uv
			FROM	`fb-ai-avatar-puzzle.fb_dw.dws_user_active_report`
			WHERE event_date>=date_add(run_date,interval -history_day  day)

			)c1 
			on c0.stats_date=c1.event_date
			and c0.platform=c1.platform
			and c0.country_code=c1.country_code;





	end;
