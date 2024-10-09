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
		,'fb.ai.avatar' as package_name
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
	,max(case when a.event_date=event_date_min and  is_new=1 then 1 else 0 end ) as is_new
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






	end;
