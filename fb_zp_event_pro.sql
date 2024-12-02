


CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fb_zp_event_pro`(run_date DATE, history_day INT64, hitory_retain_day INT64,history_end_day  INT64)
begin




-------1.dwd_user_event_di
delete `gzdw2024.fb_zp_game.dwd_user_event_di`
where event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day)
;
insert `gzdw2024.fb_zp_game.dwd_user_event_di`
	SELECT
		event_name
		,event_date
		,user_pseudo_id
		,event_timestamp
		,fbUserID
		,isFirst
		,type
		,fromon
		,package_name
		,country
		,device_category
		,mobile_brand_name
		,mobile_model_name
		,mobile_marketing_name
		,operating_system_version
		,language
		,fromUser
		,abtestVersion
		,operating_system
		,placement
		,timeuse
		,steps
		,error_code
		,win
		,hidesum
		,level_id
		,gameID
		,gameShowTime
	FROM `gzdw2024.fbgame_01_basic.dwd_all_game_user_event_di`
	WHERE event_date>=date_add(run_date,interval -history_day day)
	and event_date<=date_add(run_date,interval -history_end_day day)
	and stream_id='9817620337';
	


-------2.dwd_user_active_di
delete `gzdw2024.fb_zp_game.dwd_user_active_di`
where event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day);


insert `gzdw2024.fb_zp_game.dwd_user_active_di`
SELECT 
	event_date
	,user_pseudo_id
	,fbUserID
	,package_name
	,MIN_BY(country, event_timestamp) AS country
	,max(fromUser) as fromUser
	,max(case when abtestVersion is null then 'A' else abtestVersion end  ) as abtestVersion
	,MIN_BY(operating_system, event_timestamp) AS operating_system
FROM `gzdw2024.fb_zp_game.dwd_user_event_di`
WHERE 1=1
and event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day)
group by event_date
	,user_pseudo_id
	,fbUserID
	,package_name;


-------3.dwd_user_active_profile_di
delete `gzdw2024.fb_zp_game.dwd_user_active_profile_di`
where event_date>=date_add(run_date,interval -hitory_retain_day day);

insert `gzdw2024.fb_zp_game.dwd_user_active_profile_di`
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
	FROM `gzdw2024.fb_zp_game.dwd_user_active_di`
	WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
	and event_date<=date_add(run_date,interval -history_end_day day)
	group by fbUserID,package_name,event_date
	)a 
	join
	(
	 SELECT 
	 	fbUserID
	 	,package_name
	 	,event_date
	 	,max(case when event_name  in ('fb_zp_app_launch')  then 1 else 0 end) as is_launch
	 	,max(case when event_name  in ('fb_zp_app_launch') and isFirst='true' then 1 else 0 end) as is_new
	 	,max(case when event_name  in ('fb_zp_app_launch') and fromon='ad' then 1 else 0 end) as is_ad
	 	,max(case  when event_name  in ('fb_zp_app_launch') and fromon in ('shareable_link','feed') then 1 else 0 end) as is_liebian
	 FROM `gzdw2024.fb_zp_game.dwd_user_event_di` 
	 where 1=1
	 and event_date>=date_add(run_date,interval -hitory_retain_day day)
	 and event_date<=date_add(run_date,interval -history_end_day day)
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
	FROM `gzdw2024.fb_zp_game.dwd_user_active_di`
	WHERE event_date>=date_add(run_date,interval -(hitory_retain_day+150) day)
	and event_date<=date_add(run_date,interval -history_end_day day)
	group by fbUserID,package_name
	)c 
	on a.fbUserID=c.fbUserID
	and a.package_name=c.package_name
	group by a.fbUserID,a.event_date,a.package_name,a.country,a.operating_system;

----------活跃、新增、广告新增、裂变新增


delete `gzdw2024.fb_zp_game.dws_user_active_report`
where event_date>=date_add(run_date,interval -hitory_retain_day day)
and event_date<=date_add(run_date,interval -history_end_day day);

insert `gzdw2024.fb_zp_game.dws_user_active_report`
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
	,a.package_name
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
				,a.package_name
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
					,package_name
				FROM `gzdw2024.fb_zp_game.dwd_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
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
				,package_name
			FROM `gzdw2024.fb_zp_game.dwd_user_active_profile_di`
			WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
			and event_date<=date_add(run_date,interval -history_end_day day)
			group by event_date,fbUserID,package_name
			)b 
			on a.fbUserID=b.fbUserID
			and a.package_name=b.package_name
			left join
			(
			SELECT
				fbUserID
				,package_name
				,MIN_BY(fromUser, event_date) AS fromUser
				,min(event_date) as event_date_min
			FROM
				(
				SELECT
					event_date
					,fbUserID
					,max(fromUser) as fromUser
					,package_name
	 			FROM `gzdw2024.fb_zp_game.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				group by event_date,fbUserID,package_name
				)a 
				group by fbUserID,package_name
			
			)c
			on a.fbUserID=c.fromUser
			and a.package_name=c.package_name
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
			group by a.event_date,country_code,platform,package_name;



----------广告统计表
delete `gzdw2024.fb_zp_game.dws_user_ad_report`
where event_date>=date_add(run_date,interval -history_day day)
	and event_date<=date_add(run_date,interval -history_end_day  day);

insert `gzdw2024.fb_zp_game.dws_user_ad_report`
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
					when lower(placement) like '%reward%interstitial%' then 'rewarded_interstitial'
					when lower(placement) like '%interstitial%' then 'interstitial'
						when lower(placement) like '%hint%' then 'interstitial'
					else 'other' end as ad_type
				FROM `gzdw2024.fb_zp_game.dwd_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				and event_name in ('fb_zp_ad_load_c','fb_zp_ad_load_fail_c','fb_zp_ad_load_success_c','fb_zp_ad_impression_c')
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
				FROM `gzdw2024.fb_zp_game.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)				
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
	,count(case when event_name='fb_zp_ad_load_c' then user_pseudo_id else null end) as load_pv
	,count(case when event_name='fb_zp_ad_load_success_c' then user_pseudo_id else null end) as load_succ_pv
	,count(case when event_name='fb_zp_ad_load_fail_c' then user_pseudo_id else null end) as load_fail_pv
	,count(case when event_name='fb_zp_ad_impression_c' then user_pseudo_id else null end) as impression_pv
	,count(distinct case when event_name='fb_zp_ad_load_c' then user_pseudo_id else null end) as load_uv
	,count(distinct case when event_name='fb_zp_ad_load_success_c' then user_pseudo_id else null end) as load_succ_uv
	,count(distinct case when event_name='fb_zp_ad_load_fail_c' then user_pseudo_id else null end) as load_fail_uv
	,count(distinct case when event_name='fb_zp_ad_impression_c' then user_pseudo_id else null end) as impression_uv

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
	FROM	`gzdw2024.fb_zp_game.dws_user_active_report`
	WHERE event_date>=date_add(run_date,interval -history_day day)
	and event_date<=date_add(run_date,interval -history_end_day day)
	)c1 
	on c0.event_date=c1.event_date
	and c0.platform=c1.platform
	and c0.country_code=c1.country_code;



	----------广告请求失败统计表
delete `gzdw2024.fb_zp_game.dws_user_ad_fail_report`
where event_date>=date_add(run_date,interval -history_day  day)
and event_date<=date_add(run_date,interval -history_end_day  day);

insert `gzdw2024.fb_zp_game.dws_user_ad_fail_report`
	--create table  `gzdw2024.fb_zp_game.dws_user_ad_fail_report`
	--	PARTITION BY event_date as 
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
			,error_code
			,ad_type
			,package_name
			,event_name	
			,user_pseudo_id
	FROM
		(

			SELECT
				a.event_date
				,ifnull(fbUserID,a.user_pseudo_id) as user_pseudo_id
				,array[ifnull(country_code,country),'TOTAL'] as country_code
				,array[platform,'TOTAL'] as platform
				,array[ad_type,'TOTAL'] as ad_type
				,event_name
				,package_name
				,error_code
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
					,error_code
					,case when lower(placement) like '%banner%' then 'banner'
					when lower(placement) like '%reward%interstitial%' then 'rewarded_interstitial'
					when lower(placement) like '%interstitial%' then 'interstitial'
						when lower(placement) like '%hint%' then 'interstitial'
					else 'other' end as ad_type
				FROM `gzdw2024.fb_zp_game.dwd_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day  day)
				and event_date<=date_add(run_date,interval -history_end_day  day)
				and event_name in ('fb_zp_ad_load_fail_c')
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
				FROM `gzdw2024.fb_zp_game.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_day  day)
				and event_date<=date_add(run_date,interval -history_end_day  day)				
				group by event_date,user_pseudo_id
			   )b 
				on a.user_pseudo_id=b.user_pseudo_id
				and a.event_date=b.event_date
			  )a 
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
     ,UNNEST(ad_type) as ad_type
	),
 c as 
(
SELECT
	event_date
	,package_name
	,platform
	,country_code
	,error_code
	,ad_type
	,count(case when event_name='fb_zp_ad_load_fail_c' then user_pseudo_id else null end) as load_fail_pv
FROM
	(

		select * 
		from a  
		where 1=1
		--and event_name in ('fb_templ_res_click','fb_temp_export_fail')
		
	)e 
			group by event_date,country_code,platform,package_name,error_code,ad_type
)
SELECT
	c0.event_date
	,c0.package_name
	,c0.platform
	,c0.country_code
	,error_code
	,ad_type
	,load_fail_pv
	,active_uv
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
	FROM	`gzdw2024.fb_zp_game.dws_user_active_report`
	WHERE event_date>=date_add(run_date,interval -history_day  day)
	and event_date<=date_add(run_date,interval -history_end_day  day)

		)c1 
	on c0.event_date=c1.event_date
	and c0.platform=c1.platform
	and c0.country_code=c1.country_code;




--------6.fb后台广告统计表

delete `gzdw2024.fb_zp_game.dws_user_fb_ad_report`
where stats_date>=date_add(run_date,interval -history_day  day)
and stats_date<=date_add(run_date,interval -history_end_day day);

insert `gzdw2024.fb_zp_game.dws_user_fb_ad_report`
	--create table  `gzdw2024.fb_zp_game.dws_user_fb_ad_report`
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
						,'fb.zp' as package_name
					,array['TOTAL',case when platform='ios' then 'iOS' when platform='android' then 'Android' else platform end]  as platform
					,array['TOTAL',upper(country)] as country_code
					,array['TOTAL',case when lower(placement_name) like '%banner%' then 'banner'
					when lower(placement_name) like '%rewarded%interstitial%' then 'rewarded_interstitial'
					when lower(placement_name) like '%interstitial%' then 'interstitial'
					else 'other' end ] as ad_type
					,requests
					,filled_requests
					,impressions
					,revenue
					,clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_detail_day_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
				and date(start_timestamp)=parse_date('%Y%m%d',_table_suffix)
				and app_name='Solitaire'
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
			FROM	`gzdw2024.fb_zp_game.dws_user_active_report`
			WHERE event_date>=date_add(run_date,interval -history_day  day)
			and event_date<=date_add(run_date,interval -history_end_day day)

			)c1 
			on c0.stats_date=c1.event_date
			and c0.platform=c1.platform
			and c0.country_code=c1.country_code;

------8.各事件活跃次数与人数

delete `gzdw2024.fb_zp_game.dws_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -history_end_day day);


insert `gzdw2024.fb_zp_game.dws_event_active_report`
--create table  `gzdw2024.fb_zp_game.dws_event_active_report`
	--PARTITION BY stats_date as 
	SELECT
		c0.event_date  AS stats_date
		,c0.package_name
		,c0.platform
		,c0.country_code
		,event_name
		,pv 
		,uv 
		,active_uv
	FROM
		(
		SELECT
			event_date
			,package_name
			,platform
			,country_code
			,event_name
			,count(1) as pv 
			,count(distinct fbUserID) as uv 
		FROM
			(
			SELECT
				a.event_date
				,package_name
				,ifnull(fbUserID,a.user_pseudo_id) as fbUserID
				,event_name
				,platform
				,country_code
			FROM
				(
				SELECT
					 event_date
					,user_pseudo_id
					,ARRAY['TOTAL',ifnull(country_name_3,a.country)] as country_code
					,ARRAY['TOTAL',case when operating_system ='iOS' then 'iOS'
					when operating_system ='Android' then 'Android' 
					else 'web' end] as platform						
					,package_name
					,event_name
				FROM `gzdw2024.fb_zp_game.dwd_user_event_di`   a 
				left join `hzdw2024.hz_dim.dim_country` b
				on upper(a.country)=upper(b.country_name_2)
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				and event_name in (
					'fb_zp_app_launch'
						,'fb_zp_first_open'
						,'fb_zp_home_show'
						,'fb_zp_home_func_sound'
						,'fb_zp_home_func_language'
						,'fb_zp_game_play_show'
						,'fb_zp_game_play_finish'
						,'fb_zp_game_play_exit'
						,'fb_zp_new_game_play'
						,'fb_zp_share_show'
						,'fb_zp_share_click'
						,'fb_zp_share_share'
						,'fb_zp_ad_load_c'
						,'fb_zp_ad_load_success_c'
						,'fb_zp_ad_load_fail_c'
						,'fb_zp_ad_impression_c'
						,'fb_zp_ad_impression_c_100'
						,'fb_zp_ad_click_c'
						,'fb_zp_ad_back_c'
						,'fb_zp_ad_close_c'
						,'fb_zp_ad_about_to_show'
						,'fb_zp_banner_show'
						,'fb_zp_banner_click'
						,'fb_zp_templ_invite'
						,'fb_zp_templ_invite_c'
						,'fb_zp_templ_invite_e'
						,'fb_zp_mess_authorize'
						,'fb_zp_mess_authorize_c'
						,'fb_zp_mess_authorize_l'
						,'fb_zp_favorite'
						,'fb_zp_favorite_c'
						,'fb_zp_favorite_l'
						,'fb_zp_openAdWatchTask'
						,'fb_zp_openAdWatchTask_c'
						,'fb_zp_openAdWatchTask_watch_ad_c'
						,'fb_zp_reward_ad_fail'
						,'fb_zp_reward_ad_not_complete'
						,'fb_zp_openAdWatchTask_watch_ad_s'
						,'fb_zp_start_move_click_card'
						)
				)a 
				 left	join 
				(
				SELECT
					event_date
					,user_pseudo_id
					,max(fbUserID) as fbUserID
				FROM `gzdw2024.fb_zp_game.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				group by event_date,user_pseudo_id
			   )b 
				on a.user_pseudo_id=b.user_pseudo_id
				and a.event_date=b.event_date
				,UNNEST(platform) AS platform
				,UNNEST(country_code) AS country_code
				)c 
			group by event_date
			,package_name
			,platform
			,country_code
			,event_name
			)c0 
			left join
			(
			SELECT
				event_date
				,platform
				,country_code
				,active_uv
			FROM	`gzdw2024.fb_zp_game.dws_user_active_report`
			WHERE event_date>=date_add(run_date,interval -history_day  day)
			and event_date<=date_add(run_date,interval -history_end_day day)

			)c1 
			on c0.event_date=c1.event_date
			and c0.platform=c1.platform
			and c0.country_code=c1.country_code;


----------------事件参数明细

delete `gzdw2024.fb_zp_game.dws_fb_events_detail`
where event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day)
;
--drop table `gzdw2024.fb_zp_game.dws_fb_events_detail`;
	--	create table  `gzdw2024.fb_zp_game.dws_fb_events_detail`
	--	PARTITION BY event_date as 
	insert `gzdw2024.fb_zp_game.dws_fb_events_detail`
	SELECT
		C0.event_date
		,C0.package_name
		,C0.country_code
		,C0.platform
		,event_name
		,event_params_key
		,event_params_value
		,event_num
		,user_num
		,active_uv
	FROM
		(
		SELECT
			event_date
			,package_name
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
			,event_name
			,event_params_key
			,event_params_value
			,count(1) as event_num 
			,count(distinct user_pseudo_id) as user_num
		FROM
			(
			SELECT
				a.event_date
				,package_name
				,array[ifnull(country_name_3,a.country),'TOTAL'] as country_code
				,array[platform,'TOTAL'] as platform
				,event_name
				,event_params_key
				,event_params_value
				,ifnull(fbUserID,a.user_pseudo_id) as user_pseudo_id
			FROM
				(
				SELECT
				  PARSE_DATE('%Y%m%d', event_date) AS event_date,
				  geo.country AS country,
				  case when stream_id ='9692329810' then 'fb.ai.avatar.puzzle' 
		       when stream_id ='9817620337' then 'fb.zp' end  as package_name,
				  event_name
				  ,case when device.operating_system ='iOS' then 'iOS'
							when device.operating_system ='Android' then 'Android' 
							else 'web' end as platform,
				  case when event_params.key='from' then 'fromon' else event_params.key end AS event_params_key,
			  	  COALESCE(CAST(event_params.value.string_value AS string),CAST(event_params.value.int_value AS string),CAST(event_params.value.float_value AS string),CAST(event_params.value.double_value AS string)) AS event_params_value,
				  user_pseudo_id
				FROM
				  `recorder-pro-50451.analytics_250268757.events_*`, 
				  UNNEST (event_params)event_params
				WHERE
				  _TABLE_SUFFIX >= REPLACE(CAST(DATE_ADD(run_date,INTERVAL - history_day day)AS string),'-','')
				  and _TABLE_SUFFIX <= REPLACE(CAST(DATE_ADD(run_date,INTERVAL - history_end_day day)AS string),'-','')
				  and  stream_id='9817620337'
				  and event_name in (
									'fb_zp_app_launch'
										,'fb_zp_first_open'
										,'fb_zp_home_show'
										,'fb_zp_home_func_sound'
										,'fb_zp_home_func_language'
										,'fb_zp_game_play_show'
										,'fb_zp_game_play_finish'
										,'fb_zp_game_play_exit'
										,'fb_zp_new_game_play'
										,'fb_zp_share_show'
										,'fb_zp_share_click'
										,'fb_zp_share_share'
										,'fb_zp_ad_load_c'
										,'fb_zp_ad_load_success_c'
										,'fb_zp_ad_load_fail_c'
										,'fb_zp_ad_impression_c'
										,'fb_zp_ad_impression_c_100'
										,'fb_zp_ad_click_c'
										,'fb_zp_ad_back_c'
										,'fb_zp_ad_close_c'
										,'fb_zp_ad_about_to_show'
										,'fb_zp_banner_show'
										,'fb_zp_banner_click'
										,'fb_zp_templ_invite'
										,'fb_zp_templ_invite_c'
										,'fb_zp_templ_invite_e'
										,'fb_zp_mess_authorize'
										,'fb_zp_mess_authorize_c'
										,'fb_zp_mess_authorize_l'
										,'fb_zp_favorite'
										,'fb_zp_favorite_c'
										,'fb_zp_favorite_l'
										,'fb_zp_openAdWatchTask'
										,'fb_zp_openAdWatchTask_c'
										,'fb_zp_openAdWatchTask_watch_ad_c'
										,'fb_zp_reward_ad_fail'
										,'fb_zp_reward_ad_not_complete'
										,'fb_zp_openAdWatchTask_watch_ad_s'
										,'fb_zp_start_move_click_card'
										)
									 AND event_params.key IN ("error",
											    "code",
											    "ga_session_id",
											    "type",
											    "entrance",
											    "id",
											    "placement",
											    "from",
											    "platform",
											    "msg",
											    "timeuse",
											    "timeout",
											    "steps",
												"win",
												"hidesum",
												"level_id")
						)a 
						left join `hzdw2024.hz_dim.dim_country` b
						on upper(a.country)=upper(b.country_name_2)
					    left join 
						(
						SELECT
							event_date
							,user_pseudo_id
							,max(fbUserID) as fbUserID
						FROM `gzdw2024.fb_zp_game.dwd_user_active_di` 
						WHERE event_date>=date_add(run_date,interval -history_day day)
						and event_date<=date_add(run_date,interval -history_end_day day)
						group by event_date,user_pseudo_id
					   )c 
						on a.user_pseudo_id=c.user_pseudo_id
						and a.event_date=c.event_date
					)C 
					,UNNEST(country_code) AS country_code
					,UNNEST(platform) as platform
					group by event_date
			,package_name
			,case when country_code='SYRIA' then 'SY'
				when country_code='TÜRKIYE' then 'TR'
				when country_code='MYANMAR (BURMA)' then 'MM'
				when country_code='PALESTINE' then 'PS'
				when country_code='AUSTRALIA' then 'AU'
				when country_code='CONGO - KINSHASA' then 'CD'
				when country_code='BOSNIA & HERZEGOVINA' then 'BA'
							when country_code='NORTH MACEDONIA' then 'MK'
							when country_code='KOSOVO' then 'XK'
				else country_code end 
				,platform
			,event_name
			,event_params_key
			,event_params_value
			)c0 
			left join
			(
			SELECT
				event_date
				,package_name
				,platform
				,country_code
				,active_uv
			FROM	`gzdw2024.fb_zp_game.dws_user_active_report`
			WHERE event_date>=date_add(run_date,interval -history_day  day)
			and event_date<=date_add(run_date,interval -history_end_day day)

			)c1 
			on c0.event_date=c1.event_date
			and c0.platform=c1.platform
			and c0.country_code=c1.country_code
			AND c0.package_name=c1.package_name
			;


							
----插屏与激励期望展示率
delete `gzdw2024.fb_zp_game.dws_ad_expect_show_report`
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -history_end_day day);


insert `gzdw2024.fb_zp_game.dws_ad_expect_show_report`
--create table  `gzdw2024.fb_zp_game.dws_ad_expect_show_report`
--	PARTITION BY stats_date as 
SELECT
	a.stats_date
	,a.package_name
	,a.platform
	,a.country_code
	,pv 
	,impressions
FROM
	(
	SELECT
		stats_date
		,package_name
		,platform
		,country_code
		,sum(pv ) as pv 
	FROM `gzdw2024.fb_zp_game.dws_event_active_report`
	WHERE event_name IN ('fb_zp_game_play_finish','fb_zp_new_game_play')
	AND stats_date>=date_add(run_date,interval -history_day day)
	and stats_date<=date_add(run_date,interval -history_end_day day)
	group by stats_date,package_name,platform,Country_code
	)a 
	left join
	(
		SELECT
		stats_date
		,package_name
		,platform
		,country_code
		,sum(impressions ) as impressions 
	FROM `gzdw2024.fb_zp_game.dws_user_fb_ad_report`
	WHERE ad_type like '%interstitial%'
	AND stats_date>=date_add(run_date,interval -history_day day)
	and stats_date<=date_add(run_date,interval -history_end_day day)
	group by stats_date,package_name,platform,Country_code
	)b 
	on a.stats_date=b.stats_date
	and a.package_name=b.package_name
	and a.platform=b.platform
	and a.country_code=b.country_code;


	end;
