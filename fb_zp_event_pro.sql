


CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fb_zp_event_pro`(run_date DATE, history_day INT64, hitory_retain_day INT64)
begin




-------1.dwd_user_event_di
delete `gzdw2024.fb_zp_game.dwd_user_event_di`
where event_date>=date_add(run_date,interval -history_day day)
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
	FROM `gzdw2024.fbgame_01_basic.dwd_all_game_user_event_di`
	WHERE event_date>=date_add(run_date,interval -history_day day)
	and stream_id='9817620337';
	


-------2.dwd_user_active_di
delete `gzdw2024.fb_zp_game.dwd_user_active_di`
where event_date>=date_add(run_date,interval -history_day day);


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
	 FROM `gzdw2024.fb_zp_game.dwd_user_event_di` 
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
	FROM `gzdw2024.fb_zp_game.dwd_user_active_di`
	WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
	group by fbUserID,package_name
	)c 
	on a.fbUserID=c.fbUserID
	and a.package_name=c.package_name
	group by a.fbUserID,a.event_date,a.package_name,a.country,a.operating_system;

----------活跃、新增、广告新增、裂变新增


delete `gzdw2024.fb_zp_game.dws_user_active_report`
where event_date>=date_add(run_date,interval -hitory_retain_day day);

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
				FROM `gzdw2024.fb_zp_game.dwd_user_active_profile_di`
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
			FROM `gzdw2024.fb_zp_game.dwd_user_active_profile_di`
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
	 			FROM `gzdw2024.fb_zp_game.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
				group by event_date,fbUserID
				)a 
				group by fbUserID
			
			)c
			on a.fbUserID=c.fromUser
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
			group by a.event_date,country_code,platform;



----------广告统计表
delete `gzdw2024.fb_zp_game.dws_user_ad_report`
where event_date>=date_add(run_date,interval -history_day day);

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
					when lower(placement) like '%interstitial%' then 'Interstitial'
					else 'other' end as ad_type
				FROM `gzdw2024.fb_zp_game.dwd_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
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
				FROM `gzdw2024.fb_zp_game.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				
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
	FROM	`gzdw2024.fb_zp_game.dws_user_active_report`
	WHERE event_date>=date_add(run_date,interval -history_day day)

		)c1 
	on c0.event_date=c1.event_date
	and c0.platform=c1.platform
	and c0.country_code=c1.country_code;




	end;



