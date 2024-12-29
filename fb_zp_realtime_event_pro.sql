CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fb_zp_realtime_event_pro`(run_date DATE, history_day INT64, hitory_retain_day INT64, history_end_day INT64)
begin




-------1.dwd_all_game_user_event_di
delete `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di`
where event_day>=date_add(run_date,interval -(history_day+1) day)
and event_day<=date_add(run_date,interval -history_end_day day);



insert `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di`
	--drop table if exists  `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di`;
	--create table  `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di`
	--PARTITION BY event_date as 
	SELECT 
		event_name
		,date(TIMESTAMP_TRUNC(event_timestamp_micros, DAY)) as event_day
		,date(format_timestamp("%Y-%m-%d", timestamp_seconds( cast ((event_timestamp_millis/1000) as int64)),'America/Los_Angeles')) as event_date
	    ,event_timestamp_micros
		,format_timestamp("%Y-%m-%d %H:%M:%S", timestamp_seconds( cast ((event_timestamp_millis/1000) as int64)),'America/Los_Angeles') as event_time
		,substr(cast(format_timestamp("%Y-%m-%d %H:%M:%S", timestamp_seconds( cast ((event_timestamp_millis/1000) as int64)),'America/Los_Angeles') as string),12,2) as event_hour
		,player_id as user_id
		,event_timestamp_millis
		,case when app_id ='3755114334752318' then 'fb.zp' 
         else app_id end  as package_name
		,upper(country) as country_code
		,platform
		,os_version
		,user_default_language
		,(SELECT value FROM UNNEST(event_params) WHERE name='fbUserID') fbUserID 
		,(SELECT value FROM UNNEST(event_params) WHERE name='isFirst') isFirst 
		,(SELECT value FROM UNNEST(event_params) WHERE name='type') type 
		,(SELECT value FROM UNNEST(event_params) WHERE name='from') fromon 	
		,(SELECT value FROM UNNEST(event_params) WHERE name='fromUser') fromUser 
		,(SELECT value FROM UNNEST(event_params) WHERE name='abtestVersion') abtestVersion
		,(SELECT value FROM UNNEST(event_params) WHERE name='placement') placement
		,(SELECT value FROM UNNEST(event_params) WHERE name='timeuse') timeuse
		,(SELECT value FROM UNNEST(event_params) WHERE name='error') steps
		,(SELECT value FROM UNNEST(event_params) WHERE name='error') error_code
		,(SELECT value FROM UNNEST(event_params) WHERE name='win') win
		,(SELECT value FROM UNNEST(event_params) WHERE name='hidesum') hidesum
		,(SELECT value FROM UNNEST(event_params) WHERE name='level') level_id
		,(SELECT value FROM UNNEST(event_params) WHERE name='gameID') gameID
		,(SELECT value FROM UNNEST(event_params) WHERE name='gameShowTime') gameShowTime
		,(SELECT value FROM UNNEST(event_params) WHERE name='code') e_code
		,(SELECT value FROM UNNEST(event_params) WHERE name='score') score
		,(SELECT value FROM UNNEST(event_params) WHERE name='coinsum') coinsum
		,(SELECT value FROM UNNEST(event_params) WHERE name='starsum') starsum
		,(SELECT value FROM UNNEST(event_params) WHERE name='livesum') livesum
		,(SELECT value FROM UNNEST(event_params) WHERE name='coinsum') proptype
		,(SELECT value FROM UNNEST(event_params) WHERE name='propsum') propsum
		,(SELECT value FROM UNNEST(event_params) WHERE name='buytype') buytype
		,(SELECT value FROM UNNEST(event_params) WHERE name='entrance') entrance
		,(SELECT value FROM UNNEST(event_params) WHERE name='eventTime') eventTime
		,(SELECT value FROM UNNEST(event_params) WHERE name='uuid') uuid
		,(SELECT value FROM UNNEST(user_properties) WHERE name='country_code') user_country
	FROM `fb-ai-avatar-puzzle.fbinstant.app_event` 
	WHERE 1=1
	 and  date(TIMESTAMP_TRUNC(event_timestamp_micros, DAY)) >= date_add(run_date,interval -(history_day+1) day)
	 and  date(TIMESTAMP_TRUNC(event_timestamp_micros, DAY)) <= date_add(run_date,interval -history_end_day day);



-------2.dwd_user_active_di
delete `gzdw2024.fbgame_real_01_basic.dwd_user_active_di`
where event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day);


insert `gzdw2024.fbgame_real_01_basic.dwd_user_active_di`
	--drop table if exists  `gzdw2024.fbgame_real_01_basic.dwd_user_active_di`;
	--create table  `gzdw2024.fbgame_real_01_basic.dwd_user_active_di`
	--PARTITION BY event_date as 
SELECT 
	event_date
	,user_id
	,package_name
	,MIN_BY(country_code, event_timestamp_millis) AS country_code
	,max(fromUser) as fromUser
	,max(case when abtestVersion is null then 'A' else abtestVersion end  ) as abtestVersion
	,MIN_BY(platform, event_timestamp_millis) AS platform
FROM `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di`
WHERE 1=1
and event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day)
group by event_date
	,user_id
	,package_name;


-------3.dwd_user_active_profile_di
delete `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
where event_date>=date_add(run_date,interval -hitory_retain_day day)
and event_date<=date_add(run_date,interval -history_end_day day);

insert `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
	--drop table if exists  `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`;
	--create table  `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
	--PARTITION BY event_date as 
	SELECT
		a.event_date
		,a.package_name	
		,a.country_code
		,a.platform
		,a.user_id
		,max(is_launch) as is_launch
		,max(case when a.event_date=event_date_min  then 1 else 0 end ) as is_new
		,max(is_ad) as is_ad
		,max(is_liebian) as is_liebian	
	FROM
		(
		SELECT
			user_id
			,event_date
			,package_name
			,max(country_code) as country_code
			,max(platform) as platform
		FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_di`
		WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
		and event_date<=date_add(run_date,interval -history_end_day day)
		group by user_id,package_name,event_date
		)a 
		join
		(
		 SELECT 
		 	user_id
		 	,package_name
		 	,event_date
		 	,max(case when event_name  in ('fb_zp_app_launch')  then 1 else 0 end) as is_launch
		 	,max(case when event_name  in ('fb_zp_app_launch') and isFirst='true' then 1 else 0 end) as is_new
		 	,max(case when event_name  in ('fb_zp_app_launch') and fromon='ad' then 1 else 0 end) as is_ad
		 	,max(case  when event_name  in ('fb_zp_app_launch') and fromon in ('shareable_link','feed') then 1 else 0 end) as is_liebian
		 FROM `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di` 
		 where 1=1
		 and event_date>=date_add(run_date,interval -hitory_retain_day day)
		 and event_date<=date_add(run_date,interval -history_end_day day)
		 group by user_id,event_date,package_name
		)b 
		on a.user_id=b.user_id
		and a.event_date=b.event_date
		left join 
		(
		SELECT
			user_id
			,package_name
			,min(event_date_min) as event_date_min
		FROM
			(
			SELECT
				user_id
				,min(event_date) as event_date_min
				,package_name
			FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_di`
			WHERE event_date>=date_add(run_date,interval -(hitory_retain_day+150) day)
			and event_date<=date_add(run_date,interval -history_end_day day)
			and event_date>='2024-12-22'
			group by user_id,package_name
			union all 
			SELECT
				fbUserID
				,min(event_date) as event_date_min
				,package_name
			FROM `gzdw2024.fb_zp_game.dwd_user_active_di`
			WHERE event_date>=date_add(run_date,interval -(hitory_retain_day+150) day)
			and event_date<=date_add(run_date,interval -history_end_day day)
			and event_date<'2024-12-22'
			group by fbUserID,package_name
			)a 
			group by user_id,package_name

		)c 
		on a.user_id=c.user_id
		and a.package_name=c.package_name
		group by a.user_id,a.event_date,a.package_name,a.country_code,a.platform;

----------活跃、新增、广告新增、裂变新增


delete `gzdw2024.fbgame_real_01_basic.dws_user_active_report`
where event_date>=date_add(run_date,interval -hitory_retain_day day)
and event_date<=date_add(run_date,interval -history_end_day day);

insert `gzdw2024.fbgame_real_01_basic.dws_user_active_report`
	--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_user_active_report`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_user_active_report`
	--PARTITION BY event_date as 
SELECT
	a.event_date	
	,a.package_name
	,platform
	, country_code
	,count(distinct case when is_launch=1 then a.user_id else null end) as active_uv 
	,count(distinct case when is_launch=1 and is_new=1 then a.user_id else null end) as new_uv
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1 then a.user_id else null end) as new_ad_uv
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as new_liebian_uv 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=1 and is_new=1 then a.user_id else null end) as retain_uv2 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=2 and is_new=1 then a.user_id else null end) as retain_uv3 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=3 and is_new=1 then a.user_id else null end) as retain_uv4 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=4 and is_new=1 then a.user_id else null end) as retain_uv5 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=5 and is_new=1 then a.user_id else null end) as retain_uv6 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=6 and is_new=1 then a.user_id else null end) as retain_uv7 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=7 and is_new=1 then a.user_id else null end) as retain_uv8 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=14 and is_new=1 then a.user_id else null end) as retain_uv15 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=28 and is_new=1 then a.user_id else null end) as retain_uv29
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=1 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv2 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=2 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv3 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=3 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv4 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=4 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv5 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=5 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv6 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=6 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv7 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=7 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv8 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=14 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv15 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=28 and is_new=1  and is_ad=1 then a.user_id else null end) as retain_ad_uv29
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=1 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv2 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=2 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv3 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=3 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv4 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=4 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv5 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=5 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv6 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=6 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv7 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=7 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv8 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=14 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv15 
	,count(distinct case when  date_diff(b.event_date,a.event_date,day)=28 and is_new=1 and is_ad=0 and  is_liebian=1  then a.user_id else null end) as retain_liebian_uv29
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1    then c.user_id else null end) as source_liebian_uv_ad 
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1 and date_diff(c.event_date_min,a.event_date,day)<=0  then c.user_id else null end) as source_liebian_uv_ad_0_day 
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1 and date_diff(c.event_date_min,a.event_date,day)<=1  then c.user_id else null end) as source_liebian_uv_ad_1_day
	,count(distinct case when is_launch=1 and is_new=1 and is_ad=1 and  date_diff(c.event_date_min,a.event_date,day)<=2  then c.user_id else null end) as source_liebian_uv_ad_2_day 
	
FROM
	(

			SELECT
				event_date
				,user_id
				,is_launch
				,is_new
				,is_ad
				,is_liebian
				,array[country_code,'TOTAL'] as country_code
				,array[platform,'TOTAL'] as platform
				,a.package_name
			FROM
				(
				SELECT
					event_date
					,user_id
					,is_launch
					,is_new
					,is_ad
					,is_liebian
					,upper(country_code) as country_code
					,case when lower(platform) ='ios' then 'iOS'
					when lower(platform)  ='android' then 'Android' 
					else 'web' end as platform
					,package_name
				FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				)a 
				
			  )a 
			left join
			(
			SELECT
				event_date
				,user_id
				,package_name
			FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
			WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
			and event_date<=date_add(run_date,interval -history_end_day day)
			group by event_date,user_id,package_name
			)b 
			on a.user_id=b.user_id
			and a.package_name=b.package_name
			left join
			(
			SELECT
				user_id
				,package_name
				,MIN_BY(fromUser, event_date) AS fromUser
				,min(event_date) as event_date_min
			FROM
				(
				SELECT
					event_date
					,user_id
					,max(fromUser) as fromUser
					,package_name
	 			FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -hitory_retain_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				group by event_date,user_id,package_name
				)a 
				group by user_id,package_name
			
			)c
			on a.user_id=c.fromUser
			and a.package_name=c.package_name
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
			group by a.event_date,country_code,platform,package_name;

	


----------广告统计表
delete `gzdw2024.fbgame_real_01_basic.dws_user_ad_report`
where event_date>=date_add(run_date,interval -history_day day)
	and event_date<=date_add(run_date,interval -0  day);

insert `gzdw2024.fbgame_real_01_basic.dws_user_ad_report`
--	drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_user_ad_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_user_ad_report`
--	PARTITION BY event_date as 
		with a as (
		SELECT
			a.event_date	
			,country_code
			,platform
			,placement
			,package_name
			,event_name	
			,user_id
  			,ad_type
  			,is_new
		FROM
			(
			SELECT
				a.event_date
				,a.user_id
				,array[country_code,'TOTAL'] as country_code
				,array[platform,'TOTAL'] as platform

				,array[placement,'TOTAL'] as placement
				,array[ad_type,'TOTAL'] as ad_type
				,event_name
				,a.package_name
				,ARRAY['TOTAL',case when is_new =1 then 'new'
				when is_new =0 then 'old' 
				else 'old' end] as is_new	
			FROM
				(
				SELECT
					 event_date
					,user_id
					,upper(country_code) as country_code
					,case when lower(platform) ='ios' then 'iOS'
					when lower(platform)  ='android' then 'Android' 
					else 'web' end as platform		
					,package_name
					,event_name
					,placement
					,case when lower(placement) like '%banner%' then 'banner'
					when lower(placement) like '%reward%interstitial%' then 'rewarded_interstitial'
					when lower(placement) like '%interstitial%' then 'interstitial'
						when lower(placement) like '%hint%' then 'interstitial'
					else 'other' end as ad_type
				FROM `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				and event_name in ('fb_zp_ad_load_c','fb_zp_ad_load_fail_c','fb_zp_ad_load_success_c','fb_zp_ad_impression_c')
				)a 
			    left	join 
				(
				SELECT
					package_name
					,event_date
					,user_id
					,max(is_new) as is_new
				FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				group by event_date,user_id,package_name
			   )b 
				on a.user_id=b.user_id
				and a.package_name=b.package_name
				and a.event_date=b.event_date
			  )a 
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
			,UNNEST(placement) as placement
      		,UNNEST(ad_type) as ad_type
      		,UNNEST(is_new) as is_new
		),
 c as 
(
	SELECT
	event_date
	,package_name
	,platform
	,country_code
	,is_new
	,placement
	,ad_type
	,count(case when event_name='fb_zp_ad_load_c' then user_id else null end) as load_pv
	,count(case when event_name='fb_zp_ad_load_success_c' then user_id else null end) as load_succ_pv
	,count(case when event_name='fb_zp_ad_load_fail_c' then user_id else null end) as load_fail_pv
	,count(case when event_name='fb_zp_ad_impression_c' then user_id else null end) as impression_pv
	,count(distinct case when event_name='fb_zp_ad_load_c' then user_id else null end) as load_uv
	,count(distinct case when event_name='fb_zp_ad_load_success_c' then user_id else null end) as load_succ_uv
	,count(distinct case when event_name='fb_zp_ad_load_fail_c' then user_id else null end) as load_fail_uv
	,count(distinct case when event_name='fb_zp_ad_impression_c' then user_id else null end) as impression_uv

FROM
	(

		select * 
		from a  
		where 1=1
		--and event_name in ('fb_templ_res_click','fb_temp_export_fail')
		
	)e 
	group by event_date,country_code,platform,package_name,placement,ad_type,is_new
)
SELECT
	c0.event_date
	,c0.package_name
	,c0.platform
	,c0.country_code
	,c0.is_new
	,placement
	,load_pv
	,load_succ_pv
	,load_fail_pv
	,impression_pv
	,load_uv
	,load_succ_uv 
	,load_fail_uv
	,impression_uv
	,case when is_new='TOTAL' then  active_uv
	when is_new='new' then  new_uv
	when is_new='old' then active_uv -  new_uv end as active_uv
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
		,package_name
		,platform
		,country_code
		,active_uv
		,new_uv
	FROM	`gzdw2024.fbgame_real_01_basic.dws_user_active_report`
	WHERE event_date>=date_add(run_date,interval -history_day day)
	and event_date<=date_add(run_date,interval -history_end_day day)
	)c1 
	on c0.event_date=c1.event_date
	and c0.package_name=c1.package_name
	and c0.platform=c1.platform
	and c0.country_code=c1.country_code;



	----------广告请求失败统计表
delete `gzdw2024.fbgame_real_01_basic.dws_user_ad_fail_report`
where event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day);

insert `gzdw2024.fbgame_real_01_basic.dws_user_ad_fail_report`
--	drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_user_ad_fail_report`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_user_ad_fail_report`
	--PARTITION BY event_date as 
	with a as (
	SELECT
		a.event_date	
		,country_code
		,platform
		,error_code
		,ad_type
		,package_name
		,event_name	
		,user_id
		,is_new
		,e_code
	FROM
		(

			SELECT
				a.event_date
				,a.user_id as user_id
				,array[country_code,'TOTAL'] as country_code
				,array[platform,'TOTAL'] as platform
				,array[ad_type,'TOTAL'] as ad_type
				,event_name
				,a.package_name
				,error_code
				,e_code
				,ARRAY['TOTAL',case when is_new =1 then 'new'
				when is_new =0 then 'old' 
				else 'old' end] as is_new	
			FROM
				(
				SELECT
					 event_date
					,user_id
					,upper(country_code) as country_code
					,case when lower(platform) ='ios' then 'iOS'
					when lower(platform)  ='android' then 'Android' 
					else 'web' end as platform			
					,package_name
					,event_name
					,error_code
					,e_code
					,case when lower(placement) like '%banner%' then 'banner'
					when lower(placement) like '%reward%interstitial%' then 'rewarded_interstitial'
					when lower(placement) like '%interstitial%' then 'interstitial'
						when lower(placement) like '%hint%' then 'interstitial'
					else 'other' end as ad_type
				FROM `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				and event_name in ('fb_zp_ad_load_fail_c')
				)a
			   left	join 
				(
				SELECT
					package_name
					,event_date
					,user_id
					,max(is_new) as is_new
				FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				group by event_date,user_id,package_name
			   )b 
				on a.user_id=b.user_id
				and a.package_name=b.package_name
				and a.event_date=b.event_date
			  )a 
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
		     ,UNNEST(is_new) as is_new
		     ,UNNEST(ad_type) as ad_type
	),
 c as 
(
SELECT
	event_date
	,package_name
	,platform
	,country_code
	,is_new
	,error_code
	,ad_type
	,e_code
	,count(case when event_name='fb_zp_ad_load_fail_c' then user_id else null end) as load_fail_pv
FROM
	(

		select * 
		from a  
		where 1=1
		--and event_name in ('fb_templ_res_click','fb_temp_export_fail')
		
	)e 
			group by event_date,country_code,platform,package_name,error_code,ad_type,e_code,is_new
)
SELECT
	c0.event_date
	,c0.package_name
	,c0.platform
	,c0.country_code
	,is_new
	,e_code
	,error_code
	,ad_type
	,load_fail_pv
	,case when is_new='TOTAL' then  active_uv  when is_new='new' then  new_uv
	when is_new='old' then active_uv -  new_uv end as active_uv
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
		,new_uv
	FROM	`gzdw2024.fbgame_real_01_basic.dws_user_active_report`
	WHERE event_date>=date_add(run_date,interval -history_day day)
	and event_date<=date_add(run_date,interval -history_end_day day)

		)c1 
	on c0.event_date=c1.event_date
	and c0.platform=c1.platform
	and c0.country_code=c1.country_code;






------8.各事件活跃次数与人数

delete `gzdw2024.fbgame_real_01_basic.dws_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -history_end_day day);


insert `gzdw2024.fbgame_real_01_basic.dws_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_event_active_report`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_event_active_report`
	--PARTITION BY stats_date as 
	SELECT
		c0.event_date  AS stats_date
		,c0.package_name
		,c0.platform
		,c0.country_code
		,is_new
		,event_name
		,pv 
		,uv 
		,case when is_new='TOTAL' then  active_uv  when is_new='new' then  new_uv
	when is_new='old' then active_uv -  new_uv end as active_uv
	FROM
		(
		SELECT
			event_date
			,package_name
			,platform
			,country_code
			,event_name
			,is_new
			,count(1) as pv 
			,count(distinct user_id) as uv 
		FROM
			(
			SELECT
				a.event_date
				,a.package_name
				,a.user_id 
				,event_name
				,platform
				,country_code
				,ARRAY['TOTAL',case when is_new =1 then 'new'
				when is_new =0 then 'old' 
				else 'old' end] as is_new	
			FROM
				(
				SELECT
					 event_date
					,user_id
					,ARRAY['TOTAL',country_code] as country_code
					,ARRAY['TOTAL',case when lower(platform) ='ios' then 'iOS'
					when lower(platform)  ='android' then 'Android' 
					else 'web' end] as platform						
					,package_name
					,event_name
				FROM `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di`  
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				)a 
			 left	join 
				(
				SELECT
					package_name
					,event_date
					,user_id
					,max(is_new) as is_new
				FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				group by event_date,user_id,package_name
			   )b 
				on a.user_id=b.user_id
				and a.package_name=b.package_name
				and a.event_date=b.event_date
				,UNNEST(platform) AS platform
				,UNNEST(country_code) AS country_code
				
				)c 
				,UNNEST(is_new) AS is_new
			group by event_date
			,package_name
			,platform
			,country_code
			,event_name
			,is_new
			)c0 
			left join
			(
			SELECT
				event_date
				,platform
				,country_code
				,active_uv
				,new_uv
			FROM	`gzdw2024.fbgame_real_01_basic.dws_user_active_report`
			WHERE event_date>=date_add(run_date,interval -history_day day)
			and event_date<=date_add(run_date,interval -history_end_day day)

			)c1 
			on c0.event_date=c1.event_date
			and c0.platform=c1.platform
			and c0.country_code=c1.country_code;


----------------事件参数明细

delete `gzdw2024.fbgame_real_01_basic.dws_fb_events_detail`
where event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day)
;

	insert `gzdw2024.fbgame_real_01_basic.dws_fb_events_detail`
	--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_fb_events_detail`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_fb_events_detail`
	--PARTITION BY event_date as 
	  with a as 
	  (
  		SELECT
  			event_date
  			,package_name
  			,user_id
  			,event_name
			,platform
			,country_code
			,is_new
			,fromon
			,type
			,placement
			,steps
			,error_code
			,e_code
			,win
			,hidesum
			,level_id
			,entrance
		FROM
			(	
			SELECT
				a.event_date
				,a.package_name
				,a.user_id 
				,event_name
				,platform
				,country_code
				,ARRAY['TOTAL',case when is_new =1 then 'new'
				when is_new =0 then 'old' 
				else 'old' end] as is_new
				,fromon
				,type
				,placement
				,steps
				,error_code
				,e_code
				,win
				,hidesum
				,level_id
				,entrance	
			FROM
				(
				SELECT
					 event_date
					,user_id
					,ARRAY['TOTAL',country_code] as country_code
					,ARRAY['TOTAL',case when lower(platform) ='ios' then 'iOS'
					when lower(platform)  ='android' then 'Android' 
					else 'web' end] as platform						
					,package_name
					,event_name
					,fromon
					,type
					,placement
					,steps
					,error_code
					,e_code
					,win
					,hidesum
					,level_id
					,entrance
				FROM `gzdw2024.fbgame_real_01_basic.dwd_all_game_user_event_di`  
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				)a 
			 left	join 
				(
				SELECT
					package_name
					,event_date
					,user_id
					,max(is_new) as is_new
				FROM `gzdw2024.fbgame_real_01_basic.dwd_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and event_date<=date_add(run_date,interval -history_end_day day)
				group by event_date,user_id,package_name
			   )b 
				on a.user_id=b.user_id
				and a.package_name=b.package_name
				and a.event_date=b.event_date
				,UNNEST(platform) AS platform
				,UNNEST(country_code) AS country_code
				
				)c 
				,UNNEST(is_new) AS is_new
			),
	  	 transformed_data AS (
				  SELECT
				    event_name
				    ,event_date
				    ,package_name
				    ,platform
				    ,country_code
				    ,is_new
				    ,key_value.key AS event_key        
				    ,key_value.value AS event_value
				    ,COUNT(*) AS event_num
				    ,COUNT(DISTINCT user_id) AS user_num
				  FROM
				    a,
				    UNNEST([
				      STRUCT('fromon' AS key, fromon AS value),
				      STRUCT('type' AS key, type AS value),
				      STRUCT('placement' AS key, placement AS value),
				      STRUCT('steps' AS key, steps AS value),
				      STRUCT('error_code' AS key, error_code AS value),
				      STRUCT('e_code' AS key, e_code AS value),
				      STRUCT('win' AS key, win AS value),
				      STRUCT('hidesum' AS key, hidesum AS value),
				      STRUCT('level_id' AS key, level_id AS value),
				      STRUCT('entrance' AS key, hidesum AS entrance)
				    ]) AS key_value
				  GROUP BY event_name, event_date, event_key, event_value,package_name,country_code,platform,is_new
				)
	  	 		SELECT
					c0.event_date
					,c0.package_name
					,c0.country_code
					,c0.platform
					,is_new
					,event_name
					,event_key
					,event_value
					,event_num
					,user_num
					,case when is_new='TOTAL' then  active_uv  when is_new='new' then  new_uv
					when is_new='old' then active_uv -  new_uv end as active_uv
				FROM
					(
					SELECT
						event_date
						,package_name
						,country_code
						,platform
						,is_new
						,event_name
						,event_key
						,event_value
						,event_num
						,user_num
					FROM
					    transformed_data
					  )c0
					  left join
						(
						SELECT
							event_date
							,package_name
							,platform
							,country_code
							,active_uv
              				,new_uv
						FROM	`gzdw2024.fbgame_real_01_basic.dws_user_active_report`
						WHERE event_date>=date_add(run_date,interval -history_day day)
						and event_date<=date_add(run_date,interval -history_end_day day)
						)c1 
						on c0.event_date=c1.event_date
						and c0.platform=c1.platform
						and c0.country_code=c1.country_code
						AND c0.package_name=c1.package_name;

----------------BI用事件参数明细
/*
delete `gzdw2024.fbgame_real_01_basic.dws_fb_events_detail`
where event_date>=date_add(run_date,interval -history_day day)
and event_date<=date_add(run_date,interval -history_end_day day)
;

	insert `gzdw2024.fbgame_real_01_basic.dws_fb_events_detail`
	drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_fb_events_detail`;
	create table  `gzdw2024.fbgame_real_01_basic.dws_fb_events_detail`
	PARTITION BY event_date as 
	SELECT
		event_date
		,package_name
		,country_code
		,platform
		,is_new
		,event_name
		,event_key
		,event_value
		,event_num
		,user_num
	where event_name in ()
*/


end;
