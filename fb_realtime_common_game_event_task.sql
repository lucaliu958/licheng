CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.fb_realtime_common_game_event_task`(run_date DATE, history_day INT64, history_retain_day INT64, history_end_day INT64)
begin



-------0.dwd_all_game_user_event_di
delete `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di`
where event_day>=date_add(run_date,interval -(history_day+1) day)
and event_day<=date_add(run_date,interval -history_end_day day );



insert `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di`
	--drop table if exists  `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di`;
	--create table  `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di`
	--PARTITION BY event_date as 
	SELECT
		date(TIMESTAMP_TRUNC(event_timestamp_micros, DAY)) as event_day
		,date(format_timestamp("%Y-%m-%d", timestamp_seconds( cast ((event_timestamp_millis/1000) as int64)),'America/Los_Angeles')) as event_date 
		,event_name
		,case when app_id ='2693472074192945' then 'fb.ai.avatar.puzzle' 
        when app_id ='3755114334752318' then 'fb.zp' 
		when app_id ='429751963482400' then 'fb.otme.fate.quest' 
		when app_id ='8700493450016813' then 'fb.fruit.bubble' 
		when app_id ='539697615554300' then 'fb.ai.aha'
		when app_id ='430206880027412' then 'fb.save.dog' 
		when app_id ='1280174103111730' then 'fb.egg.bubble' 
		when app_id ='553262573823399' then 'fb.bubble.shoot.pro'
		else 'other'  end  as package_name
		
	    ,event_timestamp_micros
		,format_timestamp("%Y-%m-%d %H:%M:%S", timestamp_seconds( cast ((event_timestamp_millis/1000) as int64)),'America/Los_Angeles') as event_time
		,substr(cast(format_timestamp("%Y-%m-%d %H:%M:%S", timestamp_seconds( cast ((event_timestamp_millis/1000) as int64)),'America/Los_Angeles') as string),12,2) as event_hour
		,player_id as user_id
		,event_timestamp_millis	
		,upper(country) as country_code
		,case when lower(platform) like '%android%' then 'Android'
			when lower(platform) like '%ios%' then 'iOS'
			when lower(platform) like '%web%' then 'web'
			else 'other' 
			end as platform
		,os_version
		,device_model
		,user_default_language
		,(SELECT value FROM UNNEST(user_properties) WHERE name='country_code') user_country
		,(SELECT value FROM UNNEST(event_params) WHERE name='fbUserID') fbUserID 
		,(SELECT value FROM UNNEST(event_params) WHERE name='isFirst') isFirst 
		,(SELECT value FROM UNNEST(event_params) WHERE name='type') type 
		,(SELECT value FROM UNNEST(event_params) WHERE name='from') fromon 	
		,(SELECT value FROM UNNEST(event_params) WHERE name='fromUser') fromUser 
		,(SELECT value FROM UNNEST(event_params) WHERE name='abtestVersion') abtestVersion
		,(SELECT value FROM UNNEST(event_params) WHERE name='placement') placement
		,(SELECT value FROM UNNEST(event_params) WHERE name='timeuse') timeuse
		,(SELECT value FROM UNNEST(event_params) WHERE name='steps') steps
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
		,(SELECT value FROM UNNEST(event_params) WHERE name='id') cat_id
		,(SELECT value FROM UNNEST(event_params) WHERE name='proplist') proplist
		,(SELECT value FROM UNNEST(event_params) WHERE name='propselect') propselect
		,(SELECT value FROM UNNEST(event_params) WHERE name='browserType') browserType
		,(SELECT value FROM UNNEST(event_params) WHERE name='bombsum') bombsum
		,(SELECT value FROM UNNEST(event_params) WHERE name='uuid') combomax
	FROM `fb-ai-avatar-puzzle.fbinstant.app_event` 
	WHERE 1=1
	 and  date(TIMESTAMP_TRUNC(event_timestamp_micros, DAY)) >= date_add(run_date,interval -(history_day+1) day)
	 and  date(TIMESTAMP_TRUNC(event_timestamp_micros, DAY)) <= date_add(run_date,interval -history_end_day day );








-------1.dwd_common_game_user_active_di
delete `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_di`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );


insert `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_di`
	--drop table if exists `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_di`;
--create table `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_di`
--	PARTITION BY event_date as 
SELECT 
	event_date
	,user_id
	,package_name
	,MIN_BY(country_code, event_timestamp_millis) AS country_code
	,max(fromUser) as fromUser
	,MIN_BY(platform, event_timestamp_millis) AS platform
FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di`
WHERE 1=1
and event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day )
group by event_date
	,user_id
	,package_name;


-------2.dwd_common_game_user_active_profile_di
delete `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
		--drop table if exists `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`;
--create table `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
--	PARTITION BY event_date as 
SELECT
	a.user_id
	,a.event_date	
	,max(is_launch) as is_launch
	,max(case when a.event_date=event_date_min  then 1 else 0 end ) as is_new
	,max(is_ad) as is_ad
	,max(is_liebian) as is_liebian
	,a.package_name
	,a.country_code
	,a.platform
FROM
	(
	SELECT
		user_id
		,event_date
		,package_name
		,max(country_code) as country_code
		,max(platform) as platform
	FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_di`
	WHERE event_date>=date_add(run_date,interval -history_retain_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	group by user_id,package_name,event_date
	)a 
	join
	(
	 SELECT 
	 	user_id
	 	,package_name
	 	,event_date
	 	,max(case when event_name  like '%app_launch%'  then 1 else 0 end) as is_launch
	 	,max(case when event_name  like '%app_launch%' and isFirst='true' then 1 else 0 end) as is_new
	 	,max(case when event_name  like '%app_launch%' and fromon='ad' then 1 else 0 end) as is_ad
	 	,max(case  when event_name like '%app_launch%' and fromon in ('shareable_link','feed') then 1 else 0 end) as is_liebian
	 FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di` 
	 where 1=1
	 and event_date>=date_add(run_date,interval -history_retain_day day )
	 and event_date<=date_add(run_date,interval -history_end_day day )
	 group by user_id,event_date,package_name
	)b 
	on a.user_id=b.user_id
	and a.event_date=b.event_date
	and a.package_name=b.package_name
	left join 
	(
	SELECT
		package_name
		,user_id
		,min(event_date_min) as event_date_min
	FROM
		(
		SELECT
			user_id
			,min(event_date) as event_date_min
			,package_name
		FROM `gzdw2024.fbgame_01_basic.dwd_common_game_user_active_di`
		WHERE event_date>=date_add(run_date,interval -(history_retain_day +150) day)
		and event_date<=date_add(run_date,interval -history_end_day day )
		group by user_id,package_name
		union all 
		SELECT
			user_id
			,min(event_date) as event_date_min
			,package_name
		FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_di`
		WHERE event_date>=date_add(run_date,interval -(history_retain_day +150) day)
		and event_date<=date_add(run_date,interval -history_end_day day )
		group by user_id,package_name
		)a 
		group by user_id,package_name
	)c 
	on a.user_id=c.user_id
	and a.package_name=c.package_name
	group by a.user_id,a.event_date,a.package_name,a.country_code,a.platform;

----------3.活跃、新增、广告新增、裂变新增
delete `gzdw2024.fbgame_real_01_basic.dws_common_game_user_active_report`
where event_date>=date_add(run_date,interval -history_retain_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_game_user_active_report`
	--create table `gzdw2024.fbgame_real_01_basic.dws_common_game_user_active_report`
	--PARTITION BY event_date as 
		SELECT
			a.event_date
			,a.package_name	
			,country_code
			,platform
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
			,
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
				,package_name
			FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
			WHERE event_date>=date_add(run_date,interval -history_retain_day day )
			and event_date<=date_add(run_date,interval -history_end_day day )	
		    )a 
			left join
			(
			SELECT
				event_date
				,user_id
				,package_name
			FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
			WHERE event_date>=date_add(run_date,interval -history_retain_day day )
			and event_date<=date_add(run_date,interval -history_end_day day )
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
	 			FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_retain_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
				group by event_date,user_id,package_name
				)a 
				group by user_id,package_name			
			)c
			on a.user_id=c.fromUser
			and a.package_name=c.package_name
			,UNNEST(country_code) as country_code
			,UNNEST(platform) as platform
			group by a.event_date,country_code,platform,package_name;






------4.各事件活跃次数与人数

delete `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );


insert `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
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
					,array[platform,'TOTAL']  as platform					
					,package_name
					,event_name
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di`  
				WHERE event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
				)a 
			 left	join 
				(
				SELECT
					package_name
					,event_date
					,user_id
					,max(is_new) as is_new
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
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
				,package_name
				,platform
				,country_code
				,active_uv
				,new_uv
			FROM	`gzdw2024.fbgame_real_01_basic.dws_common_game_user_active_report`
			WHERE event_date>=date_add(run_date,interval -history_day day )
			and event_date<=date_add(run_date,interval -history_end_day day )

			)c1 
			on c0.event_date=c1.event_date
			and c0.platform=c1.platform
			and c0.country_code=c1.country_code
			and c0.package_name=c1.package_name;


------4.1乙游事件
delete `gzdw2024.fbgame_real_01_basic.dws_common_fq_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_fq_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_fq_event_active_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_fq_event_active_report`
--	PARTITION BY stats_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
	WHERE 1=1
	and stats_date>=date_add(run_date,interval -history_day day )
	and stats_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.otme.fate.quest';


------4.2纸牌事件
delete `gzdw2024.fbgame_real_01_basic.dws_common_zp_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_zp_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_zp_event_active_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_zp_event_active_report`
--	PARTITION BY stats_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
	WHERE 1=1
	and stats_date>=date_add(run_date,interval -history_day day )
	and stats_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.zp';



------4.3 oho事件
delete `gzdw2024.fbgame_real_01_basic.dws_common_oho_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_oho_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_oho_event_active_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_oho_event_active_report`
--	PARTITION BY stats_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
	WHERE 1=1
	and stats_date>=date_add(run_date,interval -history_day day )
	and stats_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.ai.avatar.puzzle';

------4.4 水果泡事件
delete `gzdw2024.fbgame_real_01_basic.dws_common_fruit_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_fruit_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_fruit_event_active_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_fruit_event_active_report`
--	PARTITION BY stats_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
	WHERE 1=1
	and stats_date>=date_add(run_date,interval -history_day day )
	and stats_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.fruit.bubble';

------4.5 AHA事件
delete `gzdw2024.fbgame_real_01_basic.dws_common_aha_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_aha_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_aha_event_active_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_aha_event_active_report`
--	PARTITION BY stats_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
	WHERE 1=1
	and stats_date>=date_add(run_date,interval -history_day day )
	and stats_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.ai.aha';


------4.6 DOG事件
delete `gzdw2024.fbgame_real_01_basic.dws_common_dog_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_dog_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_dog_event_active_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_dog_event_active_report`
--	PARTITION BY stats_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
	WHERE 1=1
	and stats_date>=date_add(run_date,interval -history_day day )
	and stats_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.save.dog';


------4.7 egg事件
delete `gzdw2024.fbgame_real_01_basic.dws_common_egg_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_egg_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_egg_event_active_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_egg_event_active_report`
--	PARTITION BY stats_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
	WHERE 1=1
	and stats_date>=date_add(run_date,interval -history_day day )
	and stats_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.egg.bubble';

------4.8 无限泡事件
delete `gzdw2024.fbgame_real_01_basic.dws_common_bubble_pro_event_active_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_bubble_pro_event_active_report`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_bubble_pro_event_active_report`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_bubble_pro_event_active_report`
--	PARTITION BY stats_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
	WHERE 1=1
	and stats_date>=date_add(run_date,interval -history_day day )
	and stats_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.bubble.shoot.pro';



----------------5.事件参数明细

delete `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

	insert `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
--	PARTITION BY event_date as 
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
			,error_code
			,e_code
			,win
			,level_id
			,cat_id
			,buytype
			,type
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
				,error_code
				,e_code
				,win
				,level_id
				,cat_id
				,buytype
				,type
			FROM
				(
				SELECT
					 event_date
					,user_id
					,ARRAY['TOTAL',country_code] as country_code
					,array[platform,'TOTAL']  as platform					
					,package_name
					,event_name
					,fromon
					,error_code
					,e_code
					,win
					,level_id
					,cat_id
					,buytype
					,type
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di`  
				WHERE event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
				and (event_name in (
					'otme_app_launch'
					,'otme_share_click'
					,'otme_game_show'
					,'otme_game_click'
					,'otme_game_start'
					,'otme_game_keep'
					,'otme_game_finish'
					,'otme_game_back'
					,'otme_changeface_start'
					,'otme_changeface_succ'
					,'otme_changeface_fail'
					,'otme_change_switch'
					,'fb_zp_game_play_finish'
					,'fb_zp_app_launch'
					,'fb_zp_app_launch'
					,'fb_templ_res_show'
					,'fb_templ_res_click'
					,'fb_templ_res_export'
					,'fb_share_show'
					,'fb_templ_categ_show'
					,'fb_templ_categ_click'
					,'fb_templ_categ_export'
					,'fb_templ_categ_share'
					,'fb_fruit_shop_buyprop'
					,'fb_fruit_app_launch'
					,'fb_app_launch'
					,'fb_mess_export'
						)
				or REGEXP_CONTAINS(event_name, r'.*(app_launch|game_play_finish|game_play_start|fb_ibb_game_play_fail)$'))
				)a 
			 left	join 
				(
				SELECT
					package_name
					,event_date
					,user_id
					,max(is_new) as is_new
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
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
				      STRUCT('error_code' AS key, error_code AS value),
				      STRUCT('e_code' AS key, e_code AS value),
				      STRUCT('win' AS key, win AS value),
				      STRUCT('level_id' AS key, level_id AS value),
				      STRUCT('cat_id' AS key, cat_id AS value),
				      STRUCT('buytype' AS key, cat_id AS value),
  					STRUCT('type' AS key, type AS value)
				    ]) AS key_value
				  GROUP BY event_name, event_date, event_key, event_value,package_name,country_code,platform,is_new
				)
	  	 		SELECT
					c0.event_date
					,c0.package_name
					,c0.country_code
					,c0.platform
					,c0.is_new
					,c0.event_name
					,event_key
					,event_value
					,event_num
					,user_num
					,active_uv
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
							stats_date
							,package_name
							,platform
							,country_code
							,is_new
							,event_name
							,active_uv
						FROM	`gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
						WHERE stats_date>=date_add(run_date,interval -history_day day )
						and stats_date<=date_add(run_date,interval -history_end_day day )
						)c2 
						on c0.event_date=c2.stats_date
						and c0.platform=c2.platform
						and c0.country_code=c2.country_code
						AND c0.package_name=c2.package_name
						and c0.is_new=c2.is_new
						and c0.event_name=c2.event_name;




------5.1乙游事件明细
delete `gzdw2024.fbgame_real_01_basic.dws_common_fq_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_fq_events_detail`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_fq_events_detail`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_fq_events_detail`
	--PARTITION BY event_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	WHERE 1=1
	and event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.otme.fate.quest';


------5.2纸牌事件明细
delete `gzdw2024.fbgame_real_01_basic.dws_common_zp_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_zp_events_detail`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_zp_events_detail`;
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_zp_events_detail`
	--PARTITION BY event_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	WHERE 1=1
	and event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.zp';



------5.3 oho事件明细
delete `gzdw2024.fbgame_real_01_basic.dws_common_oho_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_oho_events_detail`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_oho_events_detail`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_oho_events_detail`
	--PARTITION BY event_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	WHERE 1=1
	and event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.ai.avatar.puzzle';



------5.4 fruit事件明细
delete `gzdw2024.fbgame_real_01_basic.dws_common_fruit_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_fruit_events_detail`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_fruit_events_detail`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_fruit_events_detail`
	--PARTITION BY event_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	WHERE 1=1
	and event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.fruit.bubble';


------5.5 aha事件明细
delete `gzdw2024.fbgame_real_01_basic.dws_common_aha_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_aha_events_detail`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_aha_events_detail`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_aha_events_detail`
	--PARTITION BY event_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	WHERE 1=1
	and event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.ai.aha';


------5.6 dog事件明细
delete `gzdw2024.fbgame_real_01_basic.dws_common_dog_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_dog_events_detail`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_dog_events_detail`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_dog_events_detail`
	--PARTITION BY event_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	WHERE 1=1
	and event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.save.dog';


------5.7 egg事件明细
delete `gzdw2024.fbgame_real_01_basic.dws_common_egg_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_egg_events_detail`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_egg_events_detail`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_egg_events_detail`
	--PARTITION BY event_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	WHERE 1=1
	and event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.egg.bubble';

	------5.8 无限泡事件明细
delete `gzdw2024.fbgame_real_01_basic.dws_common_bubble_pro_events_detail`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_bubble_pro_events_detail`
--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_bubble_pro_events_detail`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_bubble_pro_events_detail`
	--PARTITION BY event_date as 
	SELECT
		* 
	FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_events_detail`
	WHERE 1=1
	and event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	and package_name='fb.bubble.shoot.pro';








-------6.1人均广告数据

delete `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_report`
where event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_report`
	--drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_report`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_report`
	--PARTITION BY event_date as 
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
					,platform		
					,package_name
					,event_name
					,placement
					,case when (lower(placement)  like '%banner%' or lower(type) like '%banner%')  then 'banner'
					when (lower(placement) like '%reward%interstitial%' or lower(type) like '%reward%interstitial%' or event_name like '%reward_interstitial_skip%') then 'rewarded_interstitial'
					when (lower(placement) like '%interstitial%' or lower(type)like '%interstitial%' )  then 'interstitial'
					when (lower(placement) like '%reward%video%' or lower(type) like '%reward%video%') then 'rewarded_video'
					--	when lower(placement) like '%hint%' then 'interstitial'
					else 'other' end as ad_type
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
				and REGEXP_CONTAINS(event_name, r'.*(ad_load_c|ad_load_fail_c|ad_load_success_c|ad_impression_c|ad_expect_impression_c|ad_impression_fail_c|reward_interstitial_skip)$')
				)a 
			    left	join 
				(
				SELECT
					package_name
					,event_date
					,user_id
					,max(is_new) as is_new
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
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
	,ad_type
	,placement
	,count(case when event_name like '%ad_load_c' then user_id else null end) as load_pv
	,count(case when event_name like '%ad_load_success_c' then user_id else null end) as load_succ_pv
	,count(case when event_name like '%ad_load_fail_c'  then user_id else null end) as load_fail_pv
	,count(case when event_name like '%ad_impression_c' or event_name like '%reward_interstitial_skip' then user_id else null end) as impression_pv
	,count(case when event_name like '%ad_expect_impression_c' then user_id else null end) as expect_pv
	,count(case when event_name like '%ad_impression_fail_c' then user_id else null end) as impression_fail_pv
	,count(distinct case when event_name like '%ad_load_c' then user_id else null end) as load_uv
	,count(distinct case when event_name like '%ad_load_success_c' then user_id else null end) as load_succ_uv
	,count(distinct case when event_name like '%ad_load_fail_c' then user_id else null end) as load_fail_uv
	,count(distinct case when event_name like '%ad_impression_c' or event_name like '%reward_interstitial_skip' then user_id else null end) as impression_uv
	,count(distinct case when event_name like '%ad_expect_impression_c' then user_id else null end) as expect_uv
	,count(distinct case when event_name like '%ad_impression_fail_c' then user_id else null end) as impression_fail_uv
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
	,ad_type
	,placement
	,load_pv
	,load_succ_pv
	,load_fail_pv
	,impression_pv
	,impression_fail_pv
	,expect_pv
	,load_uv
	,load_succ_uv 
	,load_fail_uv
	,impression_uv
	,impression_fail_uv
	,expect_uv
	,case when is_new='TOTAL' then  active_uv
	when is_new='new' then  new_uv
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
		,package_name
		,platform
		,country_code
		,active_uv
		,new_uv
	FROM	`gzdw2024.fbgame_real_01_basic.dws_common_game_user_active_report`
	WHERE event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )
	)c1 
	on c0.event_date=c1.event_date
	and c0.package_name=c1.package_name
	and c0.platform=c1.platform
	and c0.country_code=c1.country_code;



---------6.2广告请求失败统计表
delete `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_fail_report`
where event_date>=date_add(run_date,interval -history_day day )
and event_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_fail_report`
--	drop table if exists  `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_fail_report`;
	--create table  `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_fail_report`
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
					,platform				
					,package_name
					,event_name
					,error_code
					,e_code
					,case when (lower(placement)  like '%banner%' or lower(type) like '%banner%')  then 'banner'
					when (lower(placement) like '%reward%interstitial%' or lower(type) like '%reward%interstitial%') then 'rewarded_interstitial'
					when (lower(placement) like '%interstitial%' or lower(type)like '%interstitial%' )  then 'interstitial'
					when lower(placement) like '%reward%video%' then 'rewarded_video'
					--	when lower(placement) like '%hint%' then 'interstitial'
					else 'other' end as ad_type
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
				and REGEXP_CONTAINS(event_name, r'.*(ad_load_fail_c|ad_impression_fail_c)$')
				
				)a
			   left	join 
				(
				SELECT
					package_name
					,event_date
					,user_id
					,max(is_new) as is_new
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_active_profile_di`
				WHERE event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
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
	,count(case when event_name like '%ad_load_fail_c' then user_id else null end) as load_fail_pv
	,count(case when event_name like '%ad_impression_fail_c' then user_id else null end) as impression_fail_pv
	,count(distinct case when event_name like '%ad_load_fail_c' then user_id else null end) as load_fail_uv
	,count(distinct case when event_name like '%ad_impression_fail_c' then user_id else null end) as impression_fail_uv
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
	,impression_fail_pv
	,load_fail_uv
	,impression_fail_uv
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
		package_name
		,event_date
		,platform
		,country_code
		,active_uv
		,new_uv
	FROM	`gzdw2024.fbgame_real_01_basic.dws_common_game_user_active_report`
	WHERE event_date>=date_add(run_date,interval -history_day day )
	and event_date<=date_add(run_date,interval -history_end_day day )

		)c1 
	on c0.event_date=c1.event_date
	and c0.platform=c1.platform
	and c0.country_code=c1.country_code
	and c0.package_name=c1.package_name;



--------6.3.fb后台广告统计表

delete `gzdw2024.fbgame_real_01_basic.dws_common_game_user_fb_ad_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_game_user_fb_ad_report`
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_game_user_fb_ad_report`
	--	PARTITION BY stats_date as 
		SELECT
			stats_date
			,c0.package_name
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
					,case when app_name='Solitaire' then 'fb.zp' 
					when app_name='OHO' then 'fb.ai.avatar.puzzle' 
					when app_name='Bubble Pop Fruit' then 'fb.fruit.bubble' 
					when app_name='AHA' then 'fb.ai.aha' 
					when app_name='Fate Quest' then 'fb.otme.fate.quest' 
					when app_name='Block Juggle' then 'fb.block.juggle' 
					when app_name='Bubble Shoot Pro' then 'fb.bubble.shoot.pro'
					when app_name='Save The Dog' then 'fb.save.dog' 
                                        when app_name='Egg Shoot Dino' then 'fb.egg.bubble' 
					else 'other' end 
					as package_name
					,array['TOTAL',case when lower(platform)='ios' then 'iOS' when lower(platform)='android' then 'Android' else 'web' end]  as platform
					,array['TOTAL',upper(country)] as country_code
					,array['TOTAL',case when lower(placement_name) like '%banner%' then 'banner'
					when lower(placement_name) like '%rewarded%interstitial%' or lower(placement_name) like '%激励插屏%' then 'rewarded_interstitial'
					when lower(placement_name) like '%interstitial%' then 'interstitial'
					when lower(placement_name) like '%reward%video%' or lower(placement_name) like '%激励视频%' then 'rewarded_video'
					else 'interstitial' end ] as ad_type
					,requests
					,filled_requests
					,impressions
					,revenue
					,clicks
				FROM `fb-ai-avatar-puzzle.analytics_439907691.ad_analytics_detail_day_*` 
				where _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day ) as string),'-','')
				and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day ) as string),'-','')
				)c 
				,UNNEST(platform) as platform
				,UNNEST(country_code) as country_code
				,UNNEST(ad_type) as ad_type
				group by stats_date,platform,country_code,ad_type,package_name
			)c0
			left join
			(
			SELECT
				package_name
				,event_date
				,platform
				,country_code
				,active_uv
				,new_uv
			FROM	`gzdw2024.fbgame_real_01_basic.dws_common_game_user_active_report`
			WHERE event_date>=date_add(run_date,interval -history_day day )
			and event_date<=date_add(run_date,interval -history_end_day day )

				)c1 
			on c0.stats_date=c1.event_date
			and c0.platform=c1.platform
			and c0.country_code=c1.country_code
			and c0.package_name=c1.package_name;



--------6.5.期望展示统计表

delete `gzdw2024.fbgame_real_01_basic.dws_common_game_ad_expect_show_report`
where stats_date>=date_add(run_date,interval -history_day day )
and stats_date<=date_add(run_date,interval -history_end_day day );

insert `gzdw2024.fbgame_real_01_basic.dws_common_game_ad_expect_show_report`
--	create table  `gzdw2024.fbgame_real_01_basic.dws_common_game_ad_expect_show_report`
	--	PARTITION BY stats_date as 
		SELECT
			a.stats_date
			,a.package_name
			,a.platform
			,a.country_code
			,pv 
			,impressions
			,impression_pv
			,expect_pv
		FROM
			(
			SELECT
				stats_date
				,package_name
				,platform
				,country_code
				,count(1) as pv 
			FROM
				(
				SELECT
					 event_date as stats_date
					,user_id
					,ARRAY['TOTAL',upper(country_code)] as country_code
					,ARRAY['TOTAL',platform] as platform				
					,package_name
					,event_name
				FROM `gzdw2024.fbgame_real_01_basic.dwd_common_game_user_event_di`    
				WHERE 1=1
				and  (
					event_name IN ('fb_zp_new_game_play','fb_zp_game_shuffle_clickads','fb_zp_fgame_star_click','fb_zp_game_refillstock_click','fb_zp_game_homeshuffle_clickads'
						           ,'fb_fruit_get_heart_click','fb_fruit_get_coin_click','fb_fruit_shop_coinclick'
						           ,'fb_fruit_daily_draw_watch_ad_click','fb_fruit_daily_draw_double_ad_click','fb_fruit_sign_clickdouble'
						           ,'fb_fruit_shop_clickdouble','fb_fruit_game_play_fail_clickcoin','fb_fruit_game_play_fail_continuefree'
						           ,'fb_fruit_game_prop_clickad','fb_fruit_get_morelives_clickcoin','fb_fruit_get_morelives_freelives'
						           ,'fb_fruit_game_play_finish','fb_fruit_game_play_fail','fb_fruit_game_play_start_adclick'
							   ,'fb_fruit_game_prop_clickad','fb_fruit_get_morelives_clickcoin','fb_fruit_get_morelives_freelives'
						           ,'fb_dog_game_market_bomb_get','fb_dog_game_brush_get_click','fb_dog_game_skip_click','fb_dog_game_hint_click','fb_dog_game_play_finish'
							   ,'fb_egg_change_eggs_click','fb_egg_double_bomb_click','fb_egg_break_click','fb_egg_over_click','fb_egg_again_revive_click'
							   ,'fb_egg_break_clickbutton','fb_egg_game_play_finish','fb_egg_game_play_fail'
							   ,'fb_ibb_fail_thanks_click','fb_ibb_fail_continue_click','fb_ibb_fail_goto_ads')
					or (event_name in ('fb_zp_game_play_finish') and win='true')
					or (event_name in ('fb_dog_game_play_succ_next') and type='is_ads')
					or (event_name in ('fb_egg_bomb_click','fb_ibb_bomb_click') and bombsum='0')
					)

				AND event_date>=date_add(run_date,interval -history_day day )
				and event_date<=date_add(run_date,interval -history_end_day day )
				)a 
				,UNNEST(country_code) as country_code
				,UNNEST(platform) as platform
				group by a.stats_date,package_name,platform,Country_code
			)a 
			left join
			(
				SELECT
				 stats_date
				,package_name
				,platform
				,country_code
				,sum(impressions ) as impressions 
			FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_user_fb_ad_report`
			WHERE (ad_type like '%interstitial%' or  ad_type like '%video%')
			AND stats_date>=date_add(run_date,interval -history_day day )
			and stats_date<=date_add(run_date,interval -history_end_day day )
			group by stats_date,package_name,platform,Country_code
			)b 
			on a.stats_date=b.stats_date
			and a.package_name=b.package_name
			and a.platform=b.platform
			and a.country_code=b.country_code
			left join
			(
				SELECT
				event_date stats_date
				,package_name
				,platform
				,country_code
				,sum(impression_pv ) as impression_pv 
			FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_user_ad_report`
			WHERE (ad_type like '%interstitial%' or  ad_type like '%video%')
			AND event_date>=date_add(run_date,interval -history_day day )
			and event_date<=date_add(run_date,interval -history_end_day day )
			and is_new='TOTAL'
			AND placement='TOTAL'
			group by stats_date,package_name,platform,Country_code
			)c 
			on a.stats_date=c.stats_date
			and a.package_name=c.package_name
			and a.platform=c.platform
			and a.country_code=c.country_code
			left join
			(
				SELECT
				stats_date
				,package_name
				,platform
				,country_code
				,sum(pv ) as expect_pv 
			FROM `gzdw2024.fbgame_real_01_basic.dws_common_game_event_active_report`
			WHERE 1=1
			AND stats_date>=date_add(run_date,interval -history_day day )
			and stats_date<=date_add(run_date,interval -history_end_day day )
			and is_new='TOTAL'
			and REGEXP_CONTAINS(event_name, r'.*(ad_expect_impression_c)$')		
			group by stats_date,package_name,platform,country_code
			)d 
			on a.stats_date=d.stats_date
			and a.package_name=d.package_name
			and a.platform=d.platform
			and a.country_code=d.country_code;
			--where a.platform='TOTAL'
			--AND a.country_code='TOTAL';


delete `gzdw2024.fbgame_real_01_basic.dws_upadate_total`
where cat='event_day_real';

insert `gzdw2024.fbgame_real_01_basic.dws_upadate_total`
--create table `gzdw2024.fbgame_real_01_basic.dws_upadate_total`	as 
SELECT 'event_day_real' as cat 
,SUBSTRING(CAST(DATETIME(CURRENT_TIMESTAMP(), "Asia/Shanghai") AS STRING),1,19) AS update_time;





end;
