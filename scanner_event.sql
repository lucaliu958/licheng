CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.scanner_event`(run_date DATE, history_day INT64, hitory_retain_day INT64,history_end_day INT64)
begin


-------1.dwd_user_event_di
delete gzdw2024.scanner_02_event.dwd_user_event_di
where event_date>=date_add(run_date,interval -history_day day)
;


insert gzdw2024.scanner_02_event.dwd_user_event_di

with  active_info as(
select distinct
event_date
,user_pseudo_id
FROM  `scanner-master-android.analytics_196427335.events_*`,unnest(event_params) event_params
WHERE  _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','') 
and ((event_name in ('user_engagement','screen_view','app_exception') and event_params.key='engagement_time_msec')  or event_name in('first_open'))
and user_pseudo_id is not null
),

vip_info as(
select event_date,user_pseudo_id,max(case when is_vip in (1,2) then is_vip else 0 end) is_vip 
from (
select 
event_date
,user_pseudo_id
,safe_cast((SELECT value.string_value 
   FROM UNNEST(user_properties) WHERE key='is_vip') as bigint) as is_vip
FROM  `scanner-master-android.analytics_196427335.events_*`
WHERE  _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','') 
and user_pseudo_id is not null
) group by event_date,user_pseudo_id
)

SELECT 
PARSE_DATE('%Y%m%d',a.event_date) event_date
,a.user_pseudo_id
,event_name
,count(*) AS event_num
,cast(min(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',TIMESTAMP_MICROS(event_timestamp))) as datetime) as first_event_time
,is_vip
,case when geo.country='' or geo.country is null then 'undefined' else geo.country end as country
,case when geo.city='' or geo.city is null then 'undefined' else geo.city end as city
,case when traffic_source.name='' or traffic_source.name is null then 'undefined' else traffic_source.name end as traffic_source_name
,case when lower(traffic_source.name) like '%inhouse%' then 'inhouse' 
      when traffic_source.source like '%link%' then 'inhouse'
      when (traffic_source.name in('(direct)')  or traffic_source.name is null) then 'nature' 
      else 'delivery' end traffic_source_type

--,case when lower(traffic_source.name) like '%inhouse%' then 'inhouse' when traffic_source.medium = "organic" then "nature"  else 'delivery' end traffic_source_type
,case when traffic_source.medium='' or traffic_source.medium is null then 'undefined' else traffic_source.medium end as traffic_source_medium
,case when traffic_source.source='' or traffic_source.source is null then 'undefined' else traffic_source.source end as traffic_source_source
,case when device.category='' or device.category is null then 'undefined' else device.category end as device_category
,case when device.mobile_brand_name='' or device.mobile_brand_name is null then 'undefined' else device.mobile_brand_name  end as mobile_brand_name
,case when device.mobile_model_name='' or device.mobile_model_name is null then 'undefined' else device.mobile_model_name end as mobile_model_name
,case when device.mobile_marketing_name='' or device.mobile_marketing_name is null then 'undefined' else device.mobile_marketing_name end as mobile_marketing_name
,case when device.operating_system_version='' or device.operating_system_version is null then 'undefined' else device.operating_system_version end as operating_system_version
,case when device.language='' or device.language is null then 'undefined' else device.language end as language
,case when app_info.id='' or app_info.id is null then 'undefined' else app_info.id end AS package_name
,case when app_info.version='' or app_info.version is null then 'undefined' else app_info.version end as app_version

FROM `scanner-master-android.analytics_196427335.events_*` a 
left join vip_info b on a.event_date=b.event_date and a.user_pseudo_id=b.user_pseudo_id
WHERE _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','') 
and a.user_pseudo_id is not null
GROUP BY 
a.event_date
,app_info.id
,app_info.version
,a.user_pseudo_id
,geo.country
,geo.city
,traffic_source.name
,traffic_source_type
,traffic_source.medium
,traffic_source.source
,device.category
,device.mobile_brand_name
,device.mobile_model_name
,device.mobile_marketing_name
,device.operating_system_version
,device.language
,event_name
,is_vip
;






-------history_day.dwd_user_active_di
delete gzdw2024.scanner_01_basic.dwd_user_active_di
where event_date>=date_add(run_date,interval -history_day day)
;

insert gzdw2024.scanner_01_basic.dwd_user_active_di
select 
a.event_date
,a.user_pseudo_id
,min(case when event_name in ('user_engagement','screen_view','app_exception','first_open') then first_event_time end) as first_active_time 
,max(case when event_name='first_open' then 1 else 0 end) is_new
,max(case when event_name='app_remove' then 1 else 0 end) is_remove
,max(case when event_name='in_app_purchase' then 1 else 0 end) is_purchase
,max(case when b.user_pseudo_id is not null then 1 else 0 end) is_retain
,is_vip
,min_by(country,first_event_time) country
,min_by(city,first_event_time) city
,case when lower(traffic_source_name) like '%inhouse%' then 'inhouse' when (traffic_source_name in('(direct)','undefined')  or traffic_source_name is null) then 'nature' else 'delivery' end traffic_source_type
--,case when lower(traffic_source_name) like '%inhouse%' then 'inhouse' when traffic_source_medium = "organic" then "nature"  else 'delivery' end traffic_source_type
,traffic_source_name
,traffic_source_medium
,traffic_source_source
,min_by(device_category,first_event_time) device_category
,min_by(mobile_brand_name,first_event_time) mobile_brand_name
,min_by(mobile_model_name,first_event_time) mobile_model_name
,min_by(mobile_marketing_name,first_event_time) mobile_marketing_name
,min_by(operating_system_version,first_event_time) operating_system_version
,min_by(language,first_event_time) language
,package_name
,c.app_version
,max_by(a.app_version,first_event_time) last_app_version
,if(c.app_version like '%,%',1,0) is_update
from gzdw2024.scanner_02_event.dwd_user_event_di a 
left join (
select date_add(event_date,interval -1 day) event_day_1
,user_pseudo_id
from gzdw2024.scanner_02_event.dwd_user_event_di
where event_date>=date_add(run_date,interval -history_day day)
and event_name  in ('user_engagement','screen_view','app_exception','first_open')
group by date_add(event_date,interval -1 day),user_pseudo_id
) b 
on a.event_date=b.event_day_1 and a.user_pseudo_id=b.user_pseudo_id
left join (
select event_date,user_pseudo_id,array_to_string(array_agg(app_version order by first_active_time),',') app_version
from 
(select event_date,user_pseudo_id,app_version,min(first_event_time) first_active_time
from gzdw2024.scanner_02_event.dwd_user_event_di
where event_date>=date_add(run_date,interval -history_day day)
group by event_date,user_pseudo_id,app_version) a
group by event_date,user_pseudo_id
) c 
on a.event_date=c.event_date and a.user_pseudo_id=c.user_pseudo_id
where a.event_date>=date_add(run_date,interval -history_day day)
and  a.event_name  in ('user_engagement','screen_view','app_exception','first_open')
group by 
a.event_date
,package_name
,a.user_pseudo_id
,traffic_source_type
,traffic_source_name
,traffic_source_medium
,traffic_source_source
--,device_category
--,mobile_brand_name
--,mobile_model_name
--,mobile_marketing_name
--,operating_system_version
--,language
,is_vip
,c.app_version
,is_update
;

-----3.dws_event_profile_di
delete gzdw2024.scanner_02_event.dws_event_profile_di
where event_date>=date_add(run_date,interval -history_day day)
;

insert gzdw2024.scanner_02_event.dws_event_profile_di
with top_version as (
select event_date,package_name,app_version from 
(select event_date,app_version,package_name,row_number() over(partition by event_date,package_name order by dau desc) rn
from 
(select event_date,app_version,package_name,count(distinct user_pseudo_id) dau from 
(select event_date,split(app_version,',') app_version,user_pseudo_id,package_name
from gzdw2024.scanner_01_basic.dwd_user_active_di
where event_date>=date_add(run_date,interval -history_day day)) a,unnest(app_version) app_version
group by event_date,app_version,package_name) a
) a
where rn<=5
)

select 
a.event_date
,event_name   
,sum(event_num) event_num
,count(distinct a.user_pseudo_id) user_num
,count(distinct case when is_retain=1 then b.user_pseudo_id end) user_retain_num
,case when c.app_version is null then 'other' else b.last_app_version end last_app_version  --取最后登录的app_version
,a.package_name
,b.country
,b.traffic_source_type
,b.traffic_source_name
,b.is_new 
,b.is_vip 
,b.is_update
from  gzdw2024.scanner_02_event.dwd_user_event_di a 
left join gzdw2024.scanner_01_basic.dwd_user_active_di b 
on a.event_date=b.event_date and a.user_pseudo_id=b.user_pseudo_id
left join top_version c
on b.event_date=c.event_date and b.last_app_version=c.app_version and a.package_name=b.package_name
where a.event_date>=date_add(run_date,interval -history_day day)
group by 
a.event_date
,event_name
,last_app_version
,a.package_name
,b.country
,b.traffic_source_type
,b.traffic_source_name
,b.is_new 
,b.is_vip 
,b.is_update
;

-----4.dwd_user_new_di
delete gzdw2024.scanner_01_basic.dwd_user_new_di
where event_date>=date_add(run_date,interval -hitory_retain_day day)
;

insert gzdw2024.scanner_01_basic.dwd_user_new_di
select a.event_date

,a.user_pseudo_id
,a.first_active_time 
,a.is_remove
,a.is_purchase

,a.is_retain is_retain_2
,count(distinct case when date_diff(b.event_date,a.event_date,day)=2 then b.user_pseudo_id end) is_retain_3
,count(distinct case when date_diff(b.event_date,a.event_date,day)=3 then b.user_pseudo_id end) is_retain_4
,count(distinct case when date_diff(b.event_date,a.event_date,day)=4 then b.user_pseudo_id end) is_retain_5
,count(distinct case when date_diff(b.event_date,a.event_date,day)=5 then b.user_pseudo_id end) is_retain_6
,count(distinct case when date_diff(b.event_date,a.event_date,day)=6 then b.user_pseudo_id end) is_retain_7
,count(distinct case when date_diff(b.event_date,a.event_date,day)=13 then b.user_pseudo_id end) is_retain_14
,count(distinct case when date_diff(b.event_date,a.event_date,day)=20 then b.user_pseudo_id end) is_retain_21
,count(distinct case when date_diff(b.event_date,a.event_date,day)=29 then b.user_pseudo_id end) is_retain_30
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 1 then b.user_pseudo_id end) is_rolling_retain_2
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 2 then b.user_pseudo_id end) is_rolling_retain_3
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 3 then b.user_pseudo_id end) is_rolling_retain_4
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 4 then b.user_pseudo_id end) is_rolling_retain_5
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 5 then b.user_pseudo_id end) is_rolling_retain_6
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 6 then b.user_pseudo_id end) is_rolling_retain_7
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 13 then b.user_pseudo_id end) is_rolling_retain_14
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 20 then b.user_pseudo_id end) is_rolling_retain_21
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 29 then b.user_pseudo_id end) is_rolling_retain_30

,a.is_vip
,a.country
,a.city
,a.traffic_source_type
,a.traffic_source_name
,a.traffic_source_medium
,a.traffic_source_source
,a.device_category
,a.mobile_brand_name
,a.mobile_model_name
,a.mobile_marketing_name
,a.operating_system_version
,a.language
,a.package_name
,a.app_version
,a.last_app_version
from  gzdw2024.scanner_01_basic.dwd_user_active_di a 
left join  gzdw2024.scanner_01_basic.dwd_user_active_di b on a.user_pseudo_id=b.user_pseudo_id 
where a.is_new=1 
and a.event_date>=date_add(run_date,interval -hitory_retain_day day)
and b.event_date>=date_add(run_date,interval -hitory_retain_day day)
and date_diff(b.event_date,a.event_date,day) between 0 and 29
group by a.event_date
,a.package_name
,a.app_version
,a.last_app_version
,a.user_pseudo_id
,a.first_active_time 
,a.country
,a.city
,a.traffic_source_type
,a.traffic_source_name
,a.traffic_source_medium
,a.traffic_source_source
,a.device_category
,a.mobile_brand_name
,a.mobile_model_name
,a.mobile_marketing_name
,a.operating_system_version
,a.language
,a.is_vip
,a.is_remove
,a.is_purchase

,a.is_retain
;


----5.dws_basic_indicator_profile_di
delete gzdw2024.scanner_01_basic.dws_basic_indicator_profile_di
where event_date>=date_add(run_date,interval -hitory_retain_day day)
;

insert gzdw2024.scanner_01_basic.dws_basic_indicator_profile_di

with top_version as (
select event_date,package_name,app_version from 
(select event_date,app_version,package_name,row_number() over(partition by event_date,package_name order by dau desc) rn
from 
(select event_date,app_version,package_name,count(distinct user_pseudo_id) dau from 
(select event_date,split(app_version,',') app_version,user_pseudo_id,package_name
from gzdw2024.scanner_01_basic.dwd_user_active_di
where event_date>=date_add(run_date,interval -hitory_retain_day day)) a,unnest(app_version) app_version
group by event_date,app_version,package_name) a
) a
where rn<=5
)

select a.event_date
,count(distinct a.user_pseudo_id) dau
,count(distinct case when a.is_remove=1 then a.user_pseudo_id end) user_num_remove
,count(distinct case when a.is_purchase=1 then a.user_pseudo_id end) user_num_purchase
,count(distinct case when date_diff(b.event_date,a.event_date,day)=1 then b.user_pseudo_id end) retain_2
,count(distinct case when date_diff(b.event_date,a.event_date,day)=2 then b.user_pseudo_id end) retain_3
,count(distinct case when date_diff(b.event_date,a.event_date,day)=3 then b.user_pseudo_id end) retain_4
,count(distinct case when date_diff(b.event_date,a.event_date,day)=4 then b.user_pseudo_id end) retain_5
,count(distinct case when date_diff(b.event_date,a.event_date,day)=5 then b.user_pseudo_id end) retain_6
,count(distinct case when date_diff(b.event_date,a.event_date,day)=6 then b.user_pseudo_id end) retain_7
,count(distinct case when date_diff(b.event_date,a.event_date,day)=13 then b.user_pseudo_id end) retain_14
,count(distinct case when date_diff(b.event_date,a.event_date,day)=20 then b.user_pseudo_id end) retain_21
,count(distinct case when date_diff(b.event_date,a.event_date,day)=29 then b.user_pseudo_id end) retain_30
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 1  then b.user_pseudo_id end) rolling_retain_2
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 2  then b.user_pseudo_id end) rolling_retain_3
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 3  then b.user_pseudo_id end) rolling_retain_4
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 4  then b.user_pseudo_id end) rolling_retain_5
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 5  then b.user_pseudo_id end) rolling_retain_6
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 6  then b.user_pseudo_id end) rolling_retain_7
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 13 then b.user_pseudo_id end) rolling_retain_14
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 20 then b.user_pseudo_id end) rolling_retain_21
,count(distinct case when date_diff(b.event_date,a.event_date,day) between 1 and 29 then b.user_pseudo_id end) rolling_retain_30

,a.is_new
,a.is_vip
,a.country
,a.traffic_source_type
,a.traffic_source_name
,a.package_name
,case when c.app_version is null then 'other' else a.last_app_version end last_app_version
from  gzdw2024.scanner_01_basic.dwd_user_active_di a 
left join  gzdw2024.scanner_01_basic.dwd_user_active_di b on a.user_pseudo_id=b.user_pseudo_id 
left join top_version c on a.event_date=c.event_date and a.package_name=c.package_name and  a.last_app_version=c.app_version
where a.event_date>=date_add(run_date,interval -hitory_retain_day day)
and b.event_date>=date_add(run_date,interval -hitory_retain_day day)
group by a.event_date
,a.is_retain
,a.is_new
,a.is_vip
,a.country
,a.traffic_source_type
,a.traffic_source_name
,a.package_name
,last_app_version
;



-----6.dws_event_param_profile_di 
delete gzdw2024.scanner_02_event.dws_event_param_profile_di
where event_date>=date_add(run_date,interval -history_day day)
;

insert gzdw2024.scanner_02_event.dws_event_param_profile_di
select 
event_date
,package_name
,country
,traffic_source_type
,traffic_source_name
,is_new
,is_vip
,event_name
,event_params_key
,event_params_value
,count(*) event_num
,count(distinct user_pseudo_id) user_num
   ,traffic_source_medium
	,app_version
from 
(SELECT 
PARSE_DATE('%Y%m%d',a.event_date) event_date
,app_info.id AS package_name
,a.user_pseudo_id
,geo.country country
,traffic_source.name as traffic_source_name
,case when lower(traffic_source.name) like '%inhouse%' then 'inhouse' when (traffic_source.name in('(direct)')  or traffic_source.name is null) then 'nature' else 'delivery' end traffic_source_type
,b.is_new
,b.is_vip
	,case when app_info.version='' or app_info.version is null then 'undefined' else app_info.version end as app_version
,event_name
,event_params.key as event_params_key
     ,case when traffic_source.medium='' or traffic_source.medium is null then 'undefined' else traffic_source.medium end as traffic_source_medium
,coalesce(cast(event_params.value.int_value as string),cast(event_params.value.string_value as string),cast(event_params.value.float_value as string),cast(event_params.value.double_value as string))  as event_params_value
FROM `scanner-master-android.analytics_196427335.events_*` a 
cross join unnest(event_params) event_params
inner join  gzdw2024.scanner_01_basic.dwd_user_active_di b on a.user_pseudo_id=b.user_pseudo_id and PARSE_DATE('%Y%m%d',a.event_date) =b.event_date
WHERE _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','') 
and a.user_pseudo_id is not null
and b.event_date>=date_add(run_date,interval -history_day day)
AND event_name NOT IN ('screen_view','user_engagement','session_start','firebase_campaign')
and event_params.key not in ('engagement_time_msec','ga_session_number','engaged_session_event','ga_session_id'
'firebase_screen','firebase_screen_class','firebase_screen_id','firebase_conversion','ga_session_id',
'firebase_previous_class','firebase_previous_id','firebase_error')
) a
group by 
event_date
,package_name
,country
,traffic_source_type
,traffic_source_name
,is_new
,is_vip
,event_name
,event_params_key
	,app_version
,event_params_value
   ,traffic_source_medium
;




-----7.dwd_user_event_param_di 
delete gzdw2024.scanner_02_event.dwd_user_event_param_di
  
where
  event_date>=date_add(run_date,interval -history_day day) ;


INSERT
  gzdw2024.scanner_02_event.dwd_user_event_param_di
SELECT
  event_date,
  user_pseudo_id,
  event_name,
  event_params_key,
  event_params_value,
  COUNT(*) event_num,
  package_name,
  country,
  traffic_source_type,
  traffic_source_name,
  app_version,
  traffic_source_medium
FROM (
  SELECT
    PARSE_DATE('%Y%m%d',event_date) event_date,
    app_info.id AS package_name,
    user_pseudo_id,
    geo.country country,
    traffic_source.name AS traffic_source_name,
    CASE
      WHEN LOWER(traffic_source.name) LIKE '%inhouse%' THEN 'inhouse'
      WHEN (traffic_source.name IN('(direct)')
      OR traffic_source.name IS NULL) THEN 'nature'
      ELSE 'delivery'
  END
    traffic_source_type ,
    event_name,
    app_info.version app_version,
    event_params.key AS event_params_key,
    COALESCE(CAST(event_params.value.string_value AS string),CAST(event_params.value.int_value AS string),CAST(event_params.value.float_value AS string),CAST(event_params.value.double_value AS string)) AS event_params_value
    ,case when traffic_source.medium='' or traffic_source.medium is null then 'undefined' else traffic_source.medium end as traffic_source_medium

  FROM
    `scanner-master-android.analytics_196427335.events_*`
  CROSS JOIN
    UNNEST(event_params) event_params
  WHERE
    _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
    and user_pseudo_id is not null
    AND event_name NOT IN ('screen_view',
      'user_engagement',
      'session_start',
      'firebase_campaign')
    AND event_params.key NOT IN ('engagement_time_msec',
      'ga_session_id',
      'ga_session_number',
      'engaged_session_event',
      'firebase_screen',
      'firebase_screen_class',
      'firebase_screen_id',
      'firebase_event_origin',
      'firebase_conversion',
      'firebase_previous_class',
      'firebase_previous_id',
      'firebase_error') ) a
GROUP BY
  event_date,
  user_pseudo_id,
  package_name,
  country,
  traffic_source_type,
  traffic_source_name,
  event_name,
  event_params_key,
  event_params_value,
  traffic_source_medium,
  app_version;



-----8.dwd_user_userid_di 
delete gzdw2024.scanner_01_basic.dwd_user_userid_di
where event_date>=date_add(run_date,interval -history_day day)
;

insert gzdw2024.scanner_01_basic.dwd_user_userid_di

SELECT
   PARSE_DATE('%Y%m%d',event_date) event_date
   ,case when app_info.id='' or app_info.id is null then 'undefined' else app_info.id end AS package_name
   ,user_pseudo_id
   ,user_id
	   ,(SELECT value.string_value 
   FROM UNNEST(user_properties) WHERE key='is_vip') as is_vip
   ,(SELECT value.string_value 
   FROM UNNEST(user_properties) WHERE key='vip_type') as vip_type
    ,(SELECT value.string_value 
   FROM UNNEST(user_properties) WHERE key='custom_uid') as custom_uid
  FROM
    `scanner-master-android.analytics_196427335.events_*`
  WHERE
    _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
group by user_pseudo_id,event_date,user_id,package_name
,is_vip,vip_type,custom_uid;


-------9.日活与留存统计
delete gzdw2024.scanner_01_basic.dws_user_active_report
where stats_date>=date_add(run_date,interval -history_day day)
;

insert gzdw2024.scanner_01_basic.dws_user_active_report

SELECT
   stats_date
   ,app_name
   ,package_name
   ,country_code
   ,active_uv
   ,new_uv
   ,case when stats_date=date_add(run_date,interval -1 day) then null else new_retain_uv end as new_retain_uv
   ,case when stats_date=date_add(run_date,interval -1 day) then null else ratio end as ratio
   ,traffic_source_type
FROM 
   (
   SELECT
   event_date as stats_date

   ,package_name
  ,app_name
   ,case when country_code='SYRIA' then 'SY'
      when country_code='TÜRKIYE' then 'TR'
      when country_code='MYANMAR (BURMA)' then 'MM'
      when country_code='PALESTINE' then 'PS'
      when country_code='AUSTRALIA' then 'AU'
      when country_code='CONGO - KINSHASA' then 'CD'
      else country_code end as country_code
   
   ,count(distinct user_pseudo_id) as active_uv 
   ,count(distinct case when is_new=1 then user_pseudo_id else null end) as new_uv 
   ,count(distinct case when is_new=1 and is_retain=1  then user_pseudo_id else null end) as new_retain_uv 
   ,safe_divide(count(distinct case when is_new=1 and is_retain=1  then user_pseudo_id else null end),count(distinct case when is_new=1 then user_pseudo_id else null end)) as ratio
   ,traffic_source_type
FROM 
   (
   SELECT 
      event_date
      ,array[ifnull(country_code,country),'TOTAL'] as country_code
      ,user_pseudo_id 
      ,a.package_name
    ,app_name
      ,is_new
      ,is_retain
      ,array[case when traffic_source_type in ('nature','delivery','inhouse') then traffic_source_type else 'undefined' end ,'TOTAL'] as traffic_source_type
    FROM  
      (
      SELECT 
         package_name
         ,event_date
         ,upper(country) as country
         ,user_pseudo_id
         ,max(is_new) as is_new
         ,max(is_retain) as is_retain
         ,max(traffic_source_type) as traffic_source_type
      FROM `gzdw2024.scanner_01_basic.dwd_user_active_di` 
      WHERE event_date >=date_add(run_date,interval -history_day day)
      group by package_name,country,user_pseudo_id,event_date
      )a 
      left join
      (
      SELECT 
         upper(country_name_2) as country_name_2
         ,country_name_3 as country_code
      FROM `hzdw2024.hz_dim.dim_country`
      )b 
      on a.country=b.country_name_2
    left join
    (
    SELECT package_name,app_name FROM `gzdw2024.gz_dim.app_info`

      )c 
    on a.package_name=c.package_name
   )c
   ,UNNEST(country_code) as country_code
   ,UNNEST(traffic_source_type) as traffic_source_type
   group by country_code,event_date,package_name,traffic_source_type,app_name

   )d ;



-------10.日活新增与留存scanner_01_basic
delete gzdw2024.gz_bi.dws_app_daily_reports
where stats_date>=date_add(run_date,interval -history_day day)
and package_name in 
               (
               SELECT
               package_name
               FROM  gzdw2024.scanner_01_basic.dws_user_active_report
               WHERE stats_date>=date_add(run_date,interval -history_day day)
               group by package_name
               )
;


insert gzdw2024.gz_bi.dws_app_daily_reports
   SELECT 
         stats_date
         ,app_name
         ,package_name
         ,active_uv
         ,new_uv
         ,ratio
         ,country_code
         ,new_retain_uv
   FROM `gzdw2024.scanner_01_basic.dws_user_active_report`
    WHERE stats_date >= date_add(run_date,interval -history_day day) 
    --and country_code='TOTAL'
    and traffic_source_type='TOTAL';


	-------11.投放安装与事件dws_delivery_report
delete gzdw2024.scanner_02_event.dws_delivery_report
where
  stats_date>=date_add(run_date,interval - history_day day) ;


INSERT
  gzdw2024.scanner_02_event.dws_delivery_report
	SELECT
		stats_date
		,package_name
		,country
		,traffic_source_name
		,sum(install_ga) as install_ga
		,sum(trial_counts) as trial_counts
	FROM	
		(
		SELECT
			stats_date
			,package_name
			,array['TOTAL',country] as country
			,array['TOTAL',traffic_source_name] as traffic_source_name
			,install_ga
			,trial_counts
		FROM
			(
			SELECT
				stats_date
				,package_name
				,country
				,traffic_source_name
				,sum(install_ga) as install_ga
				,sum(trial_counts) as trial_counts
			FROM
				(
				SELECT  
					event_date as stats_date
					,country
					,package_name
					,traffic_source_name
					,count(distinct user_pseudo_id) as install_ga
					,0 as trial_counts
				FROM `gzdw2024.scanner_01_basic.dwd_user_active_di` 
				WHERE event_date>=date_add(run_date,interval -history_day day)
				and traffic_source_medium = "cpc"
				and is_new=1
				and traffic_source_name like 'GA%'
				--and country='Vietnam'
				--AND package_name = "vidma.video.editor.videomaker"
				group by country,traffic_source_name,event_date,package_name
				union all 
		        SELECT
		        	event_date stats_date
		           ,country
		           ,package_name
		           ,traffic_source_name
		           ,0 as install_ga
		          ,sum(event_num) AS trial_counts
		        FROM
		          `gzdw2024.scanner_02_event.dws_event_param_profile_di`
		        WHERE
		          event_name = "in_app_purchase"
		          AND event_params_key = "price"
		          AND event_params_value = '0'
		          AND traffic_source_medium = "cpc"
		          and  event_date >=date_add(run_date,interval -history_day day)
		          and traffic_source_name like 'GA%'
				 -- AND package_name = "vidma.video.editor.videomaker"
			     group by country,traffic_source_name,stats_date,package_name
			     )a 
					 group by 	stats_date
				,country
				,traffic_source_name,package_name
			)b 
		)c 
		,UNNEST(country) as country
		,UNNEST(traffic_source_name) as traffic_source_name
		group by stats_date,country,traffic_source_name,package_name;

delete `gzdw2024.scanner_02_event.dwd_user_event_time_di`
where event_date>=date_add(run_date,interval -history_day day)
and  event_date<=date_add(run_date,interval -history_end_day day);

	insert `gzdw2024.scanner_02_event.dwd_user_event_time_di` 
	SELECT 
		event_name
		,PARSE_DATE('%Y%m%d',event_date) event_date
		,user_pseudo_id
		,event_timestamp
		,FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',TIMESTAMP_MICROS(event_timestamp)) as date_time
	   ,case when geo.country='' or geo.country is null then 'undefined' else geo.country end as country
		,case when geo.city='' or geo.city is null then 'undefined' else geo.city end as city
		,case when traffic_source.name='' or traffic_source.name is null then 'undefined' else traffic_source.name end as traffic_source_name
		,case when lower(traffic_source.name) like '%inhouse%' then 'inhouse' 
		      when traffic_source.source like '%link%' then 'inhouse'
		      when (traffic_source.name in('(direct)')  or traffic_source.name is null) then 'nature' 
		      else 'delivery' end traffic_source_type
		,case when traffic_source.medium='' or traffic_source.medium is null then 'undefined' else traffic_source.medium end as traffic_source_medium
		,case when traffic_source.source='' or traffic_source.source is null then 'undefined' else traffic_source.source end as traffic_source_source
		,case when device.category='' or device.category is null then 'undefined' else device.category end as device_category
		,case when device.mobile_brand_name='' or device.mobile_brand_name is null then 'undefined' else device.mobile_brand_name  end as mobile_brand_name
		,case when device.mobile_model_name='' or device.mobile_model_name is null then 'undefined' else device.mobile_model_name end as mobile_model_name
		,case when device.mobile_marketing_name='' or device.mobile_marketing_name is null then 'undefined' else device.mobile_marketing_name end as mobile_marketing_name
		,case when device.operating_system_version='' or device.operating_system_version is null then 'undefined' else device.operating_system_version end as operating_system_version
		,case when device.language='' or device.language is null then 'undefined' else device.language end as language
		,case when app_info.id='' or app_info.id is null then 'undefined' else app_info.id end AS package_name
		,case when app_info.version='' or app_info.version is null then 'undefined' else app_info.version end as app_version
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='error_code') error_code 
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='reason') reason
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='star') star
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='mode') mode
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='provider') provider
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='net_description') net_description
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='uuid') uuid
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(user_properties) WHERE key='is_vip') is_vip
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(user_properties) WHERE key='vip_type') vip_type
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='time') time_use
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(user_properties) WHERE key='exp_cat') exp_cat
		,date(format_timestamp("%Y-%m-%d %H:%M:%S", timestamp_seconds( cast ((user_first_touch_timestamp/1000000) as int64)),'Pacific/Auckland')) as active_date
	FROM `scanner-master-android.analytics_196427335.events_*`
	WHERE 1=1
	and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
	and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','')
	AND event_name NOT IN ('screen_view','user_engagement','session_start','firebase_campaign');


    

end;
