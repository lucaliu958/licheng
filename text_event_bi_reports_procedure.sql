CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.text_event_bi_reports_procedure`(run_date DATE, history_day INT64, history_end_day INT64)
begin

delete `gzdw2024.text_03_bi.dws_event_report`
where event_date>=date_add(run_date,interval -history_day day)
and  event_date<=date_add(run_date,interval -history_end_day day);

insert `gzdw2024.text_03_bi.dws_event_report`


	SELECT  
		event_date
		,event_name
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
		,last_app_version
		,is_new
		,sum(event_num) as event_num
		,sum(user_num) as user_num
		,package_name
	FROM
		(
		SELECT  
			event_date
			,event_name
			,ifnull(country_name_3,a.country) as country_code
			,last_app_version
			,case when is_new=1 then 'new' else 'old' end as is_new
			,event_num
			,user_num
			,package_name
		FROM `gzdw2024.scanner_02_event.dws_event_profile_di`  a 
		left join `hzdw2024.hz_dim.dim_country` b
		on upper(a.country)=upper(b.country_name_2)
		WHERE event_date >= date_add(run_date,interval -history_day day)
			    and event_date <= date_add(run_date,interval -history_end_day day)
		and package_name in ('second.phone.number.text.free.call.app','com.talknow.free.text.me.now.second.phone.number.burner.app')
		and event_name in ('first_open','sn_0_app_launch','sn_1_signin_show','sn_1_signin_succ','sn_1_signin_fail'
							,'sn_vip_all_show','sn_vip_all_click','sn_vip_all_succ','sn_vip_all_fail'
							,'sn_vip_guide_show','sn_vip_guide_click','sn_vip_guide_succ','sn_5_call_dial_tap'
							,'sn_5_call_dial_no_request','sn_5_call_dial_request','sn_5_call_dial_outbound_succ'
							,'sn_5_call_dial_outbound_fail','sn_5_call_dial_outbound_ring','sn_5_call_dial_outbound_ringtone'
							,'sn_5_call_dial_connect_fail','sn_5_call_dial_inbound_succ','sn_5_call_dial_inbound_fail'
							,'sn_5_call_dial_end','sn_7_text_send_tap','sn_7_text_send_succ','sn_7_text_send_fail'
							,'sn_7_text_send_click_begin','sn_7_text_send_click_succ','sn_7_text_send_click_fail'
							,'sn_dev_call_dial_outbound_callkit','sn_dev_call_dial_outbound_connect_sdk','sn_5_serve_voice_answer_begin'
							,'sn_5_serve_voice_answer_fail','sn_5_serve_voice_answer_succ','sn_dev_call_dial_outbound_sdk_call'
							,'sn_dev_call_dial_outbound_insert_item','sn_dev_call_dial_outbound_did_answer'
							,'sn_5_serve_voice_answer_begin','sn_5_serve_voice_answer_succ','sn_5_serve_voice_answer_fail'
							,'sn_dev_call_dial_inbound_insert_item','sn_dev_call_dial_inbound_callkit','sn_dev_call_dial_inbound_connect_sdk'
							,'sn_dev_call_dial_inbound_click_answer','sn_dev_call_dial_inbound_will_answer','sn_dev_call_dial_inbound_did_answer'
							,'sn_dev_call_dial_click_hangup','sn_5_serve_voice_status','sn_5_call_quality_action'
							,'sn_11_serve_call_paid'
							,'sn_11_serve_message_status'
							,'sn_11_serve_spam_check_credit'
							,'sn_11_serve_caller_name_credit'
							,'sn_11_serve_assistant_credit'
							,'sn_11_serve_sms_paid'
							,'sn_11_serve_mms_paid'
							,'sn_11_serve_sms_back_credit'
							,'sn_11_serve_sms_activate_buy_credit'
							,'sn_11_serve_sms_activate_back_credit'
							,'sn_11_serve_call_paid')
		)a 
		group by 	event_date,country_code,last_app_version,event_name,is_new,package_name;


----失败原因分布
--create table   `gzdw2024.text_03_bi.dws_event_params_report`
--	PARTITION BY event_date as 
delete `gzdw2024.text_03_bi.dws_event_params_report`
where event_date>=date_add(run_date,interval -history_day day)
and  event_date<=date_add(run_date,interval -history_end_day day);

insert `gzdw2024.text_03_bi.dws_event_params_report`
	SELECT  
		event_date
		,event_name
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
		--,last_app_version
		,event_params_key
		,event_params_value
		,sum(event_num) as event_num
	,package_name
	FROM
		(
		SELECT  
			event_date
			,event_name
			,ifnull(country_name_3,a.country) as country_code
			,event_params_key
			,event_params_value
			,event_num
      	,package_name
			--,sum(event_num) over()
		FROM `gzdw2024.scanner_02_event.dws_event_param_profile_di`  a 
		left join `hzdw2024.hz_dim.dim_country` b
		on upper(a.country)=upper(b.country_name_2)
		WHERE event_date >= date_add(run_date,interval -history_day day)
			    and event_date <= date_add(run_date,interval -history_end_day day)
		and package_name in ('second.phone.number.text.free.call.app','com.talknow.free.text.me.now.second.phone.number.burner.app')
		and event_name in ('first_open','sn_0_app_launch','sn_1_signin_show','sn_1_signin_succ','sn_1_signin_fail'
							,'sn_vip_all_show','sn_vip_all_click','sn_vip_all_succ','sn_vip_all_fail'
							,'sn_vip_guide_show','sn_vip_guide_click','sn_vip_guide_succ','sn_5_call_dial_tap'
							,'sn_5_call_dial_no_request','sn_5_call_dial_request','sn_5_call_dial_outbound_succ'
							,'sn_5_call_dial_outbound_fail','sn_5_call_dial_outbound_ring','sn_5_call_dial_outbound_ringtone'
							,'sn_5_call_dial_connect_fail','sn_5_call_dial_inbound_succ','sn_5_call_dial_inbound_fail'
							,'sn_5_call_dial_end','sn_7_text_send_tap','sn_7_text_send_succ','sn_7_text_send_fail'
							,'sn_7_text_send_click_begin','sn_7_text_send_click_succ','sn_7_text_send_click_fail'
							,'sn_dev_call_dial_outbound_callkit','sn_dev_call_dial_outbound_connect_sdk','sn_5_serve_voice_answer_begin'
							,'sn_5_serve_voice_answer_fail','sn_5_serve_voice_answer_succ','sn_dev_call_dial_outbound_sdk_call'
							,'sn_dev_call_dial_outbound_insert_item','sn_dev_call_dial_outbound_did_answer'
							,'sn_5_serve_voice_answer_begin','sn_5_serve_voice_answer_succ','sn_5_serve_voice_answer_fail'
							,'sn_dev_call_dial_inbound_insert_item','sn_dev_call_dial_inbound_callkit','sn_dev_call_dial_inbound_connect_sdk'
							,'sn_dev_call_dial_inbound_click_answer','sn_dev_call_dial_inbound_will_answer','sn_dev_call_dial_inbound_did_answer'
							,'sn_dev_call_dial_click_hangup','sn_5_serve_voice_status','sn_5_call_quality_action'
							,'sn_11_serve_call_paid'
							,'sn_11_serve_message_status'
							,'sn_11_serve_spam_check_credit'
							,'sn_11_serve_caller_name_credit'
							,'sn_11_serve_assistant_credit'
							,'sn_11_serve_sms_paid'
							,'sn_11_serve_mms_paid'
							,'sn_11_serve_sms_back_credit'
							,'sn_11_serve_sms_activate_buy_credit'
							,'sn_11_serve_sms_activate_back_credit'
							,'sn_11_serve_call_paid')
		and event_params_key in('error_code','reason','star','mode','provider','ga_session_id','net_description','status','credit')
		)a 
		group by 	event_date,event_name,event_params_value,country_code,event_params_key,package_name;

------通话时长
--drop table  `gzdw2024.text_02_event.dwd_user_event_time_detail`;
--create table   `gzdw2024.text_02_event.dwd_user_event_time_detail`
--	PARTITION BY event_date as 

delete `gzdw2024.text_02_event.dwd_user_event_time_detail`
where event_date>=date_add(run_date,interval -history_day day)
and  event_date<=date_add(run_date,interval -history_end_day day);


	insert `gzdw2024.text_02_event.dwd_user_event_time_detail`
	SELECT 
		event_name
		,PARSE_DATE('%Y%m%d',event_date) event_date
    	,case when app_info.id='' or app_info.id is null then 'undefined' else app_info.id end AS package_name
		,user_pseudo_id
		,user_id
		,event_timestamp
	   ,(SELECT value.string_value 
   			FROM UNNEST(user_properties) WHERE key='is_vip') as is_vip
		,(SELECT value.string_value 
   			FROM UNNEST(user_properties) WHERE key='vip_type') as vip_type
    	,(SELECT value.string_value 
   			FROM UNNEST(user_properties) WHERE key='custom_uid') as custom_uid
		,(SELECT COALESCE(cast(value.int_value as string),cast(value.string_value as string),cast(value.float_value as string),cast(value.double_value as string)) FROM UNNEST(event_params) WHERE key='time') duration_time
		,app_info.version 

	FROM   `scanner-master-android.analytics_196427335.events_*`
  	WHERE 1=1
		and app_info.id in ('second.phone.number.text.free.call.app','com.talknow.free.text.me.now.second.phone.number.burner.app')
  	and event_name in ('sn_5_call_dial_end')
	and _TABLE_SUFFIX >=replace(cast(date_add(run_date,interval -history_day day) as string),'-','')
	and _TABLE_SUFFIX <=replace(cast(date_add(run_date,interval -history_end_day day) as string),'-','');



	--create table   `gzdw2024.text_03_bi.dws_call_time_report`
	--PARTITION BY event_date as 

	delete `gzdw2024.text_03_bi.dws_call_time_report`
where event_date>=date_add(run_date,interval -history_day day)
and  event_date<=date_add(run_date,interval -history_end_day day);


	insert `gzdw2024.text_03_bi.dws_call_time_report`
SELECT
	event_date
	,package_name
	,country_code
	,app_version
	,max(percentile_10) as percentile_10
	,max(percentile_25) as percentile_25
	,max(percentile_50) as percentile_50
	,max(percentile_75) as percentile_75
	,max(percentile_90) as percentile_90
	,avg(duration_time) as avg_duration_time
	,count(1) as dial_pv
		,count(case when duration_time<=10 then 1 else null end)  as dial_10_pv
FROM
	(
SELECT
	event_date
	,package_name
	,country_code
	,app_version
	,duration_time
	,user_pseudo_id
	,PERCENTILE_CONT(duration_time,0.1) over (partition by event_date,country_code,package_name,app_version) AS percentile_10
	,PERCENTILE_CONT(duration_time,0.25) over (partition by event_date,country_code,package_name,app_version) AS percentile_25
	,PERCENTILE_CONT(duration_time,0.50) over (partition by event_date,country_code,package_name,app_version) AS percentile_50
	,PERCENTILE_CONT(duration_time,0.75) over (partition by event_date,country_code,package_name,app_version) AS percentile_75
	,PERCENTILE_CONT(duration_time,0.90) over (partition by event_date,country_code,package_name,app_version) AS percentile_90
FROM
	(
		SELECT
			a.event_date
			,a.package_name
			,a.user_pseudo_id
			,a.duration_time
			,array['TOTAL',case when country_code='SYRIA' then 'SY'
			when country_code='TÜRKIYE' then 'TR'
			when country_code='MYANMAR (BURMA)' then 'MM'
			when country_code='PALESTINE' then 'PS'
			when country_code='AUSTRALIA' then 'AU'
			when country_code='CONGO - KINSHASA' then 'CD'
			when country_code='BOSNIA & HERZEGOVINA' then 'BA'
			when country_code='NORTH MACEDONIA' then 'MK'
			when country_code='KOSOVO' then 'XK'
			else country_code end] as country_code
			,array['TOTAL',app_version] as app_version
		FROM
			(
			select 
				safe_cast(duration_time as float64) as duration_time 
				,user_pseudo_id
				,event_date
				,package_name
			from `gzdw2024.text_02_event.dwd_user_event_time_detail`  
			where 1=1
			and  event_date >= date_add(run_date,interval -history_day day)
			    and event_date <= date_add(run_date,interval -history_end_day day)
			and event_name='sn_5_call_dial_end'
		and package_name in ('second.phone.number.text.free.call.app','com.talknow.free.text.me.now.second.phone.number.burner.app')
			)a 
			left join
			(
				SELECT  
					event_date
					,user_pseudo_id
					,max(ifnull(country_name_3,a.country)) as country_code
					,max(app_version) as app_version
					,max(package_name) as package_name
				FROM `gzdw2024.scanner_01_basic.dwd_user_active_di`  a 
				left join `hzdw2024.hz_dim.dim_country` b
				on upper(a.country)=upper(b.country_name_2)
				WHERE  event_date >= date_add(run_date,interval -history_day day)
			    and event_date <= date_add(run_date,interval -history_end_day day)
		and package_name in ('second.phone.number.text.free.call.app','com.talknow.free.text.me.now.second.phone.number.burner.app')
				group by event_date,user_pseudo_id
				)b  
			on a.user_pseudo_id=b.user_pseudo_id
			and a.event_date=b.event_date
			and a.package_name=b.package_name
		)d 
	,UNNEST(country_code) as country_code
	,UNNEST(app_version) as app_version
	where duration_time<3600*4
	)e 
	group by event_date,country_code,app_version,package_name
	order by event_date desc,dial_pv desc;


--------分订阅项收入情况

	delete `gzdw2024.text_03_bi.dws_subcribe_detail_report`
where stats_date>=date_add(run_date,interval -history_day day)
and  stats_date<=date_add(run_date,interval -history_end_day day);
insert  `gzdw2024.text_03_bi.dws_subcribe_detail_report`
	SELECT
		stats_date
		,a.package_name
		,c.app_name
		,upper(country_code) as country_code
		,case when period is null or length(period)<2 then 'other' else period end as period
		--,case when subscription is null or length(subscription)<2 then 'other' else subscription end as subscription
		,title
		,sum(units) as order_num
		,sum(case when revenue_usd>0 then revenue_usd else 0 end ) as charged_money
		,sum(case when revenue_usd<0 then abs(revenue_usd) else 0 end ) as refund_money
		,sum(revenue_usd) as vip_revenue
	FROM
		(
		SELECT 
		package_name 
		,revenue_usd
    	,stats_date
		,ARRAY[country_code,'TOTAL'] as country_code
		,period
		,subscription
		,units
		,title
		FROM `gzdw2024.appstoreconnect.p_sales_atlasv` 
		WHERE stats_date >= date_add(run_date,interval -history_day day)
			    and stats_date <= date_add(run_date,interval -(history_end_day+1) day)
			and (revenue_usd>0 or revenue_usd <0 )
		)a 
		join 
		(
			SELECT package_name ,app_name
			FROM `gzdw2024.gz_dim.app_info` 
			where platform='iOS'
			group by package_name,app_name
		)c 
		on a.package_name=c.package_name
		,UNNEST(country_code) as country_code
		GROUP BY package_name,stats_date,country_code,case when period is null or length(period)<2 then 'other' else period end,app_name
		,title;



-----试用套餐转正率
delete `gzdw2024.text_03_bi.dws_subcribe_convert_report`
where stats_date>=date_add(run_date,interval -(history_day +10) day)
and  stats_date<=date_add(run_date,interval -history_end_day day);
insert  `gzdw2024.text_03_bi.dws_subcribe_convert_report`
SELECT
	a.original_start_date as stats_date
	--,a.subscription_name
	,a.pv as try_pv 
	,b.pv as pay_pv
FROM
	(
	SELECT 
		original_start_date as original_start_date
		--,subscription_name
		--,event
		,sum(quantity) as pv 
	 FROM `gzdata.appstoreconnect.p_subscription_event_atlasv` 
	WHERE event_date >= date_add(run_date,interval -(history_day +10) day)
	and event_date <= date_add(run_date,interval -history_end_day day)
	and package_name='second.phone.number.text.free.call.app'
	and event in ('Start Introductory Offer')
	and subscription_offer_type='Free Trial'
	group by original_start_date
	)a 
	left join
	(
	SELECT 
		original_start_date as original_start_date
		--,subscription_name
		--,event
		,sum(quantity) as pv 
	 FROM `gzdata.appstoreconnect.p_subscription_event_atlasv` 
	WHERE event_date >= date_add(run_date,interval -(history_day +10) day)
	and event_date <= date_add(run_date,interval -history_end_day day)
	and package_name='second.phone.number.text.free.call.app'
	and event in ('Paid Subscription from Introductory Offer','Upgrade from Introductory Offer')
	group by original_start_date
	)b 
	on a.original_start_date=b.original_start_date;


-----dws_user_event_bound_reports
--drop table if  exists `gzdw2024.scanner_02_event.dws_user_event_bound_reports` ;
--CREATE TABLE `gzdw2024.scanner_02_event.dws_user_event_bound_reports` 
--	PARTITION by event_date  as 
	delete `gzdw2024.scanner_02_event.dws_user_event_bound_reports`
where event_date>=date_add(run_date,interval -history_day day)
and  event_date<=date_add(run_date,interval -history_end_day day);

	insert `gzdw2024.scanner_02_event.dws_user_event_bound_reports` 
	SELECT
		event_date
		,app_version
		,event_name
		,count(1) as event_num 
		,count(distinct user_pseudo_id) as user_num 
		,package_name
	FROM
		(
		SELECT
			event_date
			,event_name
			,user_pseudo_id
			,app_version
			,rn
			,last_event_name
			,case when event_name='sn_5_call_dial_end_out' and last_event_name ='sn_dev_call_dial_outbound_did_answer' then 1 
			when event_name not in ('sn_5_call_dial_end_out','sn_5_call_dial_end_in','sn_dev_call_dial_inbound_did_answer') then 1 
			when event_name='sn_5_call_dial_end_in' and last_event_name ='sn_dev_call_dial_inbound_did_answer' then 1
			when event_name ='sn_dev_call_dial_inbound_did_answer' and rn=1 then 1  
			else 0 end as tag 
		,package_name
		FROM
			(
			SELECT
				event_date
				--,event_name
				,user_pseudo_id
				,ifnull(country_code,a.country) as country_code
				,app_version
				,event_timestamp
				,uuid
				,mode
				,provider
				,case when event_name='sn_5_call_dial_end' and mode='outbound' then 'sn_5_call_dial_end_out'
				when event_name='sn_5_call_dial_end' and mode='inbound' then 'sn_5_call_dial_end_in'
				else event_name end as event_name
				,row_number() over(partition by user_pseudo_id,event_name,uuid) as rn 
				,lag(event_name)  over(partition by user_pseudo_id order by event_timestamp) as last_event_name 
		,package_name
			FROM  `gzdw2024.scanner_02_event.dwd_user_event_time_di`       a
			left join `gzdw2024.gz_dim.country_info` b
			on upper(a.country)=upper(b.country_name)
			where  event_date >=date_add(run_date,interval -history_day day)
			and event_date <=date_add(run_date,interval -history_end_day day)
			and package_name in ('second.phone.number.text.free.call.app','com.talknow.free.text.me.now.second.phone.number.burner.app')
			and event_name in ('sn_5_call_dial_request','sn_dev_call_dial_outbound_sdk_call','sn_5_call_dial_outbound_succ'
			,'sn_5_call_dial_outbound_ring','sn_dev_call_dial_outbound_did_answer','sn_5_call_dial_end','sn_5_serve_voice_answer_begin','sn_5_serve_voice_answer_fail'
			,'sn_5_serve_voice_answer_succ','sn_dev_call_dial_inbound_click_answer','sn_dev_call_dial_inbound_did_answer','sn_dev_call_dial_inbound_connect_sdk'
			,'sn_5_call_dial_inbound_succ')
			and ((safe_CAST(SPLIT(app_version, '.')[SAFE_OFFSET(0)] AS INT)>=3 and safe_CAST(SPLIT(app_version, '.')[SAFE_OFFSET(1)] AS INT)>=8  )
			      or app_version='undefined'
			      or (safe_CAST(SPLIT(app_version, '.')[SAFE_OFFSET(0)] AS INT)>=4 ))
			)a 
		
		)b 
		where tag=1
		group by event_date,event_name,app_version,package_name;



	end;
