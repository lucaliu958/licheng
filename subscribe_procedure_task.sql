CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.subscribe_procedure_task`(run_date DATE,history_day int64,history_end_day int64)
begin


 DECLARE recoder_pro_table_exists BOOL;
 DECLARE downloader_table_exists BOOL;
 DECLARE dt STRING;

 SET dt = FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(date_add(run_date,interval -2 day)));


 EXECUTE IMMEDIATE FORMAT
 ("""
    ALTER TABLE `scanner-master-android.analytics_196427335.woolong_events_%s`
    SET OPTIONS (expiration_timestamp = NULL)
  """, replace(cast(date_add(run_date,interval -2 day) as string),'-','') );

 EXECUTE IMMEDIATE FORMAT
 ("""
    ALTER TABLE `mv-editor-4bf54.analytics_289941232.woolong_events_%s`
    SET OPTIONS (expiration_timestamp = NULL)
  """, replace(cast(date_add(run_date,interval -2 day) as string),'-','') );

 EXECUTE IMMEDIATE FORMAT
 ("""
    ALTER TABLE `vidmaeditor-ios.analytics_296293128.woolong_events_%s`
    SET OPTIONS (expiration_timestamp = NULL)
  """, replace(cast(date_add(run_date,interval -2 day) as string),'-','') );

  SET recoder_pro_table_exists = (
        SELECT COUNT(*) > 0 
        FROM `recorder-pro-50451.analytics_250268757.INFORMATION_SCHEMA.TABLES` 
        WHERE table_name = CONCAT('woolong_events_', dt)
    );

  IF recoder_pro_table_exists THEN 
 EXECUTE IMMEDIATE FORMAT
 ("""
    ALTER TABLE `recorder-pro-50451.analytics_250268757.woolong_events_%s`
    SET OPTIONS (expiration_timestamp = NULL)
  """, replace(cast(date_add(run_date,interval -2 day) as string),'-','') );
END IF;

  SET downloader_table_exists = (
        SELECT COUNT(*) > 0 
        FROM `recorder-pro-50451.analytics_250268757.INFORMATION_SCHEMA.TABLES` 
        WHERE table_name = CONCAT('woolong_events_', dt)
    );

  IF downloader_table_exists THEN 
 EXECUTE IMMEDIATE FORMAT
 ("""
    ALTER TABLE `nova-downloader.analytics_206663592.woolong_events_%s`
    SET OPTIONS (expiration_timestamp = NULL)
  """, replace(cast(date_add(run_date,interval -2 day) as string),'-','') );
END IF;

delete gzdw2024.subscribe_data.dwd_gp_sales_event_cat_di
--	where Order_Charged_Date>='2025-01-01';
WHERE Order_Charged_Date >=date_add(run_date,interval -history_day day)
and Order_Charged_Date<=date_add(run_date,interval -history_end_day day) ;

insert gzdw2024.subscribe_data.dwd_gp_sales_event_cat_di
--drop table if exists `gzdw2024.subscribe_data.dwd_gp_sales_event_cat_di`;
--create table `gzdw2024.subscribe_data.dwd_gp_sales_event_cat_di`
--	PARTITION BY Order_Charged_Date as 
	SELECT 
	  stats_mon
	  ,Product_ID
	  ,a.Order_Number
	  ,Order_Charged_Date
	  ,Product_Title
	  ,Product_Type
	  ,Country_of_Buyer
	  ,Currency_of_Sale
	  ,Financial_Status
	  ,Item_Price
	  ,Charged_Amount
	  ,Taxes_Collected
	  ,a.SKU_ID
	  ,google_fee_rate
	  ,rate
	  ,Charged_Amount_usd
	  ,Taxes_Collected_usd
	  ,Revenue_Without_VAT
	  ,Google_Fee
	  ,Revenue
	  ,orgin_order_number
	  ,case when c.Order_Number is not null  then 'new' else 'renew' end as renew_tag 
	FROM
		(
		SELECT 
		  *
		  ,substring(Order_Number,1,24) as orgin_order_number
		FROM  `gzdw2024.googleplay.dwd_gp_sales_event_di`
		where 1=1
		and Order_Charged_Date>='2025-01-01'
		and Order_Charged_Date >=date_add(run_date,interval -history_day day)
		and Order_Charged_Date<=date_add(run_date,interval -history_end_day day)
		)a  
		left join
		(
		SELECT
			Order_Number
			,SKU_ID
		FROM
			(	
			SELECT
				Order_Number
				,orgin_order_number
				,SKU_ID
				,row_number() over(partition by orgin_order_number order by Order_Charged_Date) as rn 
			FROM
				(
				SELECT 
				  Order_Number
				  ,SKU_ID
				  ,Order_Charged_Date
				  ,substring(Order_Number,1,24) as orgin_order_number
				FROM  `gzdw2024.googleplay.dwd_gp_sales_event_di`
				WHERE Financial_Status='Charged'
				)a  
			)b 
			where 1=1
			and  rn=1 
			and   (NOT REGEXP_CONTAINS(Order_Number, r'\.\.') 
                  OR REGEXP_CONTAINS(Order_Number, r'\.\.0$') )
		)c 
		on  a.Order_Number=c.Order_Number;


delete gzdw2024.subscribe_data.dws_subscribe_sales_cat_report
WHERE 1=1
--and stats_date>='2025-01-01';
and stats_date >=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -history_end_day day) ;

insert `gzdw2024.subscribe_data.dws_subscribe_sales_cat_report`

--drop table if exists `gzdw2024.subscribe_data.dws_subscribe_sales_cat_report`;
--create table `gzdw2024.subscribe_data.dws_subscribe_sales_cat_report`
--	PARTITION BY stats_date as 
	SELECT
		Order_Charged_Date as stats_date
		,a.package_name
		,app_name
		,upper(country_code) as country_code
		,sub_type
		,SKU_ID as sub_id
		,sum(case when Revenue>=0 then Revenue else 0 end ) as charged_money
		,abs(sum(case when Revenue<0 then Revenue else 0 end)) as refund_money  
		,sum(Revenue) as vip_revenue
		,count(Order_Number) as order_num
		,sum(case when renew_tag='new' and lower(Product_Type) ='subscription' then  Revenue else 0 end) as new_vip_revenue
		,sum(case when renew_tag='renew' and lower(Product_Type) ='subscription' then  Revenue else 0 end) as renew_vip_revenue
		,sum(case when  lower(Product_Type) ='inapp' then  Revenue else 0 end) as one_time_vip_revenue
		,count(distinct case when renew_tag='new'  and lower(Product_Type) ='subscription' then Order_Number else null end) as new_order_num
		,count(distinct case when renew_tag='renew'  and lower(Product_Type) ='subscription' then Order_Number else null end) as renew_order_num
		,count(distinct  case when lower(Product_Type) ='inapp' then Order_Number else null end) as one_time_order_num
	FROM
		(
		SELECT
			Order_Charged_Date
			,Order_Number
			,ARRAY[Country_of_Buyer,'TOTAL'] as country_code
			,Revenue
			,Product_ID package_name
			,SKU_ID
			,Product_Type
			,case  WHEN lower(Product_Type) ='inapp' then 'one_time'
			when REGEXP_CONTAINS(SKU_ID, r'.*(12_month|year)')  then 'yeraly'
			when REGEXP_CONTAINS(SKU_ID, r'.*(month)') then 'monthly'
			when REGEXP_CONTAINS(SKU_ID, r'.*(week)') then 'weekly'
			 when REGEXP_CONTAINS(SKU_ID, r'.*(sub_us_ca)') and safe_divide(Item_Price,rate)>=20  then 'yeraly'
			 when REGEXP_CONTAINS(SKU_ID, r'.*(sub_us_ca)') and safe_divide(Item_Price,rate)>=12  then 'monthly'
			 when REGEXP_CONTAINS(SKU_ID, r'.*(sub_us_ca)') and safe_divide(Item_Price,rate)>=0.2  then 'weekly'
			else 'other' end  as sub_type
			,safe_divide(Item_Price,rate) as Item_Price_usd
			,Charged_Amount_usd
			,renew_tag
			,Financial_Status
		FROM `gzdw2024.subscribe_data.dwd_gp_sales_event_cat_di` a
		WHERE 1=1
		and Order_Charged_Date >='2025-01-01'
		and a.Order_Charged_Date >= date_add(run_date,interval -history_day day)
	    and a.Order_Charged_Date <= date_add(run_date,interval -history_end_day day)
		)a 
		join 
		(
			SELECT package_name ,app_name
			FROM `gzdw2024.gz_dim.app_info` 
			--where platform='iOS'
			group by package_name,app_name
		)c 
		on a.package_name=c.package_name
	,UNNEST(country_code) as country_code
	group by Order_Charged_Date,country_code,package_name,sub_type,sub_id,app_name;


----------ios订阅收入划分

--------分订阅项收入情况


insert `gzdw2024.subscribe_data.dws_subscribe_sales_cat_report`
	SELECT
		stats_date
		,a.package_name
		,app_name
		,upper(country_code) as country_code
		,sub_type
		,sku as sub_id
		,sum(case when revenue_usd>0 then revenue_usd else 0 end ) as charged_money
		,sum(case when revenue_usd<0 then abs(revenue_usd) else 0 end ) as refund_money
		,sum(revenue_usd) as vip_revenue
		,cast(sum(units) as int64) as order_num
		,0 as new_vip_revenue
		,0 as renew_vip_revenue
		,sum(case when sub_type='one_time' then revenue_usd else 0 end) as one_time_vip_revenue
		,0 as new_order_num
		,0 as renew_order_num
		,cast(sum(case when  sub_type='one_time' then  units else 0 end) as int64) as one_time_order_num
	FROM
		(
		SELECT 
		package_name 
		,units * developer_proceeds / b.rate  revenue_usd
    	,a.stats_date
		,ARRAY[country_code,'TOTAL'] as country_code
		,period
		,subscription
		,units
		,title
		,product_type_identifier
		,sku
		,proceeds_reason
		,case when lower(product_type_identifier) not like '%iay%'  then 'one_time'
			when REGEXP_CONTAINS(period, r'.*(1 Month)') then 'monthly'
			when REGEXP_CONTAINS(period, r'.*(7 Days)') then 'weekly'
			 when REGEXP_CONTAINS(period, r'.*(1 Year)')  then 'yeraly'
			else 'other' end  as sub_type
		FROM `gzdw2024.appstoreconnect.p_sales_atlasv`  a
	    left join `gzdw2024.gz_dim.exchange_rate_days` b on cast(a.stats_date as date) = cast(b.stats_date as date) and a.currency_of_proceeds=b.currency
    	WHERE 1=1
		and a.stats_date>='2025-01-01'
		and a.stats_date >= date_add(run_date,interval -history_day day)
	    and a.stats_date <= date_add(run_date,interval -history_end_day day)
		and (units * developer_proceeds>0 or units * developer_proceeds <0 )
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
		GROUP BY package_name,stats_date,country_code,app_name,sku
		,sub_type;

END;
