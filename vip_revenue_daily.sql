CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.vip_revenue_daily`(run_date DATE, history_day INT64, end_day INT64)
begin

---------gp订阅收，广州所有产品

delete gzdw2024.googleplay.dwd_gp_sales_event_di
WHERE Order_Charged_Date >=date_add(run_date,interval -history_day day)
and Order_Charged_Date<=date_add(run_date,interval -end_day day) 
and Product_ID in (SELECT		package_name 
		FROM `gzdw2024.gz_dim.app_info` 
		where platform='android'
		group by package_name
		);


	insert    gzdw2024.googleplay.dwd_gp_sales_event_di 
--drop table if exists `gzdw2024.googleplay.dwd_gp_sales_event_di`;
--create table `gzdw2024.googleplay.dwd_gp_sales_event_di`
--	PARTITION BY Order_Charged_Date as 
SELECT
	stats_mon
	,Product_ID 
	,Order_Number
	,Order_Charged_Date
	,Product_Title
	,Product_Type
	,Country_of_Buyer
	,Currency_of_Sale
	,Financial_Status
	,Item_Price
	,Charged_Amount
	,Taxes_Collected	
	,SKU_ID
	,google_fee_rate
	,round(rate,4) as rate
	,safe_divide(Charged_Amount,rate) as Charged_Amount_usd
	,safe_divide(Taxes_Collected,rate) as Taxes_Collected_usd
	--,safe_divide(Item_Price,rate) as Item_Price_usd
	,round(safe_divide(Charged_Amount - Taxes_Collected,rate),4) as Revenue_Without_VAT
	,round(safe_divide(Charged_Amount - Taxes_Collected,rate)*google_fee_rate,4) as Google_Fee
	,round(safe_divide(Charged_Amount - Taxes_Collected,rate) - safe_divide(Charged_Amount - Taxes_Collected,rate)*google_fee_rate,4) as Revenue
FROM
	(
	SELECT
		a.stats_mon
		,Product_ID 
		,Order_Number
		,Order_Charged_Date
		,Product_Title
		,Product_Type
		,Country_of_Buyer
		,Currency_of_Sale
		,Financial_Status
		,Item_Price
		,Charged_Amount
		,Taxes_Collected	
		,SKU_ID
		,google_fee_rate
		,case when b1.rate is not null then b1.rate else b.rate end as rate 
	FROM
		(
		SELECT 
		Product_ID 
		,Order_Number
		,Order_Charged_Date
		,Product_Title
		,Product_Type
		,Country_of_Buyer
		,Currency_of_Sale
		,Financial_Status
		,Item_Price
		,Charged_Amount
		,Taxes_Collected
		,substring(cast(Order_Charged_Date as string),1,7) as stats_mon
		,LOWER(SKU_ID) AS SKU_ID
		 ,CASE WHEN lower(Product_Type) ='subscription' 
		     THEN 0.15
		   WHEN lower(Product_Type) ='inapp' 
		     THEN 0.3
		     ELSE 0.3
		  END AS google_fee_rate
		FROM
			(
			SELECT
				*
			FROM `gzdata.googleplay.p_sales_vidma` 
			WHERE 1=1
			--and Order_Charged_Date='2025-03-14'
			and Order_Charged_Date >=date_add(run_date,interval -history_day day)
			and Order_Charged_Date<=date_add(run_date,interval -end_day day)
			union all 
			SELECT
				Order_Number
				,Order_Charged_Date
				,Order_Charged_Timestamp
				,Financial_Status
				,Device_Model
				,Product_Title
				,Product_ID
				,Product_Type
				,SKU_ID
				,Currency_of_Sale
				,Item_Price
				,Taxes_Collected
				,Charged_Amount
				,City_of_Buyer
				,State_of_Buyer
				,Postal_Code_of_Buyer
				,Country_of_Buyer
			FROM `hzdw2024.ads.ads_google_play_sales` 
			WHERE 1=1
			--and Order_Charged_Date='2025-03-14'
			and Order_Charged_Date >=date_add(run_date,interval -history_day day)
			and Order_Charged_Date<=date_add(run_date,interval -end_day day)
		
			)
		)a 
		left join 
		(
		SELECT 
			stats_mon
			,currency
			,rate 
		FROM `gzdw2024.gz_dim.exchange_rate` 
		)b 
		on a.stats_mon=b.stats_mon
		and a.Currency_of_Sale=b.currency
			left join 
		(
		SELECT 
		cast(stats_date as date) as stats_date
		,currency
		,rate 
		FROM `gzdw2024.gz_dim.exchange_rate_days` 
		where cast(stats_date as date)>=date_add(run_date,interval -history_day day)
		and cast(stats_date as date)<=date_add(run_date,interval -end_day day)
		)b1 
		on a.Order_Charged_Date=b1.stats_date
		and a.Currency_of_Sale=b1.currency
		join 
		(
		SELECT 
			package_name 
		FROM `gzdw2024.gz_dim.app_info` 
		where platform='android'
		group by package_name
		)c 
		on a.Product_ID=c.package_name
	)d
		union all 
	SELECT 
		substring(cast(SAFE.PARSE_DATE('%Y/%m/%d', string_field_1) as string),1,7) as   stats_mon 
		,string_field_5 as Product_ID 
		,null as Order_Number
		,SAFE.PARSE_DATE('%Y/%m/%d', string_field_1) Order_Charged_Date
		,string_field_8 as Product_Title
		,null as Product_Type
		,null as Country_of_Buyer
		,string_field_6 as Currency_of_Sale
		,null as Financial_Status
		,null as Item_Price
		,null as Charged_Amount
		,null as Taxes_Collected	
		,string_field_9 as SKU_ID
		,0 as google_fee_rate
		,1 as rate
		,null as Charged_Amount_usd
		,null as Taxes_Collected_usd
		--,safe_divide(Item_Price,rate) as Item_Price_usd
		,null as Revenue_Without_VAT
		,null as Google_Fee
		,safe_cast(string_field_7 as float64) as Revenue
	FROM `gzdw2024.revenue.guangzhou_gp_sales_adjust_data`
	WHERE SAFE.PARSE_DATE('%Y/%m/%d', string_field_1) is not null 
	and SAFE.PARSE_DATE('%Y/%m/%d', string_field_1)>=date_add(run_date,interval -history_day day)
	and SAFE.PARSE_DATE('%Y/%m/%d', string_field_1)<=date_add(run_date,interval -end_day day);

-----------gp订阅收入所有产品
delete gzdw2024.revenue.dws_app_country_vip_income
WHERE stats_date >=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day day) 
and package_name in (SELECT		package_name 
		FROM `gzdw2024.gz_dim.app_info` 
		where platform='android'
		group by package_name
		);

insert    gzdw2024.revenue.dws_app_country_vip_income 
	SELECT
		Order_Charged_Date as stats_date
		,package_name
		,upper(country_code) as country_code
		,sum(case when Revenue>=0 then Revenue else 0 end ) as charged_money
		,abs(sum(case when Revenue<0 then Revenue else 0 end)) as refund_money  
		,sum(Revenue) as vip_revenue
	FROM
		(
		SELECT
			Order_Charged_Date
			,ARRAY[Country_of_Buyer,'TOTAL'] as country_code
			,Revenue
			,Product_ID package_name
		FROM
			gzdw2024.googleplay.dwd_gp_sales_event_di 
		WHERE Order_Charged_Date >=date_add(run_date,interval -history_day day)
			and Order_Charged_Date<=date_add(run_date,interval -end_day day)
			
			)a 
		,UNNEST(country_code) as country_code
		group by stats_date,package_name,upper(country_code);
/*
-----------gp订阅收入所有产品
delete gzdw2024.revenue.dws_app_country_vip_income
WHERE stats_date >=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day day) 
and package_name in (SELECT		package_name 
		FROM `gzdw2024.gz_dim.app_info` 
		where platform='android'
		group by package_name
		);

insert    gzdw2024.revenue.dws_app_country_vip_income 
	SELECT
		stats_date
		,package_name
		,upper(country_code) as country_code
		,vip_revenue as charged_money
		,0 as refund_money  
		,vip_revenue as vip_revenue
	FROM  `gzdw2024.googleplay.gp`;
*/
-----applestore订阅收入atlasv
delete gzdw2024.revenue.dws_app_country_vip_income
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day day)
and package_name in ( SELECT package_name FROM `gzdw2024.gz_dim.app_info` where vip_income_table='gzdw2024.appstoreconnect.p_sales_atlasv');


	insert    gzdw2024.revenue.dws_app_country_vip_income 
	SELECT
		stats_date
		,a.package_name
		,upper(country_code) as country_code
		,sum(case when revenue_usd>0 then revenue_usd else 0 end ) as charged_money
		,sum(case when revenue_usd<0 then abs(revenue_usd) else 0 end ) as refund_money
		,sum(revenue_usd) as vip_revenue
	FROM
		(
		 SELECT 
    a.package_name 
    ,units * developer_proceeds / b.rate  revenue_usd
  
    ,a.stats_date
    ,ARRAY[country_code,'TOTAL'] as country_code
    FROM `gzdw2024.appstoreconnect.p_sales_atlasv` a 
    left join `gzdw2024.gz_dim.exchange_rate_days` b on cast(a.stats_date as date) = cast(b.stats_date as date) and a.currency_of_proceeds=b.currency
    WHERE a.stats_date>='2025-02-13'
		and a.stats_date>=date_add(run_date,interval -history_day day)
			and a.stats_date<=date_add(run_date,interval -end_day day)
		union all 
		SELECT 
		package_name 
		,revenue_usd
    	,stats_date
		,ARRAY[country_code,'TOTAL'] as country_code
		FROM `gzdw2024.appstoreconnect.p_sales_atlasv` 
		WHERE stats_date>=date_add(run_date,interval -history_day day)
			and stats_date<=date_add(run_date,interval -end_day day)
		and stats_date<'2025-02-13'
		)a 
		join 
		(
			SELECT package_name 
			FROM `gzdw2024.gz_dim.app_info` 
			where platform='iOS'
			group by package_name
		)c 
		on a.package_name=c.package_name
		,UNNEST(country_code) as country_code
		GROUP BY package_name,stats_date,country_code;




---applestore订阅收入vidma账号
delete gzdw2024.revenue.dws_app_country_vip_income
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day day)
and package_name in ( SELECT package_name FROM `gzdw2024.gz_dim.app_info` where vip_income_table='gzdw2024.appstoreconnect.p_sales_vidma');



	insert    gzdw2024.revenue.dws_app_country_vip_income 
	SELECT
		stats_date
		,a.package_name
		,upper(country_code) as country_code
		,sum(case when revenue_usd>0 then revenue_usd else 0 end ) as charged_money
		,sum(case when revenue_usd<0 then abs(revenue_usd) else 0 end ) as refund_money
		,sum(revenue_usd) as vip_revenue
	FROM
		(
		 SELECT 
    a.package_name 
    ,units * developer_proceeds / b.rate  revenue_usd
  
    ,a.stats_date
    ,ARRAY[country_code,'TOTAL'] as country_code
    FROM `gzdw2024.appstoreconnect.p_sales_vidma` a 
    left join `gzdw2024.gz_dim.exchange_rate_days` b on cast(a.stats_date as date) = cast(b.stats_date as date) and a.currency_of_proceeds=b.currency
    WHERE a.stats_date>='2025-02-13'
		and a.stats_date>=date_add(run_date,interval -history_day day)
			and a.stats_date<=date_add(run_date,interval -end_day day)
		union all 
		SELECT 
		package_name 
		,revenue_usd
    	,stats_date
		,ARRAY[country_code,'TOTAL'] as country_code
		FROM `gzdw2024.appstoreconnect.p_sales_vidma` 
		WHERE stats_date>=date_add(run_date,interval -history_day day)
			and stats_date<=date_add(run_date,interval -end_day day)
		and stats_date<'2025-02-13'
		)a 
		join 
		(
			SELECT package_name 
			FROM `gzdw2024.gz_dim.app_info` 
			where platform='iOS'
			group by package_name
		)c 
		on a.package_name=c.package_name
		,UNNEST(country_code) as country_code
		GROUP BY package_name,stats_date,country_code;





		end;
