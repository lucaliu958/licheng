CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.vip_revenue_daily`(run_date DATE, history_day INT64, end_day INT64)
begin


-----------gp订阅收入,vidma
delete gzdw2024.revenue.dws_app_country_vip_income
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day day)
and package_name in ( SELECT package_name FROM `gzdw2024.gz_dim.app_info` where vip_income_table='gzdw2024.googleplay.p_sales_vidma');


	insert    gzdw2024.revenue.dws_app_country_vip_income 
	SELECT
		Order_Charged_Date as stats_date
		,package_name
		,upper(country_code) as country_code
		,sum(case when Financial_Status='Charged' then  Item_Price/rate else 0 end) as charged_money
		,sum(case when Financial_Status='Refund' then  Item_Price/rate else 0 end) as refund_money  
		,sum(case when Financial_Status='Charged' then  Item_Price/rate else 0 end)- sum(case when Financial_Status='Refund' then  Item_Price/rate else 0 end) as vip_revenue
	FROM
		(
		SELECT 
		Product_ID 
		,Charged_Amount
		,ARRAY[Country_of_Buyer,'TOTAL'] as country_code
		,Currency_of_Sale
		,Financial_Status
		,Order_Charged_Date
		,Item_Price
		,substring(cast(Order_Charged_Date as string),1,7) as stats_mon
		FROM `gzdata.googleplay.p_sales_vidma` 
		WHERE Order_Charged_Date >=date_add(run_date,interval -history_day day)
		and Order_Charged_Date<=date_add(run_date,interval -end_day day)
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
		join 
		(
		SELECT 
			package_name 
		FROM `gzdw2024.gz_dim.app_info` 
		where platform='android'
		group by package_name
		)c 
		on a.Product_ID=c.package_name
		,UNNEST(country_code) as country_code
		GROUP BY package_name,Order_Charged_Date,country_code;



-----------gp订阅收入,all3
delete gzdw2024.revenue.dws_app_country_vip_income
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day day)
and package_name in ( SELECT package_name FROM `gzdw2024.gz_dim.app_info` where vip_income_table='hzdw2024.googleplay.p_sales_new_downloader');



	insert    gzdw2024.revenue.dws_app_country_vip_income 
	SELECT
		Order_Charged_Date as stats_date
		,package_name
		,upper(country_code) as country_code
	,sum(case when Financial_Status='Charged' then  Item_Price/rate else 0 end) as charged_money
		,sum(case when Financial_Status='Refund' then  Item_Price/rate else 0 end) as refund_money  
		,sum(case when Financial_Status='Charged' then  Item_Price/rate else 0 end)- sum(case when Financial_Status='Refund' then  Item_Price/rate else 0 end) as vip_revenue
	FROM
		(
		SELECT 
		Product_ID 
		,Charged_Amount
		,ARRAY[Country_of_Buyer,'TOTAL'] as country_code
		,Currency_of_Sale
		,Financial_Status
		,Order_Charged_Date
		,Item_Price
		,substring(cast(Order_Charged_Date as string),1,7) as stats_mon
		FROM `hzdw2024.googleplay.p_sales_new_downloader`  
		WHERE Order_Charged_Date >=date_add(run_date,interval -history_day day)
		and Order_Charged_Date<=date_add(run_date,interval -end_day day)
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
		join 
		(
			SELECT package_name 
			FROM `gzdw2024.gz_dim.app_info` 
			where platform='android'
			group by package_name
		)c 
		on a.Product_ID=c.package_name
		,UNNEST(country_code) as country_code
		GROUP BY package_name,Order_Charged_Date,country_code;




-----------gp订阅收入,all1
delete gzdw2024.revenue.dws_app_country_vip_income
where stats_date>=date_add(run_date,interval -history_day day)
and stats_date<=date_add(run_date,interval -end_day day)
and package_name in ( SELECT package_name FROM `gzdw2024.gz_dim.app_info` where vip_income_table='hzdw2024.googleplay.p_sales_downloader');


	insert    gzdw2024.revenue.dws_app_country_vip_income 
	SELECT
		Order_Charged_Date as stats_date
		,package_name
		,upper(country_code) as country_code
	,sum(case when Financial_Status='Charged' then  Item_Price/rate else 0 end) as charged_money
		,sum(case when Financial_Status='Refund' then  Item_Price/rate else 0 end) as refund_money  
		,sum(case when Financial_Status='Charged' then  Item_Price/rate else 0 end)- sum(case when Financial_Status='Refund' then  Item_Price/rate else 0 end) as vip_revenue
	FROM
		(
		SELECT 
		Product_ID 
		,Charged_Amount
		,ARRAY[Country_of_Buyer,'TOTAL'] as country_code
		,Currency_of_Sale
		,Financial_Status
		,Order_Charged_Date
		,Item_Price
		,substring(cast(Order_Charged_Date as string),1,7) as stats_mon
		FROM `hzdw2024.googleplay.p_sales_downloader`  
		WHERE Order_Charged_Date >=date_add(run_date,interval -history_day day)
		and Order_Charged_Date<=date_add(run_date,interval -end_day day)
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
		join 
		(
			SELECT package_name 
			FROM `gzdw2024.gz_dim.app_info` 
			where platform='android'
			group by package_name
		)c 
		on a.Product_ID=c.package_name
		,UNNEST(country_code) as country_code
		GROUP BY package_name,Order_Charged_Date,country_code;

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
		package_name 
		,revenue_usd
    	,stats_date
		,ARRAY[country_code,'TOTAL'] as country_code
		FROM `gzdw2024.appstoreconnect.p_sales_atlasv` 
		WHERE stats_date>=date_add(run_date,interval -history_day day)
			and stats_date<=date_add(run_date,interval -end_day day)
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
		package_name 
		,revenue_usd
    	,stats_date
		,ARRAY[country_code,'TOTAL'] as country_code
		FROM `gzdw2024.appstoreconnect.p_sales_vidma` 
		WHERE stats_date>=date_add(run_date,interval -history_day day)
			and stats_date<=date_add(run_date,interval -end_day day)
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
