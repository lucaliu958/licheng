CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.dws_ga_cost_daily_task`(run_date DATE, history_day INT64, end_day INT64)
begin
delete `gzdw2024.cost_data.dws_ga_cost_daily`
where stats_date>=date_add(run_date,interval -history_day day);


insert `gzdw2024.cost_data.dws_ga_cost_daily`
WITH
  campaign_info AS(
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_4891374411
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_1449238239
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_5964298848
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_8500620333
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_6006331386
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_8515949135
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_7647547266
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_9023445943
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_7114161798
  UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_5497157644
    UNION ALL
  SELECT
    DISTINCT campaign_id,
    campaign_name
  FROM
    hzdw2024.all.p_ads_Campaign_7890088142  ),

 cost_data AS(
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_4891374411 ---
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_1449238239 ---
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_5964298848 ---
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_8500620333 ---
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_6006331386 ---
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_8515949135
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_7647547266 ---
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_9023445943 ---
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_7114161798
  UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_5497157644
    UNION ALL
  SELECT
    *
  FROM
    hzdw2024.all.ads_GeoStats_7890088142 ),
  guangzhou_cost_info AS (
  SELECT
    a.campaign_id,
    b.campaign_name,
    CASE --
      WHEN LOWER(campaign_name) LIKE '%insaver%' THEN "instasaver.instagram.video.downloader.photo" --ins1 --
      WHEN LOWER(campaign_name) LIKE '%ins4%' THEN "instasaver.instagram.video.downloader.photo.ig" --ins4 --
      WHEN LOWER(campaign_name) LIKE '%ins2%' THEN "instagram.video.downloader.story.saver" --ins2 --
      WHEN LOWER(campaign_name) LIKE '%ins3 lite%'
    OR LOWER(campaign_name) LIKE '%ins3lite%' THEN "instagram.video.downloader.story.saver.ig.insaver" ---ins3-lite --
      WHEN LOWER(campaign_name) LIKE '%ins3%' THEN "instagram.video.downloader.story.saver.ig" ---ins3 --
      WHEN LOWER(campaign_name) LIKE '%ttdown%'OR LOWER(campaign_name) LIKE '%ttd1%' THEN "tiktok.video.downloader.nowatermark.tiktokdownload" --ttd1 --
      WHEN LOWER(campaign_name) LIKE '%ttd2%' THEN "tiktok.video.downloader.nowatermark.tiktokdownload.snaptik" --ttd2 --
      WHEN LOWER(campaign_name) LIKE '%fbd-%' THEN "facebook.video.downloader.savefrom.fb" --fbd1 --
      WHEN LOWER(campaign_name) LIKE '%fbd2-%' THEN "facebook.video.downloader.savefrom.fb.saver.fast" --fbd2 --
      WHEN LOWER(campaign_name) LIKE '%ga-tw%'
    OR LOWER(campaign_name) LIKE 'tw%'THEN "twittervideodownloader.twitter.videoindir.savegif.twdown" --twd --WHEN LOWER(campaign_name) LIKE '%shotcut%' THEN 'video.editor.videomaker.effects.fx' --shotcut --WHEN LOWER(campaign_name) LIKE '%collart%' THEN 'com.SpringTech.NewSC' --collart --WHEN LOWER(campaign_name) LIKE '%collart-gp%' THEN 'free.ai.photo.generator.collart.ai' --collart-android
      WHEN LOWER(campaign_name) LIKE '%scannner-ios%'
    OR LOWER(campaign_name) LIKE '%scanner-ios%' THEN "pdf.scanner.app"
      WHEN LOWER(campaign_name) LIKE '%scannerlens-ios%' THEN "pdf.scanner.app.plus" --scanner
      WHEN LOWER(campaign_name) LIKE '%ga-recorder pro%' THEN "vidma.screenrecorder.videorecorder.videoeditor.pro" --recorder
      WHEN LOWER(campaign_name) LIKE '%recorder lite%' THEN 'vidma.screenrecorder.videorecorder.videoeditor.lite' --recorder_lite
      WHEN LOWER(campaign_name) LIKE '%player%' THEN 'vidma.mkv.xvideo.player.videoplayer.free' --player
      WHEN LOWER(campaign_name) LIKE '%text-ios%' THEN 'second.phone.number.text.free.call.app' --text-ios
      WHEN LOWER(campaign_name) LIKE '%ga-vidma editor%' THEN 'vidma.video.editor.videomaker' --Vidma Editor_android
      WHEN LOWER(campaign_name) LIKE '%ga-st-pro%' THEN "com.internet.speedtest.check.wifi.meter" --stpro
      WHEN LOWER(campaign_name) LIKE '%all3%' THEN 'video.downloader.videodownloader.tube' --all3 --WHEN LOWER(campaign_name) LIKE '%all%' THEN 'free.video.downloader.converter.music' --all1
      ELSE "Other"
  END
    AS package_name,
   cast(geographic_view_country_criterion_id as string) as country_criterion_id
	,cast(a.campaign_id as string) as campaign_id
	,metrics_cost_micros
	,date(_DATA_DATE) as stats_date
  FROM
    cost_data a
  LEFT JOIN
    campaign_info b
  ON
    a.campaign_id=b.campaign_id )
SELECT
	a.stats_date
	,package_name
	,app_name
	,case when c.country_code is not null then upper(c.country_code) else a.country_criterion_id end as country_code
	,sum(metrics_cost_micros)/1000000*max(case when exchange_rate is not null then exchange_rate else 0.128 end ) as cost_usd
FROM
	(
	SELECT
		stats_date
		,campaign_name
		,package_name
    ,app_name
		,country_criterion_id
		,metrics_cost_micros
	FROM 
		(
		SELECT
		  a.stats_date,
		
		  a.campaign_name,
		  a.package_name,
		  app_name,
		  array['TOTAL',country_criterion_id] AS country_criterion_id,
		  metrics_cost_micros
		FROM
		  guangzhou_cost_info a
		join  `gzdw2024.gz_dim.app_info`  b  
		 on a.package_name=b.package_name
		 where 1=1
		 and stats_date>=date_add(run_date,interval -history_day day)
		 )a 
	,UNNEST(country_criterion_id) as country_criterion_id
	 )a 
	left join
	(
	SELECT stats_date,exchange_rate 
	FROM `scanner-master-android.scanner_ios_dw.exchange_rete` 
	where 1=1
	)d 
	on a.stats_date=d.stats_date
	left join
	(
	SELECT cast(Parent_ID as string) as country_criterion_id,upper(max(Country_Code)) as country_code
	FROM `scanner-master-android.scanner_ios_dw.geotargets20240813`
	group by Parent_ID
	)c
	on a.country_criterion_id=c.country_criterion_id
	
	GROUP BY a.stats_date,package_name,app_name,country_code;





end;
