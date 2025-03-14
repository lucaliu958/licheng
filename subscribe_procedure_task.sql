CREATE OR REPLACE PROCEDURE `gzdw2024.gz_dim.subscribe_procedure_task`(run_date DATE)
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

END;
