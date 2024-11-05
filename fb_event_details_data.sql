CREATE OR REPLACE PROCEDURE `fb-ai-avatar-puzzle.fb_dw.fb_event_details_data`(run_date DATE)
begin 
DELETE
  fb-ai-avatar-puzzle.analytics_439907691.event_details_data
WHERE
  event_date >=DATE_ADD(run_date,INTERVAL -1 day);
INSERT
  fb-ai-avatar-puzzle.analytics_439907691.event_details_data
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  geo.country AS country,
  event_name,
  event_params.key AS KEY,
  event_params.value.string_value AS VALUE,
  event_params.value.int_value AS int_value,
  COUNT(DISTINCT user_pseudo_id) AS users,
  COUNT(*) AS counts
FROM
  `recorder-pro-50451.analytics_250268757.events_*`,
  UNNEST (event_params)event_params
WHERE
  _TABLE_SUFFIX >= REPLACE(CAST(DATE_ADD(run_date,INTERVAL -1 day)AS string),'-','')
  and stream_id='9692329810'
  AND event_name IN ("fb_app_launch",
    "fb_home_show",
    "fb_templ_res_click",
    "fb_templ_res_export",
    "fb_game_play_show",
    "fb_game_play_finish",
    "fb_game_play_exit",
    "fb_first_open",
    "ad_load_c",
    "ad_load_success_c",
    "ad_load_fail_c",
    "fb_templ_categ_show_entrance",
    "fb_templ_categ_show_entrance_temp_result",
    "fb_templ_categ_show_entrance_game_play",
    "fb_templ_categ_show_entrance_game_result",
    "fb_home_show_discover",
    "fb_home_show_category",
    "ad_impression_c",
    "fb_share_show",
    "fb_templ_res_click",
    "fb_templ_res_export",
    "fb_banner_show",
    "fb_banner_click",
    "fb_templ_back",
    "fb_templ_back_last",
    "fb_templ_back_con",
    "fb_templ_invite",
    "fb_templ_invite_c",
    "fb_templ_invite_e",
    "fb_home_tu",
    "fb_home_tu_g",
    "fb_home_newpop",
    "fb_home_newpop_c",
    "fb_home_newpop_x",
    "fb_mess_authorize",
    "fb_mess_authorize_c",
    "fb_mess_authorize_l",
    "fb_mess_export",
    "fb_templ_res_fail",
    "fb_templ_lock",
    "fb_home_tu_skip",
    "fb_templ_lock_c",
    "fb_templ_lock_ad",
    "fb_templ_lock_ad_s",
    "fb_templ_lock_d",
    "fb_templ_lock_d_s",
    "fb_mess_authorize",
    "fb_mess_authorize_c",
    "fb_mess_authorize_l",
    "fb_dia_pop",
    "fb_dia_pop_x",
    "fb_dia_daily_c",
    "fb_dia_daily_s",
    "fb_dia_ad_c",
    "fb_reward_ad_fail",
    "fb_dia_ad_s",
    "fb_dia_adfree_c",
    "fb_dia_adfree_f",
    "fb_dia_adfree_s",
    "fb_reward_ad_not_complete",
    "fb_openAdWatchTask_watch_ad_s",
    "fb_openAdWatchTask",
    "fb_openAdWatchTask_c",
    "fb_openAdWatchTask_watch_ad_c",
    "fb_temp_export_fail",
    "fb_magic_ai_click",
    "fb_magic_ai_export",
    "fb_templ_res_show",
    "fb_me_history",
    "fb_me_history_click",
    "fb_me_favorite",
    "fb_me_favorite_click",
    "fb_me_favorite_export",
    "fb_me_favorite_share",
    "fb_me_favorite_l",
    "fb_me_favorite_c",
    "fb_sc_authorize",
    "fb_sc_authorize_c",
    "fb_sc_authorize_l",
    "fb_home_surpriseme_click",
    "fb_home_surpriseme_export",
    "fb_templ_generate_loading_page",
    "fb_templ_res_stop",
    "reward_interstitial_skip",
    "fb_templ_lock_share",
    "fb_templ_lock_ad_f",
    "fb_search_show",
    "fb_search_click",
    "fb_gender_male",
    "fb_gender_female",
    "fb_templ_generate_success_page"
    ,"fb_otome_dlbh"
,"fb_otome_change_female"
,"fb_otome_change_female_s"
,"fb_otome_change_face"
,"fb_otome_change_face_s"
,"fb_otome_change_male"
,"fb_otome_share"
,"fb_otome_choose"
,"fb_otome_back"
,"fb_otome_ending"
,"fb_otome_poster_change"
,"fb_otome_poster_replay"
  ,"fb_templ_muti_click"
  ,"fb_templ_muti_click_start"
  ,"fb_templ_muti_click_export"
  ,"fb_search_show"
  ,"fb_search_click"
)
  AND event_params.key IN ("error",
    "code",
    "ga_session_id",
    "type",
    "entrance",
    "id",
    "placement",
    "from",
    "platform",
    "msg",
    "timeuse",
    "timeout",
    "uuid",
    "abtestVersion")
GROUP BY
  event_date,
  country,
  event_name,
  event_params.key,
  event_params.value.string_value,
  int_value
ORDER BY
  event_date,
  country,
  event_name,
  event_params.key,
  event_params.value.string_value,
  int_value ASC;

  end;
