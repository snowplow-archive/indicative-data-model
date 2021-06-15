-- 2. Calculate with a drop and recompute logic

DROP TABLE IF EXISTS {{.scratch_schema}}.indicative_export_staged{{.entropy}};

CREATE TABLE {{.scratch_schema}}.indicative_export_staged{{.entropy}} AS
(
    WITH base_table AS
         (SELECT
              e.app_id, -- this table contains all events
              e.br_family,
              e.br_features_director,
              e.br_features_flash,
              e.br_features_gears,
              e.br_features_java,
              e.br_features_pdf,
              e.br_features_quicktime,
              e.br_features_realplayer,
              e.br_features_silverlight,
              e.br_features_windowsmedia,
              e.br_lang,
              e.br_name,
              e.br_renderengine,
              e.br_type,
              e.br_version,
              e.br_viewheight,
              e.br_viewwidth,
              e.doc_height,
              e.doc_width,
              e.domain_sessionid,
              e.domain_sessionidx,
              e.domain_userid,
              e.dvce_ismobile,
              e.dvce_screenheight,
              e.dvce_screenwidth,
              e.dvce_type,
              e.event,
              e.event_id,
              e.event_vendor,
              e.geo_city,
              e.geo_country,
              e.geo_region,
              e.geo_region_name,
              e.geo_timezone,
              e.geo_zipcode,
              e.mkt_campaign,
              e.mkt_clickid,
              e.mkt_content,
              e.mkt_medium,
              e.mkt_network,
              e.mkt_source,
              e.mkt_term,
              e.network_userid,
              e.os_family,
              e.os_manufacturer,
              e.os_name,
              e.os_timezone,
              e.page_referrer,
              e.page_title,
              e.page_url,
              e.page_urlfragment,
              e.page_urlhost,
              e.page_urlpath,
              e.page_urlquery,
              e.platform,
              e.refr_domain_userid,
              e.refr_medium,
              e.refr_source,
              e.refr_term,
              e.refr_urlfragment,
              e.refr_urlhost,
              e.refr_urlpath,
              e.refr_urlquery,
              e.se_action,
              e.se_category,
              e.se_label,
              e.se_property,
              e.se_value,
              e.ti_category,
              e.ti_currency,
              e.ti_name,
              e.ti_orderid,
              e.ti_price,
              e.ti_price_base,
              e.ti_quantity,
              e.ti_sku,
              e.tr_affiliation,
              e.tr_city,
              e.tr_country,
              e.tr_currency,
              e.tr_orderid,
              e.tr_shipping,
              e.tr_shipping_base,
              e.tr_state,
              e.tr_tax,
              e.tr_tax_base,
              e.tr_total,
              e.tr_total_base,
              e.user_fingerprint,
              e.user_id,
              e.user_ipaddress,
              e.useragent,
              e.collector_tstamp,
              e.event_name,
              CASE
                  WHEN event_name = 'page_view' AND page_urlpath = '/' THEN 'homepage_view'
                  ELSE event_name
              END AS custom_event_name, -- example derived event name field
              e.derived_tstamp
          FROM {{.atomic_schema}}.events_staged{{.entropy}} AS e
          WHERE app_id = 'website'),

        child_table_joins AS
         (SELECT
              b.*,
              cf.form_id AS change_form_form_id,      -- change form fields
              cf.element_id AS change_form_element_id,
              cf.node_name AS change_form_node_name,
              cf.element_classes AS change_form_element_classes,
              sf.form_id AS submit_form_form_id,      -- submit form fields
              sf.form_classes AS submit_form_form_classes,
              sf.elements AS submit_form_elements,
              lc.element_id AS link_click_element_id, -- link click fields
              lc.element_classes AS link_click_element_classes,
              lc.element_target AS link_click_element_target,
              lc.target_url AS link_click_target_url,
              wsf.email AS sign_up_email,             -- website sign up form fields
              wsf.company AS sign_up_company,
              wsf.serviceType AS sign_up_service_type,
              dr.company AS demo_request_company,     -- demo request fields
              dr.email AS demo_request_email,
              dr.insights AS demo_request_insights,
              wp.id AS page_view_id
          FROM base_table AS b
                   LEFT JOIN {{.atomic_schema}}.com_snowplowanalytics_snowplow_change_form_1 AS cf
          ON cf.root_id = b.event_id -- add any necessary joins into your query and the necessary columns
              AND cf.root_tstamp = b.collector_tstamp
              LEFT JOIN {{.atomic_schema}}.com_snowplowanalytics_snowplow_submit_form_1 AS sf ON sf.root_id = b.event_id
              AND sf.root_tstamp = b.collector_tstamp
              LEFT JOIN {{.atomic_schema}}.com_snowplowanalytics_snowplow_link_click_1 AS lc ON lc.root_id = b.event_id
              AND lc.root_tstamp = b.collector_tstamp
              LEFT JOIN {{.atomic_schema}}.com_snowplowanalytics_snowplow_website_signup_form_submitted_1 AS wsf ON wsf.root_id = b.event_id
              AND wsf.root_tstamp = b.collector_tstamp
              LEFT JOIN {{.atomic_schema}}.com_snowplowanalytics_snowplow_website_demo_request_1 AS dr ON dr.root_id = b.event_id
              AND dr.root_tstamp = b.collector_tstamp
              LEFT JOIN {{.atomic_schema}}.com_snowplowanalytics_snowplow_web_page_1 AS wp ON wp.root_id = b.event_id
              AND wp.root_tstamp = b.collector_tstamp),

     user_mapping AS
         (SELECT
              user_id AS custom_user_id,
              domain_userid,
              MIN(derived_tstamp) AS first_seen_tstamp
          FROM {{.atomic_schema}}.events_staged{{.entropy}}
          WHERE app_id = 'website'
          GROUP BY 1,2)
SELECT
    c.*,
    CASE
        WHEN c.user_id IS NOT NULL THEN c.user_id
        WHEN u.custom_user_id IS NULL THEN c.domain_userid
        WHEN c.user_id IS NULL THEN u.custom_user_id
        END AS alias_user_id
FROM child_table_joins AS c
         LEFT JOIN user_mapping AS u
                   ON u.domain_userid = c.domain_userid
                       AND u.first_seen_tstamp < c.derived_tstamp);