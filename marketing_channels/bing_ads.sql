SELECT 
extract(date from timeperiod) as date
, country AS shipping_country
, " " as account
, CASE WHEN REGEXP_CONTAINS(LOWER(accountname), 'pinkgellac.nl') THEN 'NL' ELSE 'DE' END AS store
, campaignname as campaign_name
, 'Bing' as medium
, 'Paid Search' as channel
, impressions
, spend
, clicks
, conversions
, revenue as conversion_value 
FROM `vul hier de source in`  o

INNER JOIN (
    SELECT 
    CONCAT(campaignname, extract(date from timeperiod), accountid, adgroupid, country ) AS id
    , MAX(_sdc_sequence) AS sequence
    , MAX(_sdc_batched_at) as batched_at
    FROM `vul hier de source in` 
    GROUP BY id
    ) oo
    ON CONCAT(campaignname, extract(date from timeperiod), accountid, adgroupid, country ) = oo.id           
            AND o._sdc_sequence = oo.sequence
            AND o._sdc_batched_at = oo.batched_at
