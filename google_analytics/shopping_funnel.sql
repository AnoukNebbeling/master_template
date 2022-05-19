--info from all the accounts
{% set accounts = [  {
                    'store'       : 'vul hier de store in',
                    'source'      : 'vul hier de source in'
                    }
                    ,
                    {
                    'store'       : 'vul hier de store in',
                    'source'      : 'vul hier de source in'
                    }
                    ,
                    {
                    'store'       : 'vul hier de store in',
                    'source'      : 'vul hier de source in'
                    }                  
                    ] %}

-- make a subquery for all accounts and deduplicate them

{% for a in accounts if a['source'] != "vul hier de source in" %} 

SELECT 
DATE(DATETIME(cast(o.ga_date as timestamp), "Europe/Amsterdam")) AS date
, o.store
, o.ga_country AS shipping_country
, LOWER(REGEXP_REPLACE(o.ga_shoppingstage, "_", " ")) AS shopping_stage
, SUM(ga_sessions) AS sessions
FROM {{a['source']}} o

 INNER JOIN (
    SELECT
    store
    , ga_date
    , ga_country
    , ga_shoppingstage
    , MAX(_sdc_sequence) AS sequence
    , MAX(_sdc_batched_at) AS batched_at
    FROM {{a['source']}} o
    GROUP BY 1,2,3,4) oo
    
    ON  o.store = oo.store
    AND o.ga_date = oo.ga_date
    AND o.ga_country = oo.ga_country
    AND o.ga_shoppingstage = oo.ga_shoppingstage
    AND o._sdc_sequence = oo.sequence
    AND o._sdc_batched_at = oo.batched_at   

GROUP BY 1,2,3,4


{% if not loop.last %} UNION ALL {% endif %}

{% endfor %} 
