WITH transactions AS (
--info from all the accounts
{% set accounts = [ {   'name'              : 'vul hier de naam van de administratie in',
                        'source'            : 'vul hier de source in'
                    }
                    ] %}

-- make a subquery for all accounts
{% for a in accounts if a['source'] != "vul hier de source in" %} 


	SELECT  financialyear
	,       financialperiod
	,       PARSE_DATE('%Y%m',cast(concat(financialyear, financialperiod) as string)) as year_month
	,	"{{a['name']}}" as administration
	,       CAST(ftl.glaccountcode AS INT64) AS glaccountcode
	,       ftl.glaccountdescription
	,       ftl.division
	,       accountcode
	,       accountname
	,       invoicenumber
	, 	duedate
    	,       date AS order_date
	,	ftl.description
    	,       itemcode
    	,       itemdescription
    	,       quantity
	,       journalcode
	,       journaldescription
	,       amountdc * -1 as amountdc
	,       amountfc * -1 as amountfc
	,       amountvatbasefc * -1 as amountvatbasefc
    	,       amountvatfc * -1 as amountvatfc
	, ROW_NUMBER() OVER (PARTITION BY ftl.id ORDER BY ftl.dataddo_extraction_timestamp DESC) AS dup_rank
	FROM {{a['source']}} ftl
	WHERE date(ftl.dataddo_extraction_timestamp) = current_date() 
    
  {% if not loop.last %}  UNION ALL  {% endif %}

  {% endfor %} 

)
SELECT *
FROM transactions
WHERE dup_rank = 1 AND date(order_date) >= DATE_SUB(current_date(), INTERVAL 6 MONTH)
