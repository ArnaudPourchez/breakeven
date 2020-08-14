## Getting the list of campaign of interest:

SELECT
	c.name,
	dt.campaign_id
FROM
	db3.campaigns AS c
INNER JOIN 
(
	SELECT DISTINCT
		d.campaign_id
	FROM
		db1.dailytransactions AS d
		INNER JOIN db3.products AS p ON d.product_id = p.id AND p.condition2 = 2
		INNER JOIN db1.summary_cost AS sc
			ON sc.costDate = d.transDate
			AND sc.campaign_id = d.campaign_id
	WHERE
		transDate BETWEEN "2017-01-01" AND CURRENT_DATE
		AND d.condition1 IN (3, 4)
		AND foreignUserID <> ''  
		AND foreignUserID IS NOT NULL
		) AS dt ON dt.campaign_id = c.id
GROUP BY
	dt.campaign_id;


## Getting the cohorts data, one campaign and one cohort at a time

SELECT
	dt.campaign_id,
	date_format(transDate, '%%Y-%%m') AS month,
	SUM(CASE WHEN user_type = "CC" THEN netAmt END) AS CC_revenue,
	SUM(CASE WHEN user_type = "Email" THEN netAmt END) AS Email_revenue,
	SUM(netAmt) AS all_revenue,
	co.cost AS cost
FROM
	db1.dailytransactions AS dt
	INNER JOIN db2.products AS p ON dt.product_id = p.id AND p.condition2 = 2
	INNER JOIN
	(
		SELECT DISTINCT
			foreignUserId,
			CASE WHEN d.transactionType_id = 4 THEN "CC"
				WHEN d.transactionType_id = 3 THEN "Email"
				END AS user_type
		FROM
			db1.dailytransactions AS d
			INNER JOIN db2.products AS p ON d.product_id = p.id AND p.condition2 = 2
		WHERE
			transDate BETWEEN "2017-01-01" AND LAST_DAY("2017-01-01")
			AND condition1 IN (3,4)
			AND foreignUserId <> ''  
			AND foreignUserId IS NOT NULL
			AND d.campaign_id = 123
	) AS ms ON ms.foreignUserId = dt.foreignUserId
	LEFT JOIN
		(
		SELECT
			date_format(costDate,'%%Y-%%m') AS cohort,
			SUM(cost) AS cost
		FROM
			db1.summary_cost AS sc 
		WHERE
			costDate  BETWEEN "2017-01-01" AND LAST_DAY("2017-01-01")
			AND campaign_id = 123
		GROUP BY
			date_format(costDate,'%%Y-%%m')
		) AS co ON co.cohort = date_format(dt.transDate, '%%Y-%%m')    
WHERE
	transDate BETWEEN "2017-01-01" AND CURRENT_DATE
	AND dt.foreignUserID <> ''  
	AND dt.foreignUserID IS NOT NULL
	AND dt.campaign_id = 123
GROUP BY	
	date_format(transDate, '%%Y-%%m'),
	dt.campaign_id;