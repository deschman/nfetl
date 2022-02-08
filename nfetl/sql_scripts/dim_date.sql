CREATE VIEW dim_date AS 
	WITH dim_date_sq1 AS (
		SELECT 
			years."Year_ID",
			months."Month_ID",
			months."Month_Name"
		FROM (
			SELECT SUBSTR(name, -4) + 0 AS "Year_ID"
			FROM sqlite_master 
			WHERE 
				type='table'
				AND SUBSTR(name, 1, 3) = 'CUR'
			GROUP BY SUBSTR(name, -4) + 0
			UNION 
			SELECT SUBSTR(name, -4) + 10 AS "Year_ID"
			FROM sqlite_master 
			WHERE 
				type='table'
				AND SUBSTR(name, 1, 3) = 'CUR'
			GROUP BY SUBSTR(name, -4) + 10) AS years, 
		(
			SELECT 
				1 AS "Month_ID",
				'January' AS "Month_Name"
			UNION ALL 
			SELECT 
				2 AS "Month_ID",
				'February' AS "Month_Name"
			UNION ALL 
			SELECT 
				3 AS "Month_ID",
				'March' AS "Month_Name"
			UNION ALL 
			SELECT 
				4 AS "Month_ID",
				'April' AS "Month_Name"
			UNION ALL 
			SELECT 
				5 AS "Month_ID",
				'May' AS "Month_Name"
			UNION ALL 
			SELECT 
				6 AS "Month_ID",
				'June' AS "Month_Name"
			UNION ALL 
			SELECT 
				7 AS "Month_ID",
				'July' AS "Month_Name"
			UNION ALL 
			SELECT 
				8 AS "Month_ID",
				'August' AS "Month_Name"
			UNION ALL 
			SELECT 
				9 AS "Month_ID",
				'September' AS "Month_Name"
			UNION ALL 
			SELECT 
				10 AS "Month_ID",
				'October' AS "Month_Name"
			UNION ALL 
			SELECT 
				11 AS "Month_ID",
				'November' AS "Month_Name"
			UNION ALL 
			SELECT 
				12 AS "Month_ID",
				'December' AS "Month_Name") AS months)
	SELECT 
		"Year_ID",
		CASE 
			WHEN "Month_ID" IN (10, 11, 12) THEN "Year_ID"
			WHEN "Month_ID" = 9 AND "Day_ID" / 7 > 1 THEN "Year_ID"
			ELSE "Year_ID" - 1
		END AS "NFL_Season",
		"Month_ID",
		"Month_Name",
		"Day_ID"
	FROM (
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			1 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			2 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			3 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			4 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			5 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			6 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			7 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			8 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			9 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			10 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			11 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			12 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			13 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			14 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			15 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			16 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			17 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			18 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			19 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			20 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			21 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			22 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			23 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			24 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			25 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			26 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			27 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			28 AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			CASE 
				WHEN "Month_ID" IN (1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12) THEN 29
				WHEN "Year_ID" % 4 = 0 AND "Year_ID" % 100 = 0 AND "Year_ID" % 400 = 0 THEN 29 
				ELSE NULL 
			END AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			CASE 
				WHEN "Month_ID" IN (1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12) THEN 30 
				ELSE NULL 
			END AS "Day_ID"
		FROM dim_date_sq1 
		UNION ALL 
		SELECT 
			"Year_ID",
			"Month_ID",
			"Month_Name",
			CASE 
				WHEN "Month_ID" IN (1, 3, 5, 7, 8, 10, 12) THEN 31 
				ELSE NULL 
			END AS "Day_ID"
		FROM dim_date_sq1) AS dim_date_sq2
	WHERE "Day_ID" IS NOT NULL;