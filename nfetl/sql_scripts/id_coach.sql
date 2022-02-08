CREATE VIEW id_coach AS 
	SELECT 
		coach_sq1."Coach_ID",
		coach_sq1."Coach" AS "Coach_Name",
		coach_stint_sq1."Coach_Stint_ID",
		coach_stint_sq1."Franchise_ID", 
		id_franchise."Team_ID"
	FROM (
		SELECT 
			ROW_NUMBER() OVER (ORDER BY "Coach") AS "Coach_ID",
			"Coach"
		FROM coaches 
		GROUP BY "Coach") AS coach_sq1
	INNER JOIN (
		SELECT 
			ROW_NUMBER() OVER (
				ORDER BY 
					coaches."Coach", 
					coaches."Team") AS "Coach_Stint_ID",
			coaches."Coach",
			id_franchise."Franchise_ID"
		FROM coaches
		LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv
			ON coaches."Team" = lkp_abrv."Abbreviation" 
		INNER JOIN id_franchise
			ON lkp_abrv."Franchise_Name" = id_franchise."Franchise_Name" 
		GROUP BY 
			coaches."Coach",
			id_franchise."Franchise_ID") AS coach_stint_sq1
		ON coach_sq1."Coach" = coach_stint_sq1."Coach" 
	INNER JOIN id_franchise 
		ON id_franchise."Franchise_ID" = coach_stint_sq1."Franchise_ID"
	UNION ALL 
	SELECT 
		-1,
		'Unknown',
		-1,
		-1,
		-1;