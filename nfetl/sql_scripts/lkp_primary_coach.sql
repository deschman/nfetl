CREATE VIEW lkp_primary_coach AS 
	SELECT 
		id_franchise."Team_ID",
		coaches."Coach"
	FROM (
		SELECT 
			"Year",
			"Team",
			MAX("Games_w_Team") AS "Coach_Rank"
		FROM coaches
		GROUP BY 
			"Year",
			"Team") as coaches_sq1
	INNER JOIN coaches
		ON coaches_sq1."Year" = coaches."Year"
		AND coaches_sq1."Team" = coaches."Team"
		AND coaches_sq1."Coach_Rank" = coaches."Games_w_Team"
	LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv 
		ON coaches."Team" = lkp_abrv."Abbreviation" 
	LEFT JOIN id_franchise
		ON lkp_abrv."Franchise_Name" = id_franchise."Franchise_Name"
		AND coaches."Year" = id_franchise."Year"
	UNION ALL 
	SELECT 
		-1,
		'Unknown';