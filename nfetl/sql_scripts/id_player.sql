CREATE VIEW id_player AS 
	SELECT 
		player_sq1."Player_ID",
		player_sq1."Player_Name",
		player_stint_sq1."Player_Stint_ID",
		id_franchise."Franchise_ID",
		id_franchise."Team_ID"
	FROM (
		SELECT  
			ROW_NUMBER() OVER (ORDER BY "Player_Name") AS "Player_ID",
			"Player_Name"
		FROM lkp_player_name
		GROUP BY 
			"Player_Name") AS player_sq1
	INNER JOIN (
		SELECT  
			ROW_NUMBER() OVER (
				ORDER BY 
					lkp_name."Player_Name",
					COALESCE(offense."Year", kicking."Year")) AS "Player_Stint_ID",
			lkp_name."Player_Name",
			COALESCE(offense."Team", kicking."Team") AS "Team_Name"
		FROM lkp_player_name AS lkp_name 
		LEFT JOIN offense 
			ON lkp_name."Player_Lookup_Name" = offense."Player"
		LEFT JOIN kicking 
			ON lkp_name."Player_Lookup_Name" = kicking."Player" 
		GROUP BY 
			lkp_name."Player_Name",
			COALESCE(offense."Team", kicking."Team")) AS player_stint_sq1
		ON player_sq1."Player_Name" = player_stint_sq1."Player_Name"
	LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv 
		ON player_stint_sq1."Team_Name" = lkp_abrv."Abbreviation" 
	LEFT JOIN id_franchise 
		ON lkp_abrv."Franchise_Name" = id_franchise."Franchise_Name"
	GROUP BY 
		player_sq1."Player_ID",
		player_sq1."Player_Name",
		player_stint_sq1."Player_Stint_ID",
		id_franchise."Franchise_ID",
		id_franchise."Team_ID"
	UNION ALL 
	SELECT 
		-1,
		'Unknown',
		-1,
		-1,
		-1;