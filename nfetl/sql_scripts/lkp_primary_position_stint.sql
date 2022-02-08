CREATE VIEW lkp_primary_position_stint AS 
	SELECT DISTINCT 
		"Player_Stint_ID",
		FIRST_VALUE("Position") OVER (
			PARTITION BY 
				"Player_Name",
				"Team_Name"
			ORDER BY 
				LENGTH("Position") DESC,
				"Games" DESC, 
				"Position" DESC) AS "Primary_Position"
	FROM (
		SELECT 
			"Player_Stint_ID",
			"Player_Name",
			"Team_Name",
			"Position",
			SUM("Games") AS "Games_at_Position"
		FROM (
			SELECT DISTINCT 
				id_player."Player_Stint_ID", 
				lkp_name."Player_Name",
				lkp_abrv."Franchise_Name" AS "Team_Name",
				offense."Year",
				offense."Fantasy_Position" AS "Position",
				offense."Games"
			FROM offense
			INNER JOIN lkp_player_name AS lkp_name 
				ON offense."Player" = lkp_name."Player_Lookup_Name"
			LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv 
				ON offense."Team" = lkp_abrv."Abbreviation"
			INNER JOIN id_franchise
				ON lkp_abrv."Franchise_Name" = id_franchise."Franchise_Name" 
			INNER JOIN id_player 
				ON lkp_name."Player_Name" = id_player."Player_Name"
				AND id_franchise."Franchise_ID" = id_player."Franchise_ID") AS offense_sq1
		GROUP BY
			"Player_Stint_ID",
			"Player_Name",
			"Team_Name",
			"Position"
		UNION 
		SELECT 
			"Player_Stint_ID",
			"Player_Name",
			"Team_Name",
			CASE 
				WHEN "Position" = ''
					THEN 'K'
				WHEN "Position" = 'p'
					THEN 'P'
				ELSE "Position" 
			END AS "Position",
			SUM("Games") AS "Games_at_Position"
		FROM (
			SELECT DISTINCT 
				id_player."Player_Stint_ID",
				lkp_name."Player_Name",
				lkp_abrv."Franchise_Name" AS "Team_Name",
				kicking."Year",
				kicking."Position",
				kicking."Games"
			FROM kicking 
			INNER JOIN lkp_player_name AS lkp_name 
				ON kicking."Player" = lkp_name."Player_Lookup_Name"
			LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv 
				ON kicking."Team" = lkp_abrv."Abbreviation"
			INNER JOIN id_franchise
				ON lkp_abrv."Franchise_Name" = id_franchise."Franchise_Name" 
			INNER JOIN id_player 
				ON lkp_name."Player_Name" = id_player."Player_Name"
				AND id_franchise."Franchise_ID" = id_player."Franchise_ID") AS kicking_sq1
		GROUP BY 
			"Player_Stint_ID",
			"Player_Name",
			"Team_Name",
			"Position") AS pos_sq1;