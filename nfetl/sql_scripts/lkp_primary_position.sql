CREATE VIEW lkp_primary_position AS 
	SELECT DISTINCT
		"Player_Name",
		FIRST_VALUE("Position") OVER (
			PARTITION BY "Player_Name"
			ORDER BY 
				LENGTH("Position") DESC,
				"Games" DESC, 
				"Position" DESC) AS "Primary_Position"
	FROM (
		SELECT 
			"Player_Name",
			"Position",
			SUM("Games") AS "Games_at_Position"
		FROM (
			SELECT DISTINCT 
				lkp_name."Player_Name",
				offense."Team" AS "Team_Name",
				offense."Year",
				offense."Fantasy_Position" AS "Position",
				offense."Games"
			FROM offense
			INNER JOIN lkp_player_name AS lkp_name 
				ON offense."Player" = lkp_name."Player_Lookup_Name") AS offense_sq1
		GROUP BY
			"Player_Name",
			"Position"
		UNION 
		SELECT 
			"Player_Name",
			"Position",
			SUM("Games") AS "Games_at_Position"
		FROM (
			SELECT DISTINCT 
				lkp_name."Player_Name",
				kicking."Team" AS "Team_Name",
				kicking."Year",
				CASE 
					WHEN kicking."Position" = ''
						THEN 'K'
					WHEN kicking."Position" = 'p'
						THEN 'P'
					ELSE kicking."Position"
				END AS "Position",
				kicking."Games"
			FROM kicking 
			INNER JOIN lkp_player_name AS lkp_name 
				ON kicking."Player" = lkp_name."Player_Lookup_Name") AS kicking_sq1
		GROUP BY 
			"Player_Name",
			"Position") AS pos_sq1;