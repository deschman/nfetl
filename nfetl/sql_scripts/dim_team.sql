CREATE VIEW dim_team AS 
	SELECT
		dim_team_sq1."Franchise_ID", 
		dim_team_sq1."Team_ID", 
		dim_team_sq1."Franchise_Name", 
		dim_team_sq1."Coach_ID", 
		dim_team_sq1."Coach_Stint_ID", 
		dim_team_sq1."Coach_Name", 
		dim_team_sq1."Year", 
		SUM(COALESCE(schedule_sq2."Wins", 0)) AS "Wins", 
		SUM(COALESCE(schedule_sq2."Losses", 0)) AS "Losses", 
		SUM(COALESCE(schedule_sq2."Points", 0)) AS "Points", 
		SUM(COALESCE(schedule_sq2."Yards", 0)) AS "Yards", 
		SUM(COALESCE(schedule_sq2."Turnovers", 0)) AS "Turnovers" 
	FROM (
		SELECT DISTINCT 
			id_franchise."Franchise_ID", 
			id_franchise."Team_ID", 
			id_franchise."Franchise_Name", 
			id_coach."Coach_ID", 
			id_coach."Coach_Stint_ID", 
			id_coach."Coach_Name", 
			id_franchise."Year"
		FROM id_franchise
		LEFT JOIN lkp_primary_coach 
			ON id_franchise."Team_ID" = lkp_primary_coach."Team_ID" 
		LEFT JOIN id_coach 
			ON lkp_primary_coach."Coach" = id_coach."Coach_Name" 
			AND id_franchise."Franchise_ID" = id_coach."Franchise_ID") AS dim_team_sq1
	LEFT JOIN (
		SELECT 
			"Year",
			"Winner" AS "Team_Name",
			COUNT(1) AS "Wins",
			0 AS "Losses",
			SUM("Winner_Points") AS "Points",
			SUM("Winner_Yards") AS "Yards",
			SUM("Winner_Turnovers") AS "Turnovers"
		FROM schedule
		WHERE "Winner" <> ''
		GROUP BY 
			"Year",
			"Team_Name"
		UNION ALL 
		SELECT 
			schedule_sq1."Year",
			schedule_sq1."Team_Name",
			0 AS "Wins",
			COUNT(1) AS "Losses",
			SUM(schedule."Loser_Points") AS "Points",
			SUM(schedule."Loser_Yards") AS "Yards",
			SUM(schedule."Loser_Turnovers") AS "Turnovers"
		FROM (
			SELECT DISTINCT 
				"Year",
				"Loser" AS "Team_Name"
			FROM schedule
			WHERE "Loser" <> '') AS schedule_sq1
		INNER JOIN schedule
			ON schedule_sq1."Team_Name" = schedule."Loser"
			AND schedule_sq1."Year" = schedule."Year"
		GROUP BY 
			schedule_sq1."Year",
			schedule_sq1."Team_Name") AS schedule_sq2
		ON dim_team_sq1."Franchise_Name" = schedule_sq2."Team_Name"  
		AND dim_team_sq1."Year" = schedule_sq2."Year" 
	GROUP BY 
		dim_team_sq1."Franchise_ID", 
		dim_team_sq1."Team_ID", 
		dim_team_sq1."Franchise_Name", 
		dim_team_sq1."Coach_ID", 
		dim_team_sq1."Coach_Stint_ID", 
		dim_team_sq1."Coach_Name", 
		dim_team_sq1."Year";