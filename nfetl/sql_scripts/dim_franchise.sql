CREATE VIEW dim_franchise AS 
	SELECT 
		dim_team."Franchise_ID",
		dim_team."Franchise_Name",
		dim_team_coaches."Coach_Count",
		dim_team_seasons."Season_Count",
		MIN(dim_team."Year") AS "Founding_Season",
		MAX(dim_team."Year") AS "Final_Season",
		SUM(dim_team."Wins") AS "Wins",
		SUM(dim_team."Losses") AS "Losses",
		SUM(dim_team."Points") AS "Points",
		SUM(dim_team."Yards") AS "Yards",
		SUM(dim_team."Turnovers") AS "Turnovers"
	FROM dim_team
	INNER JOIN (
		SELECT 
			"Franchise_ID",
			COUNT(1) AS "Coach_Count"
		FROM (
			SELECT DISTINCT 
				"Franchise_ID",
				"Coach_ID"
			FROM dim_team)
		GROUP BY "Franchise_ID") AS dim_team_coaches
		ON dim_team."Franchise_ID" = dim_team_coaches."Franchise_ID"
	INNER JOIN (
		SELECT 
			"Franchise_ID",
			COUNT(1) AS "Season_Count"
		FROM (
			SELECT DISTINCT 
				"Franchise_ID",
				"Year"
			FROM dim_team)
		GROUP BY "Franchise_ID") as dim_team_seasons
		ON dim_team."Franchise_ID" = dim_team_seasons."Franchise_ID"
	GROUP BY 
		dim_team."Franchise_ID",
		dim_team."Franchise_Name",
		dim_team_coaches."Coach_Count",
		dim_team_seasons."Season_Count";