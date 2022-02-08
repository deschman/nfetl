CREATE VIEW id_franchise AS 
	SELECT 
		franchise_sq2."Franchise_ID",
		franchise_sq2."Franchise_Name",
		team_sq2."Team_ID", 
		team_sq2."Year" 
	FROM (	
		SELECT 
			ROW_NUMBER() OVER (ORDER BY "Franchise_Name") AS "Franchise_ID",
			"Franchise_Name"
		FROM (
			SELECT DISTINCT "Winner" AS "Franchise_Name"
			FROM schedule
			WHERE "Winner" <> ''
			UNION 
			SELECT DISTINCT "Loser" AS "Franchise_Name"
			FROM schedule
			WHERE "Loser" <> '') AS franchise_sq1
		GROUP BY "Franchise_Name") AS franchise_sq2
	INNER JOIN (
		SELECT 
			ROW_NUMBER() OVER (
				ORDER BY 
					"Team_Name", 
					"Year") AS "Team_ID",
			"Team_Name",
			"Year" 
		FROM (
			SELECT DISTINCT 
				"Winner" AS "Team_Name",
				"Year"
			FROM schedule
			WHERE "Winner" <> ''
			UNION 
			SELECT DISTINCT 
				"Loser" AS "Team_Name",
				"Year"
			FROM schedule
			WHERE "Loser" <> '') as team_sq1
		GROUP BY 
			"Team_Name",
			"Year") AS team_sq2
		ON franchise_sq2."Franchise_Name" = team_sq2."Team_Name"
	UNION ALL 
	SELECT 
		-1,
		'2 Teams',
		-1,
		"Year_ID" AS "Year" 
	FROM dim_date
	WHERE "Year_ID" <= STRFTIME('%Y', 'now') + 0
	GROUP BY 
		"Year_ID";