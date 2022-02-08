CREATE VIEW dim_coach AS 
	SELECT 
		"Coach_ID",
		"Coach_Name",
		COUNT(1) AS "Teams_Coached",
		MIN("Seasons") AS "Shortest_Stint",
		MAX("Seasons") AS "Longest_Stint",
		MIN("First_Season_w_Team") AS "First_Season",
		MAX("Last_Season_w_Team") AS "Last_Season",
		MIN("Min_Games_in_Season") AS "Min_Games_in_Season",
		MIN("Min_Wins_in_Season") AS "Min_Wins_in_Season",
		MIN("Min_Losses_in_Season") AS "Min_Losses_in_Season",
		MIN("Min_Ties_in_Season") AS "Min_Ties_in_Season",
		MAX("Max_Games_in_Season") AS "Max_Games_in_Season",
		MAX("Max_Wins_in_Season") AS "Max_Wins_in_Season",
		MAX("Max_Losses_in_Season") AS "Max_Losses_in_Season",
		MAX("Max_Ties_in_Season") AS "Max_Ties_in_Season",
		MAX("Games_w_Team") AS "Games_in_Career",
		MAX("Wins_w_Team") AS "Wins_in_Career",
		MAX("Losses_w_Team") AS "Losses_in_Career",
		MAX("Ties_w_Team") AS "Ties_in_Career",
		MIN(COALESCE(NULLIF("Min_Playoff_Games_in_Season", ''), 0)) AS "Min_Playoff_Games_in_Career",
		MIN(COALESCE(NULLIF("Min_Playoff_Wins_in_Season", ''), 0)) AS "Min_Playoff_Wins_in_Career",
		MIN(COALESCE(NULLIF("Min_Playoff_Losses_in_Season", ''), 0)) AS "Min_Playoff_Losses_in_Career",
		MAX(COALESCE(NULLIF("Max_Playoff_Games_in_Season", ''), 0)) AS "Max_Playoff_Games_in_Career",
		MAX(COALESCE(NULLIF("Max_Playoff_Wins_in_Season", ''), 0)) AS "Max_Playoff_Wins_in_Career",
		MAX(COALESCE(NULLIF("Max_Playoff_Losses_in_Season", ''), 0)) AS "Max_Playoff_Losses_in_Career",
		MAX(COALESCE(NULLIF("Playoff_Games_w_Team", ''), 0)) AS "Playoff_Games_in_Career",
		MAX(COALESCE(NULLIF("Playoff_Wins_w_Team", ''), 0)) AS "Playoff_Wins_in_Career",
		MAX(COALESCE(NULLIF("Playoff_Losses_w_Team", ''), 0)) AS "Playoff_Losses_in_Career"
	FROM dim_coach_stint
	GROUP BY 
		"Coach_ID",
		"Coach_Name";