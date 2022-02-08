CREATE VIEW dim_coach_stint AS 
	SELECT 
		id_coach."Coach_Stint_ID",
		id_coach."Coach_ID",
		id_coach."Coach_Name",
		id_franchise."Franchise_ID",
		id_franchise."Franchise_Name",
		MAX(coaches."Year") - MIN(coaches."Year") + 1 AS "Seasons",
		MIN(coaches."Year") AS "First_Season_w_Team",
		MAX(coaches."Year") AS "Last_Season_w_Team",
		MIN(coaches."Games_TY") AS "Min_Games_in_Season",
		MIN(coaches."Wins_TY") AS "Min_Wins_in_Season",
		MIN(coaches."Losses_TY") AS "Min_Losses_in_Season",
		MIN(coaches."Ties_TY") AS "Min_Ties_in_Season",
		MAX(coaches."Games_TY") AS "Max_Games_in_Season",
		MAX(coaches."Wins_TY") AS "Max_Wins_in_Season",
		MAX(coaches."Losses_TY") AS "Max_Losses_in_Season",
		MAX(coaches."Ties_TY") AS "Max_Ties_in_Season",
		SUM(coaches."Games_w_Team") AS "Games_w_Team",
		SUM(coaches."Wins_w_Team") AS "Wins_w_Team",
		SUM(coaches."Losses_w_Team") AS "Losses_w_Team",
		SUM(coaches."Ties_w_Team") AS "Ties_w_Team",
		MIN(coaches."Playoffs_TY") AS "Min_Playoff_Games_in_Season",
		MIN(coaches."Playoff_Wins_TY") AS "Min_Playoff_Wins_in_Season",
		MIN(coaches."Playoff_Losses_TY") AS "Min_Playoff_Losses_in_Season",
		MAX(coaches."Playoffs_TY") AS "Max_Playoff_Games_in_Season",
		MAX(coaches."Playoff_Wins_TY") AS "Max_Playoff_Wins_in_Season",
		MAX(coaches."Playoff_Losses_TY") AS "Max_Playoff_Losses_in_Season",
		SUM(coaches."Playoffs_w_Team") AS "Playoff_Games_w_Team",
		SUM(coaches."Playoff_Wins_w_Team") AS "Playoff_Wins_w_Team",
		SUM(coaches."Playoff_Losses_w_Team") AS "Playoff_Losses_w_Team"
	FROM coaches 
	LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv 
		ON coaches."Team" = lkp_abrv."Abbreviation"
	INNER JOIN id_franchise
		ON lkp_abrv."Franchise_Name" = id_franchise."Franchise_Name"
	INNER JOIN id_coach
		ON coaches."Coach" = id_coach."Coach_Name"
		AND id_franchise."Franchise_ID" = id_coach."Franchise_ID" 
	GROUP BY 
		id_coach."Coach_Stint_ID",
		id_coach."Coach_ID",
		id_coach."Coach_Name",
		id_franchise."Franchise_ID",
		id_franchise."Franchise_Name";