CREATE VIEW fact_coach_season AS 
	SELECT DISTINCT 
		coaches."Year",
		dim_coach_stint."Coach_Stint_ID",
		dim_coach_stint."Coach_ID",
		dim_coach_stint."Coach_Name",
		dim_coach_stint."Franchise_ID",
		dim_coach_stint."Franchise_Name",
		coaches."Games_TY",
		coaches."Wins_TY",
		coaches."Losses_TY",
		coaches."Ties_TY",
		coaches."Playoffs_TY",
		coaches."Playoff_Wins_TY",
		coaches."Playoff_Losses_TY"
	FROM coaches 
	LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv 
		ON coaches."Team" = lkp_abrv."Abbreviation"
	INNER JOIN id_franchise
		ON lkp_abrv."Franchise_Name" = id_franchise."Franchise_Name"
	INNER JOIN id_coach
		ON coaches."Coach" = id_coach."Coach_Name"
		AND id_franchise."Franchise_ID" = id_coach."Franchise_ID" 
	INNER JOIN dim_coach_stint
		ON id_coach."Coach_ID" = dim_coach_stint."Coach_ID"
		AND id_franchise."Franchise_ID" = dim_coach_stint."Franchise_ID";