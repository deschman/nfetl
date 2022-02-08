CREATE VIEW fact_player_season AS 
	SELECT DISTINCT 
		offense."Year", 
		dim_player_stint."Player_Stint_ID", 
		dim_player_stint."Player_ID", 
		dim_player_stint."Player_Name", 
		offense."Age" AS "Player_Age", 
		dim_player_stint."Coach_Stint_ID", 
		dim_player_stint."Coach_ID", 
		dim_player_stint."Coach_Name", 
		dim_player_stint."Franchise_ID",
		dim_player_stint."Franchise_Name",
		COALESCE(NULLIF(offense."Games", ''), 0) AS "Total_Games",
		COALESCE(NULLIF(offense."Starts", ''), 0) AS "Total_Starts",
		COALESCE(NULLIF(offense."Pass_Completions", ''), 0) AS "Total_Pass_Completions",
		COALESCE(NULLIF(offense."Pass_Attempts", ''), 0) AS "Total_Pass_Attempts",
		COALESCE(NULLIF(offense."Pass_Yards", ''), 0) AS "Total_Pass_Yards",
		COALESCE(NULLIF(offense."Pass_TDs", ''), 0) AS "Total_Pass_TDs",
		COALESCE(NULLIF(offense."Interceptions", ''), 0) AS "Total_Interceptions",
		COALESCE(NULLIF(offense."Rush_Attempts", ''), 0) AS "Total_Rush_Attempts",
		COALESCE(NULLIF(offense."Rush_Yards", ''), 0) AS "Total_Rush_Yards",
		COALESCE(NULLIF(offense."Yards_Per_Rush", ''), 0) AS "Total_Yards_Per_Rush",
		COALESCE(NULLIF(offense."Rush_TDs", ''), 0) AS "Total_Rush_TDs",
		COALESCE(NULLIF(offense."Targets", ''), 0) AS "Total_Targets",
		COALESCE(NULLIF(offense."Receptions", ''), 0) AS "Total_Receptions",
		COALESCE(NULLIF(offense."Receive_Yards", ''), 0) AS "Total_Receive_Yards",
		COALESCE(NULLIF(offense."Yards_Per_Reception", ''), 0) AS "Total_Yards_Per_Reception",
		COALESCE(NULLIF(offense."Receive_TDs", ''), 0) AS "Total_Receive_TDs",
		COALESCE(NULLIF(offense."Fumbles", ''), 0) AS "Total_Fumbles",
		COALESCE(NULLIF(offense."Fumbles_Lost", ''), 0) AS "Total_Fumbles_Lost",
		COALESCE(NULLIF(offense."Total_TDs", ''), 0) AS "Total_TDs",
		COALESCE(NULLIF(offense."Rush_Conversions", ''), 0) AS "Total_Rush_Conversions",
		COALESCE(NULLIF(offense."Pass_Conversions", ''), 0) AS "Total_Pass_Conversions",
		0 AS "Total_0-19_Attempts",
		0 AS "Total_0-19_Makes",
		0 AS "Total_20-29_Attempts",
		0 AS "Total_20-29_Makes",
		0 AS "Total_30-39_Attempts",
		0 AS "Total_30-39_Makes",
		0 AS "Total_40-49_Attempts",
		0 AS "Total_40-49_Makes",
		0 AS "Total_50+_Attempts",
		0 AS "Total_50+_Makes",
		0 AS "Total_FG_Attempts",
		0 AS "Total_FG_Makes",
		0 AS "Total_Longest_Make",
		0 AS "Total_FG_Percent",
		0 AS "Total_XP_Attempts",
		0 AS "Total_XP_Makes",
		0 AS "Total_XP_Percent",
		0 AS "Total_Kickoffs",
		0 AS "Total_Kickoff_Yards",
		0 AS "Total_Touchbacks",
		0 AS "Total_Touchback_Percent",
		0 AS "Total_Kickoff_Average",
		0 AS "Total_Punts",
		0 AS "Total_Punt_Yards",
		0 AS "Total_Longest_Punt",
		0 AS "Total_Punts_Blocked",
		0 AS "Total_Yards_Per_Punt",
		COALESCE(NULLIF(offense."Standard_Points", ''), 0) AS "Total_Standard_Points",
		COALESCE(NULLIF(offense."PPR_Points", ''), 0) AS "Total_PPR_Points",
		COALESCE(NULLIF(offense."DraftKings_Points", ''), 0) AS "Total_DraftKings_Points",
		COALESCE(NULLIF(offense."FanDuel_Points", ''), 0) AS "Total_FanDuel_Points",
		COALESCE(NULLIF(offense."VBD", ''), 0) AS "Total_VBD"
	FROM offense 
	LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv 
		ON offense."Team" = lkp_abrv."Abbreviation" 
	LEFT JOIN dim_team 
		ON lkp_abrv."Franchise_Name" = dim_team."Franchise_Name" 
		AND offense."Year" = dim_team."Year" 
	INNER JOIN lkp_player_name AS lkp_name 
		ON offense."Player" = lkp_name."Player_Lookup_Name" 
	LEFT JOIN id_player 
		ON lkp_name."Player_Name" = id_player."Player_Name" 
		AND dim_team."Franchise_ID" = id_player."Franchise_ID" 
	LEFT JOIN lkp_primary_position_stint AS lkp_pos_stint 
		ON id_player."Player_Stint_ID" = lkp_pos_stint."Player_Stint_ID"
	LEFT JOIN dim_player_stint 
		ON id_player."Player_ID" = dim_player_stint."Player_ID" 
		AND dim_team."Franchise_ID" = dim_player_stint."Franchise_ID" 
		AND dim_team."Coach_ID" = dim_player_stint."Coach_ID"
	WHERE dim_player_stint."Player_Stint_ID" IS NOT NULL 
	UNION ALL 
	SELECT DISTINCT
		kicking."Year",
		dim_player_stint."Player_Stint_ID", 
		dim_player_stint."Player_ID", 
		dim_player_stint."Player_Name", 
		kicking."Age", 
		dim_player_stint."Coach_Stint_ID", 
		dim_player_stint."Coach_ID", 
		dim_player_stint."Coach_Name", 
		dim_player_stint."Franchise_ID",
		dim_player_stint."Franchise_Name",
		COALESCE(NULLIF(kicking."Games", ''), 0) AS "Total_Games",
		COALESCE(NULLIF(kicking."Starts", ''), 0) AS "Total_Starts",
		0 AS "Total_Pass_Completions",
		0 AS "Total_Pass_Attempts",
		0 AS "Total_Pass_Yards",
		0 AS "Total_Pass_TDs",
		0 AS "Total_Interceptions",
		0 AS "Total_Rush_Attempts",
		0 AS "Total_Rush_Yards",
		0 AS "Total_Yards_Per_Rush",
		0 AS "Total_Rush_TDs",
		0 AS "Total_Targets",
		0 AS "Total_Receptions",
		0 AS "Total_Receive_Yards",
		0 AS "Total_Yards_Per_Reception",
		0 AS "Total_Receive_TDs",
		0 AS "Total_Fumbles",
		0 AS "Total_Fumbles_Lost",
		0 AS "Total_Total_TDs",
		0 AS "Total_Rush_Conversions",
		0 AS "Total_Pass_Conversions",
		COALESCE(NULLIF(kicking."0-19_Attempts", ''), 0) AS "Total_0-19_Attempts",
		COALESCE(NULLIF(kicking."0-19_Makes", ''), 0) AS "Total_0-19_Makes",
		COALESCE(NULLIF(kicking."20-29_Attempts", ''), 0) AS "Total_20-29_Attempts",
		COALESCE(NULLIF(kicking."20-29_Makes", ''), 0) AS "Total_20-29_Makes",
		COALESCE(NULLIF(kicking."30-39_Attempts", ''), 0) AS "Total_30-39_Attempts",
		COALESCE(NULLIF(kicking."30-39_Makes", ''), 0) AS "Total_30-39_Makes",
		COALESCE(NULLIF(kicking."40-49_Attempts", ''), 0) AS "Total_40-49_Attempts",
		COALESCE(NULLIF(kicking."40-49_Makes", ''), 0) AS "Total_40-49_Makes",
		COALESCE(NULLIF(kicking."50+_Attempts", ''), 0) AS "Total_50+_Attempts",
		COALESCE(NULLIF(kicking."50+_Makes", ''), 0) AS "Total_50+_Makes",
		COALESCE(NULLIF(kicking."FG_Attempts", ''), 0) AS "Total_FG_Attempts",
		COALESCE(NULLIF(kicking."FG_Makes", ''), 0) AS "Total_FG_Makes",
		COALESCE(NULLIF(kicking."Longest_Make", ''), 0) AS "Total_Longest_Make",
		COALESCE(NULLIF(kicking."FG_Percent", ''), 0) AS "Total_FG_Percent",
		COALESCE(NULLIF(kicking."XP_Attempts", ''), 0) AS "Total_XP_Attempts",
		COALESCE(NULLIF(kicking."XP_Makes", ''), 0) AS "Total_XP_Makes",
		COALESCE(NULLIF(kicking."XP_Percent", ''), 0) AS "Total_XP_Percent",
		COALESCE(NULLIF(kicking."Kickoffs", ''), 0) AS "Total_Kickoffs",
		COALESCE(NULLIF(kicking."Kickoff_Yards", ''), 0) AS "Total_Kickoff_Yards",
		COALESCE(NULLIF(kicking."Touchbacks", ''), 0) AS "Total_Touchbacks",
		COALESCE(NULLIF(kicking."Touchback_Percent", ''), 0) AS "Total_Touchback_Percent",
		COALESCE(NULLIF(kicking."Kickoff_Average", ''), 0) AS "Total_Kickoff_Average",
		COALESCE(NULLIF(kicking."Punts", ''), 0) AS "Total_Punts",
		COALESCE(NULLIF(kicking."Punt_Yards", ''), 0) AS "Total_Punt_Yards",
		COALESCE(NULLIF(kicking."Longest_Punt", ''), 0) AS "Total_Longest_Punt",
		COALESCE(NULLIF(kicking."Punts_Blocked", ''), 0) AS "Total_Punts_Blocked",
		COALESCE(NULLIF(kicking."Yards_Per_Punt", ''), 0) AS "Total_Yards_Per_Punt",
		0 AS "Total_Standard_Points",
		0 AS "Total_PPR_Points",
		0 AS "Total_DraftKings_Points",
		0 AS "Total_FanDuel_Points",
		0 AS "Total_VBD"
	FROM kicking 
	LEFT JOIN lkp_franchise_abbreviation AS lkp_abrv 
		ON kicking."Team" = lkp_abrv."Abbreviation" 
	LEFT JOIN dim_team 
		ON lkp_abrv."Franchise_Name" = dim_team."Franchise_Name" 
		AND kicking."Year" = dim_team."Year" 
	INNER JOIN lkp_player_name AS lkp_name 
		ON kicking."Player" = lkp_name."Player_Lookup_Name" 
	LEFT JOIN id_player 
		ON lkp_name."Player_Name" = id_player."Player_Name" 
		AND dim_team."Franchise_ID" = id_player."Franchise_ID" 
	LEFT JOIN dim_player_stint 
		ON id_player."Player_ID" = dim_player_stint."Player_ID"
		AND dim_team."Franchise_ID" = dim_player_stint."Franchise_ID"
		AND dim_team."Coach_ID" = dim_player_stint."Coach_ID"
	WHERE dim_player_stint."Player_Stint_ID" IS NOT NULL;
