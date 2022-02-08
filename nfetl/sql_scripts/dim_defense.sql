CREATE VIEW dim_defense AS
	SELECT 
		dim_team."Franchise_ID" AS "Defense_Franchise_ID",
		dim_team."Team_ID" AS "Defense_Team_ID",
		dim_team."Franchise_Name" AS "Defense_Name",
		dim_team."Coach_ID",
		dim_team."Coach_Stint_ID", 
		dim_team."Coach_Name",
		op."Year",
		op."Games",
		qb."Completions" AS "QB_Pass_Completions",
		op."Completions" AS "Total_Pass_Completions",
		qb."Pass_Attempts" AS "QB_Pass_Attempts",
		op."Pass_Attempts" AS "Total_Pass_Attempts",
		qb."Pass_Yards" AS "QB_Pass_Yards",
		op."Pass_Yards" AS "Total_Pass_Yards",
		op."Yards_Per_Pass_Attempt" AS "Total_Pass_Yards_Per_Attempt",
		op."Pass_1st_Downs" AS "Total_Pass_1st_Downs",
		qb."Pass_TDs" AS "QB_Pass_TDs",
		op."Pass_TDs" AS "Total_Pass_TDs",
		qb."Interceptions" AS "QB_Pass_Interceptions",
		op."Interceptions" AS "Total_Pass_Interceptions",
		qb."Pass_Conversions" AS "QB_Pass_Conversions",
		qb."Sacks" AS "QB_Sacks",
		qb."Rush_Attempts" AS "QB_Rush_Attempts",
		rb."Rush_Attempts" AS "RB_Rush_Attempts",
		op."Rush_Attempts" AS "Total_Rush_Attempts",
		qb."Rush_Yards" AS "QB_Rush_Yards",
		rb."Rush_Yards" AS "RB_Rush_Yards",
		op."Rush_Yards" AS "Total_Rush_Yards",
		qb."Rush_TDs" AS "QB_Rush_TDs",
		rb."Rush_TDs" AS "RB_Rush_TDs",
		op."Rush_TDs" AS "Total_Rush_TDs",
		op."Yards_Per_Rush" AS "Total_Rush_Yards_Per_Attempt",
		op."Rush_1st_Downs" AS "Total_Rush_1st_Downs",
		wr."Targets" AS "WR_Targets",
		rb."Receptions" AS "RB_Receptions",
		te."Receptions" AS "TE_Receptions",
		wr."Receptions" AS "WR_Receptions",
		rb."Receive_Yards" AS "RB_Receive_Yards",
		te."Yards" AS "TE_Receive_Yards",
		wr."Yards" AS "WR_Receive_Yards",
		rb."Receive_TDs" AS "RB_Receive_TDs",
		te."TDs" AS "TE_Receive_TDs",
		wr."TDs" AS "WR_Receive_TDs",
		op."Total_Yards",
		op."Total_Plays",
		op."Total_Yards_Per_Play",
		op."Total_1st_Downs",
		op."Points_Allowed" AS "Total_Points_Allowed",
		op."Score_Percent" AS "Total_Score_Percent",
		op."Fumbles_Recovered",
		op."Takeaways",
		op."Turnover_Percent" AS "Total_Turnover_Percent",
		op."Penalty_Yards" AS "Total_Penalty_Yards",
		op."Penalty_1st_Downs" AS "Total_Penalty_1st_Downs",
		op."Expected_Points" AS "Total_Expected_Points",
		qb."Standard_Points" AS "QB_Standard_Points",
		rb."Standard_Points" AS "RB_Standard_Points",
		te."Standard_Points" AS "TE_Standard_Points",
		wr."Standard_Points" AS "WR_Standard_Points",
		qb."DraftKings_Points" AS "QB_DraftKings_Points",
		rb."DraftKings_Points" AS "RB_DraftKings_Points",
		te."DraftKings_Points" AS "TE_DraftKings_Points",
		wr."DraftKings_Points" AS "WR_DraftKings_Points",
		qb."FanDuel_Points" AS "QB_FanDuel_Points",
		rb."FanDuel_Points" AS "RB_FanDuel_Points",
		te."FanDuel_Points" AS "TE_FanDuel_Points",
		wr."FanDuel_Points" AS "WR_FanDuel_Points",
		qb."Standard_Points_Per_Game" AS "QB_Standard_Points_Per_Game",
		rb."Standard_Points_Per_Game" AS "RB_Standard_Points_Per_Game",
		te."Standard_Points_Per_Game" AS "TE_Standard_Points_Per_Game",
		wr."Standard_Points_Per_Game" AS "WR_Standard_Points_Per_Game",
		qb."DraftKings_Points_Per_Game" AS "QB_DraftKings_Points_Per_Game",
		rb."DraftKings_Points_Per_Game" AS "RB_DraftKings_Points_Per_Game",
		te."DraftKings_Points_Per_Game" AS "TE_DraftKings_Points_Per_Game",
		wr."DraftKings_Points_Per_Game" AS "WR_DraftKings_Points_Per_Game",
		qb."FanDuel_Points_Per_Game" AS "QB_FanDuel_Points_Per_Game",
		rb."FanDuel_Points_Per_Game" AS "RB_FanDuel_Points_Per_Game",
		te."FanDuel_Points_Per_Game" AS "TE_FanDuel_Points_Per_Game",
		wr."FanDuel_Points_Per_Game" AS "WR_FanDuel_Points_Per_Game"
	FROM op_defense AS op
	INNER JOIN qb_defense AS qb
		ON op."Year" = qb."Year" 
		AND op."Team" = qb."Team" 
	INNER JOIN rb_defense AS rb 
		ON op."Year" = rb."Year" 
		AND op."Team" = rb."Team"
	INNER JOIN te_defense AS te 
		ON op."Year" = te."Year" 
		AND op."Team" = te."Team"
	INNER JOIN wr_defense AS wr 
		ON op."Year" = wr."Year" 
		AND op."Team" = wr."Team"
	INNER JOIN dim_team
		ON op."Team" = dim_team."Franchise_Name"
		AND op."Year" = dim_team."Year";