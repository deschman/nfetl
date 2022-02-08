SELECT 
	dim_player_stint.Coach_ID,
	dim_player_stint.Coach_Name,
	dim_player_stint.Primary_Position,
	MIN(dim_player_stint.Min_Position_Rank_in_Season) AS Min_Rank_in_Season,
	SUM(dim_player_stint.Min_Position_Rank_in_Season) / COUNT(dim_player_stint.Min_Position_Rank_in_Season) AS Average_Rank_in_Season,
	COUNT(dim_player_stint.Min_Position_Rank_in_Season) AS Sample_Size
FROM dim_player_stint 
LEFT JOIN dim_coach 
	ON dim_player_stint.Coach_ID = dim_coach.Coach_ID 
WHERE 
	dim_player_stint.Coach_ID <> -1
	AND dim_player_stint.Primary_Position = 'WR'
	AND dim_coach.Last_Season = 2020
GROUP BY 
	dim_player_stint.Coach_ID, 
	dim_player_stint.Coach_Name,
	dim_player_stint.Primary_Position
ORDER BY 
	SUM(dim_player_stint.Min_Position_Rank_in_Season) / COUNT(dim_player_stint.Min_Position_Rank_in_Season) ASC;

SELECT 
	dim_player_stint.Coach_ID,
	dim_player_stint.Coach_Name,
	dim_player_stint.Primary_Position,
	dim_player_stint.Player_Name,
	dim_player_stint.Min_Overall_Rank_in_Season
FROM dim_player_stint 
/*LEFT JOIN dim_coach 
	ON dim_player_stint.Coach_ID = dim_coach.Coach_ID */
WHERE 
	dim_player_stint.Coach_ID <> -1
	AND dim_player_stint.Primary_Position = 'WR'
	-- AND dim_coach.Last_Season = 2020
LIMIT 5;

SELECT 
	sq1.Min_Rank,
	offense.*
FROM (
	SELECT 
		MIN(COALESCE(NULLIF(offense."Overall_Rank", ''), 0)) AS Min_Rank, 
		Player, 
		Year
	FROM offense 
	GROUP BY 
		Position_Rank, 
		Player) sq1
INNER JOIN offense 
	ON sq1.Player = offense.Player 
	AND sq1.Year = offense.Year
WHERE sq1.Min_Rank = 0;
