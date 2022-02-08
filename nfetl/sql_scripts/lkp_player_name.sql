CREATE VIEW lkp_player_name AS 
	SELECT 
		TRIM(REPLACE(REPLACE("Player", '*', ''), '+', '')) AS "Player_Name",
		"Player" AS "Player_Lookup_Name"
	FROM (
		SELECT "Player"
		FROM offense 
		UNION
		SELECT "Player"
		FROM kicking) AS players_sq1 