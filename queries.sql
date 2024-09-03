
-- Return the total number of owners for games with the tag Action
WITH gt as (SELECT *
FROM games g1 
	JOIN games_tags  AS gt  ON g1.app_id = gt.game_id 
	JOIN tags  AS t ON gt.tag_id = t.tag_id

)

SELECT SUM(est_owners) FROM gt WHERE tag_name = 'Action'


-- Aggregate all of the tags for each game
SELECT app_id, array_agg(tag_name)
FROM games g1 
	JOIN games_tags  AS gt  ON g1.app_id = gt.game_id 
	JOIN tags  AS t ON gt.tag_id = t.tag_id
GROUP BY app_id


-- Insert game id and tag id rows into tagsnames
WITH gt as(
	SELECT DISTINCT g.app_id,g.tags, t.tag_name,t.tag_id 
	FROM games as g
	JOIN tags as t 
	ON g.tags LIKE '%' || t.tag_name || '%')
INSERT INTO test3 (game_id,tag_id) SELECT app_id,tag_id FROM gt