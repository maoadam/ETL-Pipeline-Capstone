use shows_tb;
SELECT DISTINCT subscription, title, episode, season, rating, Ranking
FROM (
  SELECT n.subscription, t.title, t.episode, t.season, t.rating, 
         RANK() OVER (PARTITION BY n.subscription ORDER BY t.rating DESC) AS Ranking 
  FROM top_250_usa t
  INNER JOIN netflix n ON t.title = n.title
) AS ranked_data
WHERE Ranking = 1
union
SELECT DISTINCT subscription, title, episode, season, rating, Ranking
FROM (
  SELECT n.subscription, t.title, t.episode, t.season, t.rating, 
         RANK() OVER (PARTITION BY n.subscription ORDER BY t.rating DESC) AS Ranking 
  FROM top_250_usa t
  INNER JOIN hulu n ON t.title = n.title
) AS ranked_data
WHERE Ranking = 1
union
SELECT DISTINCT subscription, title, episode, season, rating, Ranking
FROM (
  SELECT n.subscription, t.title, t.episode, t.season, t.rating, 
         RANK() OVER (PARTITION BY n.subscription ORDER BY t.rating DESC) AS Ranking 
  FROM top_250_usa t
  INNER JOIN amazon_prime n ON t.title = n.title
) AS ranked_data
WHERE Ranking = 1
union
SELECT DISTINCT subscription, title, episode, season, rating, Ranking
FROM (
  SELECT n.subscription, t.title, t.episode, t.season, t.rating, 
         RANK() OVER (PARTITION BY n.subscription ORDER BY t.rating DESC) AS Ranking 
  FROM top_250_usa t
  INNER JOIN disney_plus n ON t.title = n.title
) AS ranked_data
WHERE Ranking = 1;



